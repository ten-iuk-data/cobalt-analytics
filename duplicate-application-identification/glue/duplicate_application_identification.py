# s3://iukpreprod-data-landing-zone/semantic/ - bucket
# cobalt_application_duplicates - folder

import sys
import boto3
import time
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
import yaml
from io import StringIO
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.storagelevel import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, concat, col, coalesce, lit, when, count, length, trim, first, current_date, broadcast, pandas_udf
from pyspark.sql.types import *
from pyspark.sql.window import Window
from sentence_transformers import SentenceTransformer, util
import pyarrow.parquet as pq
from functools import reduce as reduce_func
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, BucketedRandomProjectionLSH
from pyspark.ml.linalg import Vectors, VectorUDT
import pandas as pd
import numpy as np



sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
s3 = boto3.client('s3')


spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.default.parallelism", "400")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 52428800)
spark.conf.set("spark.sql.broadcastTimeout", 1800)
spark.conf.set("spark.sql.adaptive.enabled", "true")

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job.init(args['JOB_NAME'], args)




# enforce schema
def enforce_schema(df):
    df = df.select(
    col("applicationkey").cast(LongType()).alias("ApplicationKey"),
    col("applicationid").cast(LongType()).alias("ApplicationID"),
    col("durationinmonths").cast(LongType()).alias("DurationInMonths"),
    col("applicationtitle").alias("ApplicationTitle"),
    col("projectsummary").alias("ProjectSummary"),
    col("publicdescription").alias("PublicDescription"),
    col("scope").alias("Scope"),
    col("startdatetimedatekey").cast(LongType()).alias("StartDateTimeDateKey"),
    col("startdate").cast(TimestampType()).alias("StartDate"),
    col("applicationstatus").alias("ApplicationStatus"),
    col("projectsetupstatus").alias("ProjectSetUpStatus"),
    col("ifsprojectid").alias("IFSProjectID"),
    col("researchcategory").alias("ResearchCategory"),
    col("innovationarea").alias("InnovationArea"),
    col("competitionkey").cast(LongType()).alias("CompetitionKey"),
    col("competitionname").alias("CompetitionName"),
    col("submitteddate").cast(TimestampType()).alias("SubmittedDate"),
    col("fundingdecision").alias("FundingDecision"),
    col("fileentryname").alias("FileEntryName"),
    col("completion").cast(DoubleType()).alias("Completion"),
    col("resubmission").cast(LongType()).alias("Resubmission"),
    col("previousapplicationnumber").alias("PreviousApplicationNumber"),
    col("previousapplicationtitle").alias("PreviousApplicationTitle"),
    col("noinnovationareaapplicable").cast(LongType()).alias("NoInnovationAreaApplicable"),
    col("managefundingemaildate").cast(TimestampType()).alias("ManageFundingEmailDate"),
    col("isactive").cast(LongType()).alias("IsActive"),
    col("validfrom").cast(TimestampType()).alias("ValidFrom"),
    col("validto").cast(TimestampType()).alias("ValidTo"))
    
    return df
    
    


# Read data
def read_data(spark, bucket, key, schema=False):
    if key.endswith('.sql'):
        response = s3.get_object(Bucket=bucket, Key=key)
        query = response['Body'].read().decode('utf-8')

        athena = boto3.client('athena')
        glue_database = 'preprod_semantic_db'
        s3_output = 's3://iukpreprod-data-landing-zone/athena-results/cobalt_athena_query_results/'
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': glue_database},
            ResultConfiguration={'OutputLocation': s3_output}
        )
        query_execution_id = response['QueryExecutionId']
        while True:
            response = athena.get_query_execution(QueryExecutionId=query_execution_id)
            state = response['QueryExecution']['Status']['State']
            if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(5)

        if state == 'SUCCEEDED':
            result_file = f"{s3_output}{query_execution_id}.csv"
            df = spark.read.csv(result_file, header=True, inferSchema=True)
            return enforce_schema(df) if schema else df
            
        else:
            raise Exception(f"Athena query failed with state: {state}")
    else:
        input_path = f's3://{bucket}/{key}'
        try:
            df = (spark.read.option("mergeSchema", "true")
                        .parquet(f's3://{bucket}/{key}')
                        .repartition(200, "competitionkey")
                        .persist(StorageLevel.MEMORY_AND_DISK))
            if df is None or df.count() == 0:
                raise ValueError("Dataset is empty or could not be read.")
            return df
        except Exception as e:
            raise ValueError(f"Failed to read the parquet file: {str(e)}")



# Write data
def write_data(df, bucket, key, partition_by=None, append_mode=True, partition=False, add_timestamp=False):
    output_path = f's3://{bucket}/{key}'
    if add_timestamp:
        currentdate = datetime.now().strftime("%d-%m-%Y")
        output_path = f"{output_path}/dim_application_{currentdate}"
    mode = 'append' if append_mode else 'overwrite'
    if partition_by and partition:
        (df.write.mode(mode).partitionBy(partition_by).parquet(output_path))
    if not partition:
        (df.coalesce(1).write.mode(mode).parquet(output_path))





# delete data
def delete_data(bucket, folder):
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=folder):
        if 'Contents' in page:
            for obj in page['Contents']:
                s3.delete_object(Bucket=bucket, Key=obj['Key'])
    print(f"All files in {folder} have been deleted.")




# Load and broadcast model
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2", cache_folder="s3://cobalt-analytics/libraries/")
bc_model = sc.broadcast(model)



# Feature Extraction
def feature_extraction(df):
    tokenizer = Tokenizer(inputCol="combined_text", outputCol="words")
    wordsData = tokenizer.transform(df)
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10000)
    featurizedData = hashingTF.transform(wordsData)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)
    rescaledData = rescaledData.withColumn("features", rescaledData["features"].cast(VectorUDT()))
    return rescaledData



# Approximate Matching
def approximate_matching(new_apps, all_apps, threshold=0.5):
    new_df = feature_extraction(new_apps)
    all_df = feature_extraction(all_apps)
    brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", bucketLength=3.0, numHashTables=3)
    model = brp.fit(all_df)
    transformed_new = model.transform(new_df)
    transformed_all = model.transform(all_df)
    candidates = model.approxSimilarityJoin(transformed_new, transformed_all, threshold, distCol="distance")
    print("candidates columns: ", candidates.columns)
    print("candidates shape (rows, columns): ", (candidates.count(), len(candidates.columns)))
    
    potential_matches = candidates.filter(col("datasetA.applicationid") < col("datasetB.applicationid"))
    columns = [
        "competitionkey", "applicationid", "applicationtitle", "applicationstatus", 
        "combined_text", "projectsummary", "scope", "publicdescription"
    ]
    select_expr = (
        [col(f"datasetA.{c}").alias(f"a_{c}") for c in columns] +
        [col(f"datasetB.{c}").alias(f"b_{c}") for c in columns] +
        [col("distance")]
    )
    
    potential_matches = potential_matches.select(*select_expr)
    return potential_matches



# Calculate similarity
@pandas_udf(FloatType())
def calculate_similarity(texts1: pd.Series, texts2: pd.Series) -> pd.Series:
    model = bc_model.value
    embeddings1 = model.encode(texts1.tolist(), convert_to_tensor=True)
    embeddings2 = model.encode(texts2.tolist(), convert_to_tensor=True)
    similarities = util.pytorch_cos_sim(embeddings1, embeddings2).diagonal().numpy()
    return pd.Series(similarities)




# Process in batches
def process_in_batches(df, batch_size=10000):
    total_rows = df.count()
    if total_rows == 0:
        return df.sparkSession.createDataFrame(df.sparkSession.sparkContext.emptyRDD(), df.schema)
    num_batches = (total_rows + batch_size - 1) // batch_size
    results = []
    for i in range(num_batches):
        start = i * batch_size
        end = min((i + 1) * batch_size, total_rows)
        batch_df = df.limit(end).subtract(df.limit(start))
        batch_result = calculate_batch_similarity(batch_df)
        results.append(batch_result)
    if not results:
        return df.sparkSession.createDataFrame(df.sparkSession.sparkContext.emptyRDD(), df.schema)
    return reduce_func(lambda x, y: x.unionByName(y), results)



def calculate_batch_similarity(batch_df):
    return batch_df.withColumn('similarityscore', calculate_similarity(col('a_combined_text'), col('b_combined_text')))



# Read Business Rules
def read_business_rules(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    yaml_content = obj['Body'].read().decode('utf-8')
    rules = yaml.safe_load(StringIO(yaml_content))['rules']
    for rule in rules:
        rule['conditions'] = [condition.strip() for condition in rule['conditions']]
    return rules




# preprocess applications
def preprocess_applications(df):
    df = df.dropDuplicates(["CompetitionKey", "ApplicationID"])
    df = df.withColumn('combined_text', concat(
        coalesce(col('ProjectSummary'), lit('')), lit(' '), 
        coalesce(col('Scope'), lit('')), lit(' '), 
        coalesce(col('PublicDescription'), lit('')), lit(' '), 
        coalesce(col('ApplicationTitle'), lit(''))))
    df = df.filter(length(trim(col('combined_text'))) > 0)
    return df




# Categorize applications
def categorize_applications(new_apps, all_apps, rules):
    
    print("Before preprocess_applications")
    print("new_apps columns: ", new_apps.columns)
    print("new_apps shape (rows, columns): ", (new_apps.count(), len(new_apps.columns)))
    print("all_apps columns: ", all_apps.columns)
    print("all_apps shape (rows, columns): ", (all_apps.count(), len(all_apps.columns)))
    
    new_apps = preprocess_applications(new_apps)
    all_apps = preprocess_applications(all_apps)
    
    print("After preprocess_applications")
    print("new_apps columns: ", new_apps.columns)
    print("new_apps shape (rows, columns): ", (new_apps.count(), len(new_apps.columns)))
    print("all_apps columns: ", all_apps.columns)
    print("all_apps shape (rows, columns): ", (all_apps.count(), len(all_apps.columns)))
    
    
    final_schema = StructType([
        StructField("competitionKey", LongType(), True),
        StructField("applicationid", LongType(), True),
        StructField("applicationtitle", StringType(), True),
        StructField("duplicate_applicationid", LongType(), True),
        StructField("duplicate_applicationstatus", StringType(), True),
        StructField("similarityscore", FloatType(), True),
        StructField("match_type", StringType(), True),
        StructField("validationdate", DateType(), True)])
    final_df = spark.createDataFrame([], final_schema)
    
    
    # Stage 1: Approximate Matching
    potential_matches = approximate_matching(new_apps, all_apps)
    print("After potential_matches")
    print("After potential_matches columns: ", potential_matches.columns)
    print("potential_matches shape (rows, columns): ", (potential_matches.count(), len(potential_matches.columns)))

    if potential_matches is None or potential_matches.count() == 0:
        return potential_matches
    
    
    # Stage 2: Accurate Matching with Sentence Transformer
    df_rule = process_in_batches(potential_matches)
    print("After Accurate Matching df columns: ", df_rule.columns)
    print("Accurate Matching df shape (rows, columns): ", (df_rule.count(), len(df_rule.columns)))
    
    all_matches = []
    
    for rule in rules:
        modified_conditions = [condition.replace('a.', 'a_').replace('b.', 'b_') for condition in rule['conditions']]
        condition_exprs = [eval(condition) for condition in modified_conditions]
        condition = reduce_func(lambda x, y: x & y, condition_exprs)
        
        joined_df = df_rule.filter(condition)
        
        print("joined_df columns: ", joined_df.columns)
        print("joined_df shape (rows, columns): ", (joined_df.count(), len(joined_df.columns)))
                      
        if 'window_aggregation' in rule:
            window_spec = Window.partitionBy(col('a_applicationid'))
            agg_condition = eval(rule['window_aggregation']['condition'].replace('b.', 'b_'))
            joined_df = joined_df.withColumn('count_agg', agg_condition.over(window_spec))
            joined_df = joined_df.filter(col('count_agg') >= rule['window_aggregation']['threshold'])
        joined_df = joined_df.filter(col('similarityscore') > rule['similarity_threshold'])
        
        final_df_part = joined_df.select(
            col('a_competitionkey').alias('competitionkey'),
            col('a_applicationid').alias('applicationid'),
            col('a_applicationtitle').alias('applicationtitle'),
            col('b_applicationid').alias('duplicate_applicationid'),
            col('b_applicationstatus').alias('duplicate_applicationstatus'),
            col('similarityscore'),
            lit(rule['match_type']).alias('match_type'),
            current_date().alias('validationdate'))
            
        all_matches.append(final_df_part)
    final_df = reduce_func(lambda x, y: x.unionByName(y), all_matches)
    final_df = final_df.dropDuplicates()
    
    print("final_df columns: ", final_df.columns)
    print("final_df shape (rows, columns): ", (final_df.count(), len(final_df.columns)))
    
    return final_df



# Main
def main():
    root_bucket = 'iukpreprod-data-landing-zone'
    semantic_bucket = 'iukpreprod-data-landing-zone/semantic'
    semantic_input_key = 'dim_application/dim_application.parquet'
    cobalt_bucket = 'cobalt-ml'
    cobalt_output_duplicates = 'cobalt_application_dups'
    cobalt_output_processed = 'cobalt_application_processed'
    business_rules_key = 'iukprod/input/duplicate_applications_check.yaml'
    sql_key = 'iukprod/input/get_new_applications.sql'
    athena_query_results_folder = 'athena-results/cobalt_athena_query_results/'
    
    
    try:
        # Read new applications
        new_apps = read_data(spark, cobalt_bucket, sql_key, schema=True)

        # Read all applications
        all_apps = read_data(spark, semantic_bucket, semantic_input_key)

        # Read business rules
        rules = read_business_rules(cobalt_bucket, business_rules_key)

        # Categorize applications
        final_df = categorize_applications(new_apps, all_apps, rules)
        
        if final_df.count() > 0:
            
            # Save output data
            final_df = final_df.repartition(200, "competitionkey")
            write_data(final_df, semantic_bucket, cobalt_output_duplicates, partition_by="competitionkey", partition=True)
        
        # Update processed applications
        write_data(new_apps, semantic_bucket, cobalt_output_processed, partition=False, add_timestamp=True)
        
        # Delete temp query results for new applications
        delete_data(root_bucket, athena_query_results_folder)

    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    
    finally:
        # Unpersist Datasets
        new_apps.unpersist()
        all_apps.unpersist()
        final_df.unpersist()
        
        # End the job
        job.commit()


if __name__ == "__main__":
    main()