import unittest
from unittest.mock import patch, MagicMock
from duplicate_application_identification import (
    enforce_schema, read_data, write_data, feature_extraction, approximate_matching, 
    calculate_similarity, process_in_batches, calculate_batch_similarity, 
    read_business_rules, preprocess_applications, categorize_applications, main
)
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, pandas_udf
from pyspark.sql.types import *
import yaml
import boto3

class TestDuplicateApplicationIdentification(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def read_test_data(self, file_path):
        return self.spark.read.csv(file_path, header=True, inferSchema=True)

    def test_enforce_schema(self):
        df = self.read_test_data('unit_test/test_data.csv')
        result_df = enforce_schema(df)
        self.assertIsNotNone(result_df)
        expected_columns = [
            'ApplicationKey', 'ApplicationID', 'DurationInMonths', 'ApplicationTitle', 
            'ProjectSummary', 'PublicDescription', 'Scope', 'StartDateTimeDateKey', 
            'startdate', 'ApplicationStatus', 'ProjectSetUpStatus', 'IFSProjectID', 
            'ResearchCategory', 'InnovationArea', 'CompetitionKey', 'CompetitionName', 
            'SubmittedDate', 'FundingDecision', 'FileEntryName', 'Completion', 
            'Resubmission', 'PreviousApplicationNumber', 'PreviousApplicationTitle', 
            'NoInnovationAreaApplicable', 'ManageFundingEmailDate', 'IsActive', 
            'validfrom', 'validto'
        ]
        self.assertEqual(result_df.columns, expected_columns)

    @patch('duplicate_application_identification.s3')
    @patch('duplicate_application_identification.athena')
    def test_read_data(self, mock_athena, mock_s3):
        # Test SQL file reading
        mock_s3.get_object.return_value = {'Body': MagicMock(read=lambda: b'SELECT * FROM table')}
        mock_athena.start_query_execution.return_value = {'QueryExecutionId': 'test_id'}
        mock_athena.get_query_execution.return_value = {'QueryExecution': {'Status': {'State': 'SUCCEEDED'}}}
        
        df_sql = read_data(self.spark, 'bucket', 'key.sql', 'db', cobalt_bucket='cobalt_bucket')
        self.assertIsNotNone(df_sql)
        
        # Test Parquet file reading
        with patch('duplicate_application_identification.spark.read.parquet') as mock_parquet:
            mock_parquet.return_value = self.spark.createDataFrame([(1, 'test')], ['id', 'name'])
            df_parquet = read_data(self.spark, 'bucket', 'key.parquet', 'db')
            self.assertIsNotNone(df_parquet)

    @patch('duplicate_application_identification.write_data')
    def test_write_data(self, mock_write_data):
        df = self.spark.createDataFrame([
            (1, 'Test Application', 'Summary', 'Description', 'Scope', 'Title', 'Status', 123, 1.0, 0)
        ], schema=['competitionkey', 'applicationid', 'applicationtitle', 'duplicate_applicationid', 'duplicate_applicationstatus', 'similarityscore', 'match_type', 'validationdate'])
        write_data(df, 'bucket', 'key', partition_by='competitionkey', partition=True, add_timestamp=True)
        mock_write_data.assert_called_once()

    def test_feature_extraction(self):
        df = self.spark.createDataFrame([('Test text',)], schema=['combined_text'])
        result_df = feature_extraction(df)
        self.assertIsNotNone(result_df)
        self.assertTrue('features' in result_df.columns)

    @patch('duplicate_application_identification.approximate_matching')
    def test_approximate_matching(self, mock_approximate_matching):
        new_apps = self.read_test_data('unit_test/test_data.csv')
        all_apps = self.read_test_data('unit_test/test_data.csv')
        mock_approximate_matching.return_value = self.spark.createDataFrame([
            (1, 2, 'Test Application', 'Another Application', 0.8)
        ], schema=['a_applicationid', 'b_applicationid', 'a_applicationtitle', 'b_applicationtitle', 'distance'])
        result_df = approximate_matching(new_apps, all_apps)
        self.assertIsNotNone(result_df)
        self.assertEqual(result_df.count(), 1)

    @pandas_udf(FloatType())
    def test_calculate_similarity(texts1, texts2):
        return calculate_similarity(texts1, texts2)

    @patch('duplicate_application_identification.process_in_batches')
    def test_process_in_batches(self, mock_process_in_batches):
        df = self.spark.createDataFrame([
            (1, 'Test Application')
        ], schema=['applicationid', 'applicationtitle'])
        mock_process_in_batches.return_value = df
        result_df = process_in_batches(df)
        self.assertIsNotNone(result_df)
        self.assertEqual(result_df.count(), 1)

    @patch('duplicate_application_identification.calculate_batch_similarity')
    def test_calculate_batch_similarity(self, mock_calculate_batch_similarity):
        batch_df = self.spark.createDataFrame([
            (1, 'Test Application', 'Combined Text')
        ], schema=['applicationid', 'applicationtitle', 'combined_text'])
        mock_calculate_batch_similarity.return_value = batch_df.withColumn('similarityscore', lit(0.8))
        result_df = calculate_batch_similarity(batch_df)
        self.assertIsNotNone(result_df)
        self.assertEqual(result_df.count(), 1)
        self.assertTrue('similarityscore' in result_df.columns)

    @patch('boto3.client')
    def test_read_business_rules(self, mock_boto3_client):
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: yaml.dump({'rules': [{'conditions': ['a.competitionkey == b.competitionkey'], 'similarity_threshold': 0.8, 'match_type': 'Exact'}]}).encode())
        }
        rules = read_business_rules('bucket', 'key')
        self.assertIsNotNone(rules)
        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0]['match_type'], 'Exact')

    def test_preprocess_applications(self):
        df = self.spark.createDataFrame([
            (1, 1, 'Title', 'Summary', 'Description', 'Scope')
        ], schema=['CompetitionKey', 'ApplicationID', 'ApplicationTitle', 'ProjectSummary', 'PublicDescription', 'Scope'])
        result_df = preprocess_applications(df)
        self.assertIsNotNone(result_df)
        self.assertTrue('combined_text' in result_df.columns)

    def test_categorize_applications(self):
        new_apps = self.read_test_data('unit_test/test_data.csv')
        all_apps = self.read_test_data('unit_test/test_data.csv')
        rules = [{'conditions': ['a.competitionkey == b.competitionkey'], 'similarity_threshold': 0.8, 'match_type': 'Exact'}]
        
        with patch('duplicate_application_identification.approximate_matching') as mock_approx_matching:
            mock_approx_matching.return_value = self.spark.createDataFrame([
                (1, 2, 'Test Application', 'Another Application', 0.8)
            ], schema=['a_applicationid', 'b_applicationid', 'a_applicationtitle', 'b_applicationtitle', 'distance'])
            
            with patch('duplicate_application_identification.process_in_batches') as mock_process:
                mock_process.return_value = self.spark.createDataFrame([
                    (1, 2, 'Test Application', 'Another Application', 0.85)
                ], schema=['a_applicationid', 'b_applicationid', 'a_applicationtitle', 'b_applicationtitle', 'similarityscore'])
                
                result_df = categorize_applications(new_apps, all_apps, rules)
                
                self.assertIsNotNone(result_df)
                self.assertTrue('competitionkey' in result_df.columns)
                self.assertTrue('applicationid' in result_df.columns)
                self.assertTrue('duplicate_applicationid' in result_df.columns)
                self.assertTrue('similarityscore' in result_df.columns)
                self.assertTrue('match_type' in result_df.columns)

    @patch('duplicate_application_identification.read_data')
    @patch('duplicate_application_identification.read_business_rules')
    @patch('duplicate_application_identification.categorize_applications')
    @patch('duplicate_application_identification.write_data')
    @patch('duplicate_application_identification.delete_data')
    def test_main(self, mock_delete_data, mock_write_data, mock_categorize, mock_read_rules, mock_read_data):
        mock_read_data.return_value = self.spark.createDataFrame([(1, 'Test')], ['id', 'name'])
        mock_read_rules.return_value = [{'conditions': ['a.id == b.id'], 'similarity_threshold': 0.8, 'match_type': 'Exact'}]
        mock_categorize.return_value = self.spark.createDataFrame([(1, 2, 0.9, 'Exact')], ['id1', 'id2', 'score', 'type'])
        
        main()
        
        mock_read_data.assert_called()
        mock_read_rules.assert_called()
        mock_categorize.assert_called()
        mock_write_data.assert_called()
        mock_delete_data.assert_called()

if __name__ == '__main__':
    unittest.main()



# python -m unittest test_duplicate_application_identification.py
# coverage run -m unittest discover
# coverage report -m