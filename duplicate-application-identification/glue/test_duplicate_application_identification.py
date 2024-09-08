import unittest
from unittest.mock import patch, MagicMock
from duplicate_application_identification import (
    enforce_schema, read_data, write_data, feature_extraction, approximate_matching, 
    calculate_similarity, process_in_batches, calculate_batch_similarity, 
    read_business_rules, preprocess_applications, categorize_applications, main
)
import pandas as pd
from pyspark.sql import SparkSession
import yaml

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
        df = self.read_test_data('unit_test\\test_data.csv')
        result_df = enforce_schema(df)
        self.assertIsNotNone(result_df)
        self.assertEqual(result_df.columns, [
            'ApplicationKey', 'ApplicationID', 'DurationInMonths', 'ApplicationTitle', 
            'ProjectSummary', 'PublicDescription', 'Scope', 'StartDateTimeDateKey', 
            'StartDate', 'ApplicationStatus', 'ProjectSetUpStatus', 'IFSProjectID', 
            'ResearchCategory', 'InnovationArea', 'CompetitionKey', 'CompetitionName', 
            'SubmittedDate', 'FundingDecision', 'FileEntryName', 'Completion', 
            'Resubmission', 'PreviousApplicationNumber', 'PreviousApplicationTitle', 
            'NoInnovationAreaApplicable', 'ManageFundingEmailDate', 'IsActive', 
            'ValidFrom', 'ValidTo'
        ])



    @patch('duplicate_application_identification.read_data')
    @patch('boto3.client')
    def test_read_data(self, mock_boto3_client, mock_read_data):
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_read_data.return_value = self.read_test_data('unit_test\\test_data.csv')
        df = read_data(self.spark, 'bucket', 'key', 'db')
        self.assertIsNotNone(df)
        self.assertEqual(df.count(), 3)




    @patch('duplicate_application_identification.write_data')
    def test_write_data(self, mock_write_data):
        df = self.spark.createDataFrame([
            (1, 'Test Application', 'Summary', 'Description', 'Scope', 'Title', 'Status', 123, 1.0, 0)
        ], schema=['competitionkey', 'applicationid', 'applicationtitle', 'duplicate_applicationid', 'duplicate_applicationstatus', 'similarityscore', 'match_type', 'validationdate'])
        write_data(df, 'bucket', 'key', partition_by='competitionkey', partition=True, add_timestamp=True)
        mock_write_data.assert_called_once()




    @patch('duplicate_application_identification.feature_extraction')
    def test_feature_extraction(self, mock_feature_extraction):
        df = self.spark.createDataFrame([
            ('Test text',)
        ], schema=['combined_text'])
        mock_feature_extraction.return_value = df
        result_df = feature_extraction(df)
        self.assertIsNotNone(result_df)
        self.assertEqual(result_df.columns, ['combined_text', 'words', 'rawFeatures', 'features'])



    @patch('duplicate_application_identification.approximate_matching')
    def test_approximate_matching(self, mock_approximate_matching):
        new_apps = self.read_test_data('unit_test\\test_data.csv')
        all_apps = self.read_test_data('unit_test\\test_data.csv')
        mock_approximate_matching.return_value = self.spark.createDataFrame([
            (1, 2, 'Test Application', 'Another Application', 0.8)
        ], schema=['a_applicationid', 'b_applicationid', 'a_applicationtitle', 'b_applicationtitle', 'similarityscore'])
        result_df = approximate_matching(new_apps, all_apps)
        self.assertIsNotNone(result_df)
        self.assertEqual(result_df.count(), 1)




    @patch('duplicate_application_identification.calculate_similarity')
    def test_calculate_similarity(self, mock_calculate_similarity):
        texts1 = pd.Series(['Text 1', 'Text 2'])
        texts2 = pd.Series(['Text 1', 'Text 2'])
        mock_calculate_similarity.return_value = pd.Series([0.9, 0.8])
        result = calculate_similarity(texts1, texts2)
        self.assertEqual(result.tolist(), [0.9, 0.8])

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
        mock_calculate_batch_similarity.return_value = batch_df
        result_df = calculate_batch_similarity(batch_df)
        self.assertIsNotNone(result_df)
        self.assertEqual(result_df.count(), 1)



    @patch('duplicate_application_identification.read_business_rules')
    def test_read_business_rules(self, mock_read_business_rules):
        with open('unit_test\\duplicate_application_check.yaml', 'r') as file:
            expected_rules = yaml.safe_load(file)['rules']
        rules = read_business_rules('unit_test\\duplicate_application_check.yaml')
        self.assertEqual(rules, expected_rules)



    @patch('duplicate_application_identification.preprocess_applications')
    def test_preprocess_applications(self, mock_preprocess_applications):
        df = self.read_test_data('unit_test\\test_data.csv')
        mock_preprocess_applications.return_value = df
        result_df = preprocess_applications(df)
        self.assertIsNotNone(result_df)
        self.assertEqual(result_df.count(), 3)



    @patch('duplicate_application_identification.categorize_applications')
    def test_categorize_applications(self, mock_categorize_applications):
        new_apps = self.read_test_data('unit_test\\test_data.csv')
        existing_apps = self.read_test_data('unit_test\\test_data.csv')
        mock_categorize_applications.return_value = self.spark.createDataFrame([
            (1, 2, 'Test Application', 'Existing Application', 0.85)
        ], schema=['a_applicationid', 'b_applicationid', 'a_applicationtitle', 'b_applicationtitle', 'similarityscore'])
        result_df = categorize_applications(new_apps, existing_apps)
        self.assertIsNotNone(result_df)
        self.assertEqual(result_df.count(), 1)



    @patch('duplicate_application_identification.main')
    def test_main(self, mock_main):
        mock_main.return_value = None
        main()
        mock_main.assert_called_once()



if __name__ == '__main__':
    unittest.main()

# --- Run Tests
# python -m unittest test_duplicate_application_identification.py

# --- Check Coverage
# coverage run -m unittest discover
# coverage report -m

