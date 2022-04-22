from unittest import TestCase
from petitions import petitions
import os
from pyspark.sql import SparkSession

class TestPetitions(TestCase):

    def setUp(self):
        self.sc = SparkSession \
            .builder \
            .appName("Test Petition App") \
            .getOrCreate()

        self.source_path = "input_data_folder/sample_input_data.json"
        self.destination_path = "../tests/output_data_folder/"
        self.petitions = petitions.Petitions(self.source_path, self.destination_path)

    def test_load_json_file_to_df(self):
        self.petitions.load_petitions_file_to_df()
        assert not self.petitions.df_petition.count() == 0

    def test_column_counts(self):
        input_df = self.sc.createDataFrame(
            data=[[0,51,13,27],
                [1,52,16,223],
                [2,46,12,176],
                [3,53,11,15],
                [4,39,16,12],
                [5,35,13,31],
                [6,48,8,26],
                [7,50,12,65],
                [8,59,11,181],
                [9,48,10,13],
                [10,20,7,56],
                [11,52,11,29463],
                [12,38,11,21],
                [13,48,13,43],
                [14,60,16,58],
                [15,42,10,221],
                [16,35,9,526],
                [17,41,14,1202],
                [18,46,13,905],
                [19,41,12,1164]],
            schema=['petition_id', 'abstract', 'label', 'numberOfSignatures'])

        expected_df = self.petitions.df_petition_count_by_columns
        self.assertEqual(sorted(input_df.toPandas().to_dict()), sorted(expected_df.toPandas().to_dict()))