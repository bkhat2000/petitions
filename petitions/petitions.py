from pyspark.sql import SparkSession
from pyspark.sql import functions as F


sc = SparkSession \
    .builder \
    .appName("Petition App") \
    .getOrCreate()


class Petitions:

    def __init__(self, source_path, destination_path):
        self.source_path = source_path
        self.destination_path = destination_path
        self.df_petition = None
        self.df_transformed_petition = None
        self.df_petition_count_by_columns = None
        self.generate_df_petition()

    def generate_df_petition(self):
        self.load_petitions_file_to_df()

    def load_petitions_file_to_df(self):
       self.df_petition = sc.read.json(self.source_path)
       self.transform_df_petition_count_by_columns()
       self.transform_df_petition_top_20_word_count()

    def transform_df_petition_count_by_columns(self):

        self.df_petition = self.df_petition.withColumn("petition_id", F.monotonically_increasing_id())
        self.df_petition = self.df_petition.select("petition_id",
                              F.array(F.expr("abstract.*")).alias("abstract"),
                              F.array(F.expr("label.*")).alias("label"),
                              "numberOfSignatures"
                              )

        self.df_petition = self.df_petition.withColumn("abstract", F.explode("abstract")) \
            .withColumn("label", F.explode("label"))
        self.df_transformed_petition = self.df_petition.withColumn("abstract", F.lower(F.col("abstract"))).withColumn\
             ("label", F.lower(F.col("label")))
        self.df_petition_count_by_columns = self.word_count_in_columns()
        filename = "word_count_in_columns.csv"
        destination_path = self.destination_path + filename
        self.save_df_to_csv(self.df_petition_count_by_columns, destination_path)

    def transform_df_petition_top_20_word_count(self):
        column_filter_list = self.get_top_20_words_across_petitions()
        df = self.df_petition .withColumn("words", F.explode(F.split(F.col("abstract"), " ")))
        df = df.where((F.length(F.col("words")) > 4) & (F.col("words").isin(column_filter_list)))
        df = df.groupBy("petition_id").pivot("words").count().na.fill(0)
        filename = "pivot_top_20_word_count_to_columns.csv"
        destination_path = self.destination_path + filename
        self.save_df_to_csv(df, destination_path)


    def word_count_in_columns(self):
        df = self.df_transformed_petition.withColumn("abstract", F.size(F.split(F.col("abstract"), " "))) \
            .withColumn("label", F.size(F.split(F.col("label"), " ")))
        df = df.select("petition_id", "abstract", "label", "numberOfSignatures")
        return df

    def get_top_20_words_across_petitions(self):
        df = self.df_petition.withColumn("abstract", (F.split(F.col("abstract"), " "))) \
            .withColumn("label", (F.split(F.col("label"), " ")))

        col1 = df.withColumn("explode", F.explode("abstract")).groupBy("explode").count().orderBy(F.desc("count"))
        l = col1.where(F.length(F.col("explode")) > 4).take(20)
        dataframe_columns = []
        for i, x in l:
            dataframe_columns.append(i)
        return dataframe_columns



    def save_df_to_csv(self, df, destination_path):
        df.toPandas().to_csv(destination_path, sep=',', header=True, index=False)
