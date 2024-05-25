import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, rand, count
from pyspark.sql.window import Window


np.random.seed(233)
# ~ 0.005 * 0.005

def process_coordinates(spark, data_dir, csv_files, lon, lat):
    """
    Process the coordinates within specified longitude and latitude range,
    and return a Pandas DataFrame with additional attributes.

    Parameters:
    - spark: SparkSession instance.
    - data_dir: The directory where CSV files are stored.
    - csv_files: List of CSV file names to be processed.
    - lon_min, lon_max: The minimum and maximum longitude.
    - lat_min, lat_max: The minimum and maximum latitude.

    Returns:
    - A Pandas DataFrame with processed data.
    """

    lon_min, lon_max = lon - 0.02, lon + 0.02
    lat_min, lat_max = lat - 0.02, lat + 0.02

    columns_to_keep = ['Start_Lat', 'Start_Lng']
    dfs = []

    for file in csv_files:
        df = spark.read.csv(os.path.join(data_dir, file), header=True).select(columns_to_keep)
        dfs.append(df)
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = merged_df.union(df)

    merged_df = (merged_df.withColumnRenamed("Start_Lat", "latitude")
                           .withColumnRenamed("Start_Lng", "longitude")
                           .withColumn("latitude", col("latitude").cast("double"))
                           .withColumn("longitude", col("longitude").cast("double"))
                           .withColumn("round_lat", round(col("latitude") / 0.005) * 0.005)
                           .withColumn("round_lng", round(col("longitude") / 0.005) * 0.005))

    filtered_df = merged_df.filter(
        (merged_df.longitude >= lon_min) & (merged_df.longitude <= lon_max) &
        (merged_df.latitude >= lat_min) & (merged_df.latitude <= lat_max))

    windowSpec = Window.partitionBy("round_lat", "round_lng").orderBy(rand())
    '''
    random_row_df = (filtered_df.withColumn("num_records", count("*").over(windowSpec))
                                 .withColumn("row_number", row_number().over(windowSpec))
                                 .filter("row_number = 1").drop("row_number"))'''
    random_row_df = (filtered_df.withColumn("num_records", count("*").over(windowSpec))
                     .withColumn("rank", dense_rank().over(windowSpec))
                     .filter("rank <= 10").drop("rank"))

    pd_df = random_row_df.toPandas()
    top_locations = pd_df
    # pd_df['tops'] = pd_df['num_records'].rank(method='max', ascending=False) <= 10
    # top_locations = pd_df[pd_df['tops']][['latitude', 'longitude']]
    return top_locations


if __name__ == "__main__":
    spark = SparkSession.builder.appName("filter 2 columns").getOrCreate()
    data_dir = 'E:\\SFU\\classes\\CMPT-733\\Project\\code\\data_50w\\cleaned'
    csv_files = ['part-00000-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00001-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00002-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00003-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00004-a0960d4f-919d-4e35-af21-57829675a319-c000.csv',
                 'part-00005-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00006-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00007-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00008-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00009-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00010-a0960d4f-919d-4e35-af21-57829675a319-c000.csv']
    lon = -74.00
    lat= 40.70

    pd_df = process_coordinates(spark, data_dir, csv_files, lon, lat)
    print(pd_df.head())
