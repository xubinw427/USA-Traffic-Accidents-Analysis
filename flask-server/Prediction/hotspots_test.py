import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
from pyspark.sql.functions import row_number, rand, count


np.random.seed(233)

# pip show geopandas

# 初始化SparkSession
spark = SparkSession.builder.appName("filter 2 columns").getOrCreate()

data_dir = 'E:\\SFU\\classes\\CMPT-733\\Project\\code\\data_50w\\cleaned'
csv_files = ['part-00000-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00001-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00002-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00003-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00004-a0960d4f-919d-4e35-af21-57829675a319-c000.csv',
             'part-00005-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00006-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00007-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00008-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00009-a0960d4f-919d-4e35-af21-57829675a319-c000.csv', 'part-00010-a0960d4f-919d-4e35-af21-57829675a319-c000.csv']
columns_to_keep = ['Start_Lat', 'Start_Lng']
dfs = []
for file in csv_files:
    df = spark.read.csv(os.path.join(data_dir, file), header=True).select(columns_to_keep)
    dfs.append(df)
    print(f'File \'{file}\' read done.')

merged_df = dfs[0]
for df in dfs[1:]:
    merged_df = merged_df.union(df)
merged_df = merged_df.withColumnRenamed("Start_Lat", "latitude").withColumnRenamed("Start_Lng", "longitude")
merged_df = merged_df.withColumn("latitude", col("latitude").cast("double")) \
       .withColumn("longitude", col("longitude").cast("double"))
merged_df = merged_df.withColumn("latitude", col("latitude").cast("double")) \
                     .withColumn("longitude", col("longitude").cast("double")) \
                     .withColumn("round_lat", round(col("latitude"), 3)) \
                     .withColumn("round_lng", round(col("longitude"), 3))


num_rows = merged_df.count()
num_cols = len(merged_df.columns)
print(f"Number of rows: {num_rows}, Number of columns: {num_cols}")
# Number of rows: 353519, Number of columns: 2
print(merged_df.head(3))

# lon_min, lon_max = -74.25, -73.70
# lat_min, lat_max = 40.50, 40.90

# lon_min, lon_max = -74.80, -73.20
# lat_min, lat_max = 40.50, 40.90

lon_min, lon_max = -74.03, -73.97
lat_min, lat_max = 40.67, 40.73

filtered_df = merged_df.filter(
    (merged_df.longitude >= lon_min) & (merged_df.longitude <= lon_max) &
    (merged_df.latitude >= lat_min) & (merged_df.latitude <= lat_max)
)
# filtered_df = filtered_df.sample(withReplacement=False, fraction=0.02, seed=233)

'''
rounded_df = filtered_df.withColumn("rounded_latitude", round(merged_df["latitude"], 3)) \
                      .withColumn("rounded_longitude", round(merged_df["longitude"], 3))
counts_df = rounded_df.groupBy("rounded_latitude", "rounded_longitude") \
                      .count().withColumnRenamed("count", "num_records")
'''
counts_df = filtered_df
num_rows = counts_df.count()
num_cols = len(counts_df.columns)
print(f"Number of rows: {num_rows}, Number of columns: {num_cols}")
print(counts_df.head(3))

# pd_df = counts_df.groupby(["round_lat", "round_lng"]).count().withColumnRenamed("count", "num_records").toPandas()
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("round_lat", "round_lng").orderBy(rand())
counts_df = counts_df.withColumn("num_records", count("*").over(windowSpec))
random_row_df = counts_df.withColumn("row_number", row_number().over(windowSpec))\
                         .filter("row_number = 1").drop("row_number")
pd_df = random_row_df.toPandas()
pd_df['tops'] = pd_df['num_records'].rank(method='max', ascending=False) <= 10
top_records = pd_df[pd_df['tops']]
sampled_df = top_records.groupby(['round_lat', 'round_lng']).apply(lambda x: x.sample(1)).reset_index(drop=True)
sampled_df['color'] = np.where(sampled_df['tops'], 'red', 'blue')
sampled_df['size'] = np.where(sampled_df['tops'], 50, 5)
sampled_df['alpha'] = np.where(sampled_df['tops'], 1.0, 0.1)
pd_df = sampled_df
# base_size = 10  # Base size for scatter plot points
# pd_df['size'] = np.where(pd_df['tops'], base_size * 5, 0)
# pd_df['alpha'] = np.where(pd_df['tops'], 1.0, 0.2)
print(pd_df[['longitude', 'latitude']].head(10))


import matplotlib.pyplot as plt
norm = plt.Normalize(pd_df['num_records'].min(), pd_df['num_records'].max())
plt.figure(figsize=(10, 8))
# plt.scatter(pd_df['longitude'], pd_df['latitude'], c='red', alpha=0.5)
scatter = plt.scatter(pd_df['longitude'], pd_df['latitude'],
                      c=pd_df['num_records'], s=pd_df['size'], cmap='viridis', norm=norm, alpha=pd_df['alpha'])
plt.colorbar(scatter, label='Number of Records')
plt.title('Car Accidents in NYC Area')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.xlim(lon_min, lon_max)
plt.ylim(lat_min, lat_max)
plt.grid(True)
plt.show()
