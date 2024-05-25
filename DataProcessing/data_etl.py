from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import (
    to_timestamp, year, hour, date_format
)
from pyspark.sql.types import (
    IntegerType, StringType, StructType, StructField, DoubleType, BooleanType
)
from timeit import default_timer as timer
import sys, re

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

accident_schema = StructType(
    [
    StructField("ID", StringType(), True),
    StructField("Source", StringType(), True),
    StructField("Severity", IntegerType(), True),
    StructField("Start_Time", StringType(), True),
    StructField("End_Time", StringType(), True),
    StructField("Start_Lat", DoubleType(), True),
    StructField("Start_Lng", DoubleType(), True),
    StructField("End_Lat", DoubleType(), True),
    StructField("End_Lng", DoubleType(), True),
    StructField("Distance(mi)", DoubleType(), True),
    StructField("Description", StringType(), True),
    StructField("Street", StringType(), True),
    StructField("City", StringType(), True),
    StructField("County", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Zipcode", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Timezone", StringType(), True),
    StructField("Airport_Code", StringType(), True),
    StructField("Weather_Timestamp", StringType(), True),
    StructField("Temperature(F)", DoubleType(), True),
    StructField("Wind_Chill(F)", DoubleType(), True),
    StructField("Humidity(%)", DoubleType(), True),
    StructField("Pressure(in)", DoubleType(), True),
    StructField("Visibility(mi)", DoubleType(), True),
    StructField("Wind_Direction", StringType(), True),
    StructField("Wind_Speed(mph)", DoubleType(), True),
    StructField("Precipitation(in)", DoubleType(), True),
    StructField("Weather_Condition", StringType(), True),
    StructField("Amenity", BooleanType(), True),
    StructField("Bump", BooleanType(), True),
    StructField("Crossing", BooleanType(), True),
    StructField("Give_Way", BooleanType(), True),
    StructField("Junction", BooleanType(), True),
    StructField("No_Exit", BooleanType(), True),
    StructField("Railway", BooleanType(), True),
    StructField("Roundabout", BooleanType(), True),
    StructField("Station", BooleanType(), True),
    StructField("Stop", BooleanType(), True),
    StructField("Traffic_Calming", BooleanType(), True),
    StructField("Traffic_Signal", BooleanType(), True),
    StructField("Turning_Loop", BooleanType(), True),
    StructField("Sunrise_Sunset", StringType(), True),
    StructField("Civil_Twilight", StringType(), True),
    StructField("Nautical_Twilight", StringType(), True),
    StructField("Astronomical_Twilight", StringType(), True)
    ]
)

@functions.udf(returnType=StringType())
def phrase_date(line):
    match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
    if match:
        time_part = match.group(1)
        return time_part
    else:
        return 'Invalid Format'
    
def main():
    # main logic starts here
    df = spark.read.csv("US_Accidents_March23.csv", schema=accident_schema, header=True)
    columns_to_drop = ["ID", "Source", "End_Time", "End_Lat", "End_Lng", "Distance(mi)", "Description",
                       "Country", "Timezone", "Airport_Code", "Weather_Timestamp",
                       "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight"]

    data = df.drop(*columns_to_drop)

    # data.show()
    total_rows = data.count()
    print("Total number of rows in DataFrame:", total_rows) 
    data_cleaned = data.na.drop()
    cleaned_rows = data_cleaned.count()
    print("Total number of rows after cleaned:", cleaned_rows)
    data_phrased = data_cleaned.withColumn("time", phrase_date(data_cleaned["Start_Time"]))
    data_phrased = (
        data_phrased.filter(
            (data_phrased["time"] != "Invalid Format")
        )
        .withColumn("timestamp", to_timestamp(data_phrased["time"]))
    )
    data_phrased = (
        data_phrased.withColumn("date", date_format("timestamp", "yyyy-MM-dd"))
        .withColumn("hour", hour(data_phrased["timestamp"]))
        .withColumn("year", year(data_phrased["timestamp"]))
    )
    data_phrased.groupBy("Year").count().show()
    # data_phrased.repartition("Year").write.csv("cleaned", mode="overwrite", header=True)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("load accident data").getOrCreate()
    assert spark.version >= "3.0"  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel("WARN")
    start_time = timer()
    main()
    spark.stop()
    end_time = timer()
    execution_time = end_time - start_time
    print("Execution time: {} seconds".format(execution_time))