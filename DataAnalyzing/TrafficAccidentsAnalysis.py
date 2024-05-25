from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, floor, ceil, to_timestamp, dayofweek, year, month, hour, when, lit
)
from pyspark.sql.types import (
    IntegerType, StringType, StructType, StructField, DoubleType, BooleanType
)
from timeit import default_timer as timer
import sys, calendar

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

def main():
    # Load the CSV data into DataFrame
    df = spark.read.csv("US_Accidents_March23.csv", schema=accident_schema, header=True)
    columns_to_drop = ["ID", "Source", "End_Time", "End_Lat", "End_Lng", "Distance(mi)", "Description",
                       "Country", "Timezone", "Airport_Code", "Weather_Timestamp",
                       "Civil_Twilight", "Nautical_Twilight", "Astronomical_Twilight"]

    df = df.drop(*columns_to_drop)
    df.cache()

    df_time = df.withColumn("Start_Time", to_timestamp("Start_Time"))
    df_time = df_time.filter(df.Start_Time.isNotNull())

    # GRAPH 1: Accidents by Year
    df_year = df_time.withColumn("Year", year("Start_Time"))
    df_year = df_year.groupBy("Year").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Year")
    print(df_year.show())
    df_year.coalesce(1).write.csv('Graph_1_Year', header=True, mode='overwrite')

    # GET THE MONTH NAME FROM THE MONTH NUMBER
    def get_month_name(month_num):
        return calendar.month_name[month_num]

    month_udf = udf(get_month_name, StringType())

    # GRAPH 2: Accidents by Month
    df_month = df_time.withColumn("Month", month("Start_Time"))
    df_month = df_month.groupBy("Month").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Month")
    df_month = df_month.withColumn("Month", month_udf(col("Month")))
    print(df_month.show())
    df_month.coalesce(1).write.csv('Graph_2_Month', header=True, mode='overwrite')

    # GRAPH 2-2016: Accidents by Month_2016
    df_month_2016 = df_time.filter(year("Start_Time") == 2016)
    df_month_2016 = df_month_2016.withColumn("Month", month("Start_Time"))
    df_month_2016 = df_month_2016.groupBy("Month").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Month")
    df_month_2016 = df_month_2016.withColumn("Month", month_udf(col("Month")))
    print(df_month_2016.show())
    df_month_2016.coalesce(1).write.csv('Graph_2_Month_2016', header=True, mode='overwrite')

    # GRAPH 2-2017: Accidents by Month_2017
    df_month_2017 = df_time.filter(year("Start_Time") == 2017)
    df_month_2017 = df_month_2017.withColumn("Month", month("Start_Time"))
    df_month_2017 = df_month_2017.groupBy("Month").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Month")
    df_month_2017 = df_month_2017.withColumn("Month", month_udf(col("Month")))
    print(df_month_2017.show())
    df_month_2017.coalesce(1).write.csv('Graph_2_Month_2017', header=True, mode='overwrite')

    # GRAPH 2-2018: Accidents by Month_2018
    df_month_2018 = df_time.filter(year("Start_Time") == 2018)
    df_month_2018 = df_month_2018.withColumn("Month", month("Start_Time"))
    df_month_2018 = df_month_2018.groupBy("Month").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Month")
    df_month_2018 = df_month_2018.withColumn("Month", month_udf(col("Month")))
    print(df_month_2018.show())
    df_month_2018.coalesce(1).write.csv('Graph_2_Month_2018', header=True, mode='overwrite')

    # GRAPH 2-2019: Accidents by Month_2019
    df_month_2019 = df_time.filter(year("Start_Time") == 2019)
    df_month_2019 = df_month_2019.withColumn("Month", month("Start_Time"))
    df_month_2019 = df_month_2019.groupBy("Month").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Month")
    df_month_2019 = df_month_2019.withColumn("Month", month_udf(col("Month")))
    print(df_month_2019.show())
    df_month_2019.coalesce(1).write.csv('Graph_2_Month_2019', header=True, mode='overwrite')

    # GRAPH 2-2020: Accidents by Month_2020
    df_month_2020 = df_time.filter(year("Start_Time") == 2020)
    df_month_2020 = df_month_2020.withColumn("Month", month("Start_Time"))
    df_month_2020 = df_month_2020.groupBy("Month").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Month")
    df_month_2020 = df_month_2020.withColumn("Month", month_udf(col("Month")))
    print(df_month_2020.show())
    df_month_2020.coalesce(1).write.csv('Graph_2_Month_2020', header=True, mode='overwrite')

    # GRAPH 2-2021: Accidents by Month_2021
    df_month_2021 = df_time.filter(year("Start_Time") == 2021)
    df_month_2021 = df_month_2021.withColumn("Month", month("Start_Time"))
    df_month_2021 = df_month_2021.groupBy("Month").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Month")
    df_month_2021 = df_month_2021.withColumn("Month", month_udf(col("Month")))
    print(df_month_2021.show())
    df_month_2021.coalesce(1).write.csv('Graph_2_Month_2021', header=True, mode='overwrite')

    # GRAPH 2-2022: Accidents by Month_2022
    df_month_2022 = df_time.filter(year("Start_Time") == 2022)
    df_month_2022 = df_month_2022.withColumn("Month", month("Start_Time"))
    df_month_2022 = df_month_2022.groupBy("Month").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Month")
    df_month_2022 = df_month_2022.withColumn("Month", month_udf(col("Month")))
    print(df_month_2022.show())
    df_month_2022.coalesce(1).write.csv('Graph_2_Month_2022', header=True, mode='overwrite')

    # GRAPH 2-2023: Accidents by Month_2023
    df_month_2023 = df_time.filter(year("Start_Time") == 2023)
    df_month_2023 = df_month_2023.withColumn("Month", month("Start_Time"))
    df_month_2023 = df_month_2023.groupBy("Month").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Month")
    df_month_2023 = df_month_2023.withColumn("Month", month_udf(col("Month")))
    print(df_month_2023.show())
    df_month_2023.coalesce(1).write.csv('Graph_2_Month_2023', header=True, mode='overwrite')

    # GET THE DAY OF WEEK NAME FROM THE NUMBER
    def get_day_of_week_name(day_num):
        return calendar.day_name[day_num - 1]

    day_of_week_udf = udf(get_day_of_week_name, StringType())

    # GRAPH 3: Accidents by Weekday
    df_weekday = df_time.withColumn("Day_of_Week", dayofweek("Start_Time"))
    df_weekday = df_weekday.groupBy("Day_of_Week").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Day_of_Week")
    df_weekday = df_weekday.withColumn("Day_of_Week", day_of_week_udf(col("Day_of_Week")))
    print(df_weekday.show())
    df_weekday.coalesce(1).write.csv('Graph_3_Weekday', header=True, mode='overwrite')

    # GRAPH 3-2016: Accidents by Weekday_2016
    df_weekday_2016 = df_time.filter(year("Start_Time") == 2016)
    df_weekday_2016 = df_weekday_2016.withColumn("Day_of_Week", dayofweek("Start_Time"))
    df_weekday_2016 = df_weekday_2016.groupBy("Day_of_Week").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Day_of_Week")
    df_weekday_2016 = df_weekday_2016.withColumn("Day_of_Week", day_of_week_udf(col("Day_of_Week")))
    print(df_weekday_2016.show())
    df_weekday_2016.coalesce(1).write.csv('Graph_3_Weekday_2016', header=True, mode='overwrite')

    # GRAPH 3-2017: Accidents by Weekday_2017
    df_weekday_2017 = df_time.filter(year("Start_Time") == 2017)
    df_weekday_2017 = df_weekday_2017.withColumn("Day_of_Week", dayofweek("Start_Time"))
    df_weekday_2017 = df_weekday_2017.groupBy("Day_of_Week").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Day_of_Week")
    df_weekday_2017 = df_weekday_2017.withColumn("Day_of_Week", day_of_week_udf(col("Day_of_Week")))
    print(df_weekday_2017.show())
    df_weekday_2017.coalesce(1).write.csv('Graph_3_Weekday_2017', header=True, mode='overwrite')

    # GRAPH 3-2018: Accidents by Weekday_2018
    df_weekday_2018 = df_time.filter(year("Start_Time") == 2018)
    df_weekday_2018 = df_weekday_2018.withColumn("Day_of_Week", dayofweek("Start_Time"))
    df_weekday_2018 = df_weekday_2018.groupBy("Day_of_Week").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Day_of_Week")
    df_weekday_2018 = df_weekday_2018.withColumn("Day_of_Week", day_of_week_udf(col("Day_of_Week")))
    print(df_weekday_2018.show())
    df_weekday_2018.coalesce(1).write.csv('Graph_3_Weekday_2018', header=True, mode='overwrite')

    # GRAPH 3-2019: Accidents by Weekday_2019
    df_weekday_2019 = df_time.filter(year("Start_Time") == 2019)
    df_weekday_2019 = df_weekday_2019.withColumn("Day_of_Week", dayofweek("Start_Time"))
    df_weekday_2019 = df_weekday_2019.groupBy("Day_of_Week").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Day_of_Week")
    df_weekday_2019 = df_weekday_2019.withColumn("Day_of_Week", day_of_week_udf(col("Day_of_Week")))
    print(df_weekday_2019.show())
    df_weekday_2019.coalesce(1).write.csv('Graph_3_Weekday_2019', header=True, mode='overwrite')

    # GRAPH 3-2020: Accidents by Weekday_2020
    df_weekday_2020 = df_time.filter(year("Start_Time") == 2020)
    df_weekday_2020 = df_weekday_2020.withColumn("Day_of_Week", dayofweek("Start_Time"))
    df_weekday_2020 = df_weekday_2020.groupBy("Day_of_Week").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Day_of_Week")
    df_weekday_2020 = df_weekday_2020.withColumn("Day_of_Week", day_of_week_udf(col("Day_of_Week")))
    print(df_weekday_2020.show())
    df_weekday_2020.coalesce(1).write.csv('Graph_3_Weekday_2020', header=True, mode='overwrite')

    # GRAPH 3-2021: Accidents by Weekday_2021
    df_weekday_2021 = df_time.filter(year("Start_Time") == 2021)
    df_weekday_2021 = df_weekday_2021.withColumn("Day_of_Week", dayofweek("Start_Time"))
    df_weekday_2021 = df_weekday_2021.groupBy("Day_of_Week").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Day_of_Week")
    df_weekday_2021 = df_weekday_2021.withColumn("Day_of_Week", day_of_week_udf(col("Day_of_Week")))
    print(df_weekday_2021.show())
    df_weekday_2021.coalesce(1).write.csv('Graph_3_Weekday_2021', header=True, mode='overwrite')

    # GRAPH 3-2022: Accidents by Weekday_2022
    df_weekday_2022 = df_time.filter(year("Start_Time") == 2022)
    df_weekday_2022 = df_weekday_2022.withColumn("Day_of_Week", dayofweek("Start_Time"))
    df_weekday_2022 = df_weekday_2022.groupBy("Day_of_Week").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Day_of_Week")
    df_weekday_2022 = df_weekday_2022.withColumn("Day_of_Week", day_of_week_udf(col("Day_of_Week")))
    print(df_weekday_2022.show())
    df_weekday_2022.coalesce(1).write.csv('Graph_3_Weekday_2022', header=True, mode='overwrite')

    # GRAPH 3-2023: Accidents by Weekday_2023
    df_weekday_2023 = df_time.filter(year("Start_Time") == 2023)
    df_weekday_2023 = df_weekday_2023.withColumn("Day_of_Week", dayofweek("Start_Time"))
    df_weekday_2023 = df_weekday_2023.groupBy("Day_of_Week").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Day_of_Week")
    df_weekday_2023 = df_weekday_2023.withColumn("Day_of_Week", day_of_week_udf(col("Day_of_Week")))
    print(df_weekday_2023.show())
    df_weekday_2023.coalesce(1).write.csv('Graph_3_Weekday_2023', header=True, mode='overwrite')

    # GRAPH 4: Accidents by Hour
    df_hour = df_time.withColumn("Hour", hour("Start_Time"))
    df_hour = df_hour.groupBy("Hour").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Hour")
    print(df_hour.show())
    df_hour.coalesce(1).write.csv('Graph_4_Hour', header=True, mode='overwrite')

    # GRAPH 4-2016: Accidents by Hour_2016
    df_hour_2016 = df_time.filter(year("Start_Time") == 2016)
    df_hour_2016 = df_hour_2016.withColumn("Hour", hour("Start_Time"))
    df_hour_2016 = df_hour_2016.groupBy("Hour").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Hour")
    print(df_hour_2016.show())
    df_hour_2016.coalesce(1).write.csv('Graph_4_Hour_2016', header=True, mode='overwrite')

    # GRAPH 4-2017: Accidents by Hour_2017
    df_hour_2017 = df_time.filter(year("Start_Time") == 2017)
    df_hour_2017 = df_hour_2017.withColumn("Hour", hour("Start_Time"))
    df_hour_2017 = df_hour_2017.groupBy("Hour").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Hour")
    print(df_hour_2017.show())
    df_hour_2017.coalesce(1).write.csv('Graph_4_Hour_2017', header=True, mode='overwrite')

    # GRAPH 4-2018: Accidents by Hour_2018
    df_hour_2018 = df_time.filter(year("Start_Time") == 2018)
    df_hour_2018 = df_hour_2018.withColumn("Hour", hour("Start_Time"))
    df_hour_2018 = df_hour_2018.groupBy("Hour").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Hour")
    print(df_hour_2018.show())
    df_hour_2018.coalesce(1).write.csv('Graph_4_Hour_2018', header=True, mode='overwrite')

    # GRAPH 4-2019: Accidents by Hour_2019
    df_hour_2019 = df_time.filter(year("Start_Time") == 2019)
    df_hour_2019 = df_hour_2019.withColumn("Hour", hour("Start_Time"))
    df_hour_2019 = df_hour_2019.groupBy("Hour").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Hour")
    print(df_hour_2019.show())
    df_hour_2019.coalesce(1).write.csv('Graph_4_Hour_2019', header=True, mode='overwrite')

    # GRAPH 4-2020: Accidents by Hour_2020
    df_hour_2020 = df_time.filter(year("Start_Time") == 2020)
    df_hour_2020 = df_hour_2020.withColumn("Hour", hour("Start_Time"))
    df_hour_2020 = df_hour_2020.groupBy("Hour").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Hour")
    print(df_hour_2020.show())
    df_hour_2020.coalesce(1).write.csv('Graph_4_Hour_2020', header=True, mode='overwrite')

    # GRAPH 4-2021: Accidents by Hour_2021
    df_hour_2021 = df_time.filter(year("Start_Time") == 2021)
    df_hour_2021 = df_hour_2021.withColumn("Hour", hour("Start_Time"))
    df_hour_2021 = df_hour_2021.groupBy("Hour").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Hour")
    print(df_hour_2021.show())
    df_hour_2021.coalesce(1).write.csv('Graph_4_Hour_2021', header=True, mode='overwrite')

    # GRAPH 4-2022: Accidents by Hour_2022
    df_hour_2022 = df_time.filter(year("Start_Time") == 2022)
    df_hour_2022 = df_hour_2022.withColumn("Hour", hour("Start_Time"))
    df_hour_2022 = df_hour_2022.groupBy("Hour").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Hour")
    print(df_hour_2022.show())
    df_hour_2022.coalesce(1).write.csv('Graph_4_Hour_2022', header=True, mode='overwrite')

    # GRAPH 4-2023: Accidents by Hour_2023
    df_hour_2023 = df_time.filter(year("Start_Time") == 2023)
    df_hour_2023 = df_hour_2023.withColumn("Hour", hour("Start_Time"))
    df_hour_2023 = df_hour_2023.groupBy("Hour").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Hour")
    print(df_hour_2023.show())
    df_hour_2023.coalesce(1).write.csv('Graph_4_Hour_2023', header=True, mode='overwrite')

    # GRAPH 5: Accidents by Severity
    df_Severity = df.filter(df["Severity"].isNotNull())
    df_Severity = df_Severity.groupBy("Severity").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Severity")
    print(df_Severity.show())
    df_Severity.coalesce(1).write.csv('Graph_5_Severity', header=True, mode='overwrite')

    # GRAPH 6: Accidents by State
    df_State = df.filter(df["State"].isNotNull())
    df_State = df_State.groupBy("State").count().withColumnRenamed(
        "count", "num_accidents").orderBy(col("num_accidents").desc())
    print(df_State.show())
    df_State.coalesce(1).write.csv('Graph_6_State', header=True, mode='overwrite')

    # GRAPH 7: Accidents by Severity in Top 10 States
    top_states = df.filter(col("Severity").isNotNull() & col("State").isNotNull())
    top_states = top_states.groupBy("State").count().withColumnRenamed(
        "count", "num_accidents").orderBy(col("num_accidents").desc()).limit(10)
    df_top_states = df.join(top_states.hint("broadcast"), "State")
    df_top_states = df_top_states.groupBy("Severity", "State").count().withColumnRenamed(
        "count", "num_accidents").orderBy(col("Severity").desc(), col("num_accidents").desc())
    print(df_top_states.show())
    df_top_states.coalesce(1).write.csv('Graph_7_Top10_States', header=True, mode='overwrite')

    # GRAPH 8: Accidents by City
    df_City = df.filter(df["City"].isNotNull())
    df_City = df_City.groupBy("City").count().withColumnRenamed(
        "count", "num_accidents").orderBy(col("num_accidents").desc())
    print(df_City.show())
    df_City.coalesce(1).write.csv('Graph_8_City', header=True, mode='overwrite')

    # GRAPH 9: Accidents by Severity in Top 20 Cities
    top_Cities = df.filter(col("Severity").isNotNull() & col("City").isNotNull())
    top_Cities = top_Cities.groupBy("City").count().withColumnRenamed(
        "count", "num_accidents").orderBy(col("num_accidents").desc()).limit(20)
    df_top_Cities = df.join(top_Cities.hint("broadcast"), "City")
    df_top_Cities = df_top_Cities.groupBy("Severity", "City").count().withColumnRenamed(
        "count", "num_accidents").orderBy(col("Severity").desc(), col("num_accidents").desc())
    print(df_top_Cities.show())
    df_top_Cities.coalesce(1).write.csv('Graph_9_Top20_Cities', header=True, mode='overwrite')

    # GRAPH 10: Accidents by County
    df_County = df.filter(df["County"].isNotNull())
    df_County = df_County.groupBy("County").count().withColumnRenamed(
        "count", "num_accidents").orderBy(col("num_accidents").desc())
    print(df_County.show())
    df_County.coalesce(1).write.csv('Graph_10_County', header=True, mode='overwrite')

    # GRAPH 11: Accidents by Start_Lat
    df_Start_Lat = df.filter(df["Start_Lat"].isNotNull())
    df_Start_Lat = df_Start_Lat.withColumn("Start_Lat_Category", floor(col("Start_Lat")))
    df_Start_Lat = df_Start_Lat.groupBy("Start_Lat_Category").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Start_Lat_Category")
    print(df_Start_Lat.show())
    df_Start_Lat.coalesce(1).write.csv('Graph_11_Start_Lat', header=True, mode='overwrite')

    # GRAPH 12: Accidents by Start_Lng
    df_Start_Lng = df.filter(df["Start_Lng"].isNotNull())
    df_Start_Lng = df_Start_Lng.withColumn("Start_Lng_Category", -ceil(-col("Start_Lng")))
    df_Start_Lng = df_Start_Lng.groupBy("Start_Lng_Category").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Start_Lng_Category")
    print(df_Start_Lng.show())
    df_Start_Lng.coalesce(1).write.csv('Graph_12_Start_Lng', header=True, mode='overwrite')

    # GRAPH 13: Accidents by Weather_Condition
    df_Weather_Condition = df.filter(df["Weather_Condition"].isNotNull())
    df_Weather_Condition = df_Weather_Condition.groupBy("Weather_Condition").count().withColumnRenamed(
        "count", "num_accidents").orderBy(col("num_accidents").desc())
    print(df_Weather_Condition.show())
    df_Weather_Condition.coalesce(1).write.csv('Graph_13_Weather_Condition', header=True, mode='overwrite')

    # GRAPH 14: Accidents by Top 10 Weather_Condition
    Top_Weather_Condition = df.filter(df["Weather_Condition"].isNotNull())
    Top_Weather_Condition = Top_Weather_Condition.groupBy("Weather_Condition").count().withColumnRenamed(
        "count", "num_accidents").orderBy(col("num_accidents").desc()).limit(10)
    print(Top_Weather_Condition.show())
    Top_Weather_Condition.coalesce(1).write.csv('Graph_14_Top10_Weather_Condition', header=True, mode='overwrite')

    # GRAPH 15: Accidents by Temperature(F)
    df_Temperature = df.filter(df["Temperature(F)"].isNotNull())
    bins = [-90, -80, -70, -60, -50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90,
            100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210]
    conditions = [
        (col("Temperature(F)") >= bins[i]) & (col("Temperature(F)") < bins[i + 1])
        for i in range(len(bins) - 1)
    ]
    expr = when(conditions[0], bins[0])
    for i in range(1, len(conditions)):
        expr = expr.when(conditions[i], bins[i])

    df_Temperature = df_Temperature.withColumn("Temperature_Category", expr)
    df_Temperature = df_Temperature.groupBy("Temperature_Category").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Temperature_Category")
    print(df_Temperature.show())
    df_Temperature.coalesce(1).write.csv('Graph_15_Temperature', header=True, mode='overwrite')

    # GRAPH 16: Accidents by Wind_Chill(F)
    df_Wind_Chill = df.filter(df["Wind_Chill(F)"].isNotNull())
    bins = [-90, -80, -70, -60, -50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90,
            100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210]
    conditions = [
        (col("Wind_Chill(F)") >= bins[i]) & (col("Wind_Chill(F)") < bins[i + 1])
        for i in range(len(bins) - 1)
    ]
    expr = when(conditions[0], bins[0])
    for i in range(1, len(conditions)):
        expr = expr.when(conditions[i], bins[i])

    df_Wind_Chill = df_Wind_Chill.withColumn("Wind_Chill_Category", expr)
    df_Wind_Chill = df_Wind_Chill.groupBy("Wind_Chill_Category").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Wind_Chill_Category")
    print(df_Wind_Chill.show())
    df_Wind_Chill.coalesce(1).write.csv('Graph_16_Wind_Chill', header=True, mode='overwrite')

    # GRAPH 17: Accidents by Humidity(%)
    df_Humidity = df.filter(df["Humidity(%)"].isNotNull())
    df_Humidity = df_Humidity.withColumn("Humidity_Category", (floor((col("Humidity(%)") - 1) / 10) * 10) + 1)
    df_Humidity = df_Humidity.groupBy("Humidity_Category").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Humidity_Category")
    print(df_Humidity.show())
    df_Humidity.coalesce(1).write.csv('Graph_17_Humidity', header=True, mode='overwrite')

    # GRAPH 18: Accidents by Pressure(in)
    df_Pressure = df.filter(df["Pressure(in)"].isNotNull())
    df_Pressure = df_Pressure.withColumn("Pressure_Category", (floor(col("Pressure(in)") / 10) * 10))
    df_Pressure = df_Pressure.groupBy("Pressure_Category").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Pressure_Category")
    print(df_Pressure.show())
    df_Pressure.coalesce(1).write.csv('Graph_18_Pressure', header=True, mode='overwrite')

    # GRAPH 19: Accidents by Visibility(mi)
    df_Visibility = df.filter(df["Visibility(mi)"].isNotNull())
    df_Visibility = df_Visibility.withColumn(
        "Visibility_Category",
        when(col("Visibility(mi)") < 5, 0)
        .when((col("Visibility(mi)") >= 5) & (col("Visibility(mi)") < 10), 5)
        .when((col("Visibility(mi)") >= 10) & (col("Visibility(mi)") < 15), 10)
        .when((col("Visibility(mi)") >= 15) & (col("Visibility(mi)") < 20), 15)
        .when((col("Visibility(mi)") >= 20) & (col("Visibility(mi)") < 25), 20)
        .when((col("Visibility(mi)") >= 25) & (col("Visibility(mi)") < 30), 25)
        .when((col("Visibility(mi)") >= 30) & (col("Visibility(mi)") < 35), 30)
        .when((col("Visibility(mi)") >= 35) & (col("Visibility(mi)") < 40), 35)
        .when((col("Visibility(mi)") >= 40) & (col("Visibility(mi)") < 45), 40)
        .when((col("Visibility(mi)") >= 45) & (col("Visibility(mi)") < 50), 45)
        .otherwise(50)
    )

    df_Visibility = df_Visibility.groupBy("Visibility_Category").count().withColumnRenamed(
        "count", "num_accidents").orderBy(col("Visibility_Category"))
    df_Visibility = df_Visibility.withColumn(
        "Visibility_Category",
        when(col("Visibility_Category") == 50, lit("50+")).otherwise(col("Visibility_Category").cast("string"))
    )
    print(df_Visibility.show())
    df_Visibility.coalesce(1).write.csv('Graph_19_Visibility', header=True, mode='overwrite')

    # GRAPH 20: Accidents by Wind_Speed(mph)
    df_Wind_Speed = df.filter(df["Wind_Speed(mph)"].isNotNull())
    df_Wind_Speed = df_Wind_Speed.withColumn("Wind_Speed_Category", floor(col("Wind_Speed(mph)") / 100) * 100)
    df_Wind_Speed = df_Wind_Speed.groupBy("Wind_Speed_Category").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Wind_Speed_Category")
    print(df_Wind_Speed.show())
    df_Wind_Speed.coalesce(1).write.csv('Graph_20_Wind_Speed', header=True, mode='overwrite')

    # GRAPH 21: Accidents by Precipitation(in)
    df_Precipitation = df.filter(df["Precipitation(in)"].isNotNull())
    df_Precipitation = df_Precipitation.withColumn("Precipitation_Category", floor(col("Precipitation(in)") / 5) * 5)
    df_Precipitation = df_Precipitation.groupBy("Precipitation_Category").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Precipitation_Category")
    print(df_Precipitation.show())
    df_Precipitation.coalesce(1).write.csv('Graph_21_Precipitation', header=True, mode='overwrite')

    # GRAPH 22: Accidents by Amenity
    df_Amenity = df.filter(df["Amenity"].isNotNull())
    df_Amenity = df_Amenity.groupBy("Amenity").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Amenity")
    print(df_Amenity.show())
    df_Amenity.coalesce(1).write.csv('Graph_22_Amenity', header=True, mode='overwrite')

    # GRAPH 23: Accidents by Bump
    df_Bump = df.filter(df["Bump"].isNotNull())
    df_Bump = df_Bump.groupBy("Bump").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Bump")
    print(df_Bump.show())
    df_Bump.coalesce(1).write.csv('Graph_23_Bump', header=True, mode='overwrite')

   # GRAPH 24: Accidents by Crossing
    df_Crossing = df.filter(df["Crossing"].isNotNull())
    df_Crossing = df_Crossing.groupBy("Crossing").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Crossing")
    print(df_Crossing.show())
    df_Crossing.coalesce(1).write.csv('Graph_24_Crossing', header=True, mode='overwrite')

   # GRAPH 25: Accidents by Give_Way
    df_Give_Way = df.filter(df["Give_Way"].isNotNull())
    df_Give_Way = df_Give_Way.groupBy("Give_Way").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Give_Way")
    print(df_Give_Way.show())
    df_Give_Way.coalesce(1).write.csv('Graph_25_Give_Way', header=True, mode='overwrite')

   # GRAPH 26: Accidents by Junction
    df_Junction = df.filter(df["Junction"].isNotNull())
    df_Junction = df_Junction.groupBy("Junction").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Junction")
    print(df_Junction.show())
    df_Junction.coalesce(1).write.csv('Graph_26_Junction', header=True, mode='overwrite')

   # GRAPH 27: Accidents by No_Exit
    df_No_Exit = df.filter(df["No_Exit"].isNotNull())
    df_No_Exit = df_No_Exit.groupBy("No_Exit").count().withColumnRenamed(
        "count", "num_accidents").orderBy("No_Exit")
    print(df_No_Exit.show())
    df_No_Exit.coalesce(1).write.csv('Graph_27_No_Exit', header=True, mode='overwrite')

   # GRAPH 28: Accidents by Railway
    df_Railway = df.filter(df["Railway"].isNotNull())
    df_Railway = df_Railway.groupBy("Railway").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Railway")
    print(df_Railway.show())
    df_Railway.coalesce(1).write.csv('Graph_28_Railway', header=True, mode='overwrite')

   # GRAPH 29: Accidents by Roundabout
    df_Roundabout = df.filter(df["Roundabout"].isNotNull())
    df_Roundabout = df_Roundabout.groupBy("Roundabout").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Roundabout")
    print(df_Roundabout.show())
    df_Roundabout.coalesce(1).write.csv('Graph_29_Roundabout', header=True, mode='overwrite')

   # GRAPH 30: Accidents by Station
    df_Station = df.filter(df["Station"].isNotNull())
    df_Station = df_Station.groupBy("Station").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Station")
    print(df_Station.show())
    df_Station.coalesce(1).write.csv('Graph_30_Station', header=True, mode='overwrite')

    # GRAPH 31: Accidents by Stop
    df_Stop = df.filter(df["Stop"].isNotNull())
    df_Stop = df_Stop.groupBy("Stop").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Stop")
    print(df_Stop.show())
    df_Stop.coalesce(1).write.csv('Graph_31_Stop', header=True, mode='overwrite')

    # GRAPH 32: Accidents by Traffic_Calming
    df_Traffic_Calming = df.filter(df["Traffic_Calming"].isNotNull())
    df_Traffic_Calming = df_Traffic_Calming.groupBy("Traffic_Calming").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Traffic_Calming")
    print(df_Traffic_Calming.show())
    df_Traffic_Calming.coalesce(1).write.csv('Graph_32_Traffic_Calming', header=True, mode='overwrite')

    # GRAPH 33: Accidents by Traffic_Signal
    df_Traffic_Signal = df.filter(df["Traffic_Signal"].isNotNull())
    df_Traffic_Signal = df_Traffic_Signal.groupBy("Traffic_Signal").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Traffic_Signal")
    print(df_Traffic_Signal.show())
    df_Traffic_Signal.coalesce(1).write.csv('Graph_33_Traffic_Signal', header=True, mode='overwrite')

    # GRAPH 34: Accidents by Turning_Loop
    df_Turning_Loop = df.filter(df["Turning_Loop"].isNotNull())
    df_Turning_Loop = df_Turning_Loop.groupBy("Turning_Loop").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Turning_Loop")
    print(df_Turning_Loop.show())
    df_Turning_Loop.coalesce(1).write.csv('Graph_34_Turning_Loop', header=True, mode='overwrite')

    # GRAPH 35: Accidents by Sunrise_Sunset
    df_Sunrise_Sunset = df.filter(df["Sunrise_Sunset"].isNotNull())
    df_Sunrise_Sunset = df_Sunrise_Sunset.groupBy("Sunrise_Sunset").count().withColumnRenamed(
        "count", "num_accidents").orderBy("Sunrise_Sunset")
    print(df_Sunrise_Sunset.show())
    df_Sunrise_Sunset.coalesce(1).write.csv('Graph_35_Sunrise_Sunset', header=True, mode='overwrite')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('TrafficAccidentsAnalysis').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    start_time = timer()
    main()
    spark.stop()
    end_time = timer()
    execution_time = end_time - start_time
    print("Execution time: {} seconds".format(execution_time))