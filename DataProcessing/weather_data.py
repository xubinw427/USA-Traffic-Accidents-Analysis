import pandas as pd
import csv
import time
from selenium import webdriver
import requests
import datetime
import csv
import os
from timezonefinder import TimezoneFinder
import pytz
import datetime



# 转换函数
def celsius_to_fahrenheit(celsius):
    if not celsius:
        return -1000
    return (celsius * 9 / 5) + 32


def hPa_to_inHg(hPa):
    if not hPa:
        return -1000
    return hPa * 0.02953


def kmh_to_mph(kmh):
    if not kmh:
        return -1000
    return kmh * 0.621371

def mm_to_inch(mm):
    if not mm:
        return -1000
    return mm / 25.4

def time_zone_mapping():
    hash_map = {}
    hash_map['US/Pacific'] = -7
    hash_map['US/Mountain'] = -6
    hash_map['US/Central'] = -6
    hash_map['US/Eastern'] = -4
    return hash_map
def air_zone_mapping(df):
    airport_timezone_mapping = {}
    for index, row in df.iterrows():
        airport_code = row['Airport_Code']
        timezone = row['Timezone']
        airport_timezone_mapping[airport_code] = timezone
    return airport_timezone_mapping


def get_data(section_code, date, number, total_number, airport_timezone_mapping, time_mapping):
    date = date.replace("-", "")
    url = f""

    re = requests.get(url)
    re.text
    time.sleep(0.1)
    import json
    obj = json.loads(re.text)

    try:
        obj_list = obj['observations']

        column1 = section_code
        column2 = date
        all_rows_data = []
        time_diff = time_mapping[airport_timezone_mapping[column1]]
        for i in range(len(obj_list)):
            cur_list = obj_list[i]
            # 转换和提取信息
            time_ = datetime.datetime.fromtimestamp(cur_list['valid_time_gmt'],
                                                      datetime.timezone(datetime.timedelta(hours=time_diff))).strftime(
                '%Y-%m-%d %H:%M:%S')
            temperature = celsius_to_fahrenheit(cur_list['temp'])
            dew_point = celsius_to_fahrenheit(cur_list['dewPt'])
            humidity = cur_list['rh']
            wind = cur_list['wdir_cardinal']
            wind_speed = round(kmh_to_mph(cur_list['wspd']),2)
            wind_gust = round(kmh_to_mph(cur_list['gust']), 2) if cur_list['gust'] else 0
            pressure = round(hPa_to_inHg(cur_list['pressure']), 2)
            precip = round(mm_to_inch(cur_list['precip_hrly']), 2)
            condition = cur_list['wx_phrase']
            all_rows_data.append(
                [column1, column2, time_, temperature, dew_point, humidity, wind, wind_speed, wind_gust, pressure,
                 precip, condition])

        print(section_code, date)
        print(all_rows_data)
        print(f"This is {number}th data, the percentage is {number / total_number}%")
        with open('weather_data.csv', 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerows(all_rows_data)
    except KeyError as e:
           print(f"Error: {e}")
           print(section_code, date)
           print(f"This is {number}th data, the percentage is {number / total_number}%")







def main():
    year = 2016
    file_path = 'weather_data.csv'

    if not os.path.exists(file_path):
        with open(file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(
                ['Column1', 'Column2', 'Time', 'Temperature', 'Dew Point', 'Humidity', 'Wind', 'Wind Speed',
                 'Wind Gust',
                 'Pressure', 'Precip Rate', 'Condition'])

    input_file_path = f'./data/data_by_hour_{year}.csv'

    # Read CSV
    df = pd.read_csv(input_file_path)

    df['Weather_Timestamp_day'] = pd.to_datetime(df['Weather_Timestamp']).dt.strftime('%Y-%m-%d')
    df = df.dropna(subset=['Airport_Code', 'Weather_Timestamp_day'])
    df['Start-time'] = pd.to_datetime(df['Start-time'])


    filtered_df = df[df['Airport_Code'] == 'K04W']


    # 建立unique的 时间和机场pair
    airport_weather_pairs = list(zip(df['Airport_Code'], df['Weather_Timestamp_day']))

    unique_pairs = list(set(airport_weather_pairs))

    airport_timezone_mapping =  air_zone_mapping(df)
    time_mapping = time_zone_mapping()
    unique_pairs_sorted = sorted(unique_pairs, key=lambda x: x[0])

    total_number = len(unique_pairs_sorted)

    start_index = 46018
    for i in range(start_index, total_number):
        cur_pair = unique_pairs_sorted[i]
        get_data(cur_pair[0], cur_pair[1], i, total_number, airport_timezone_mapping, time_mapping)


if __name__ == "__main__":
    main()





