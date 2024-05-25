import pandas as pd
#from pyspark.sql import SparkSession
import os
import csv


def merge_data(year, file_path_real,file_path_fake):

    input_file_path = f'./data/data_by_hour_{year}.csv'
    year_data = pd.read_csv(input_file_path)

    input_file_path2 = f'./{year}/weather_data.csv'
    weather_data = pd.read_csv(input_file_path2)

    # 获取列名并存储在列表中
    columns_list = list(year_data.columns)
    columns_list2 = list(weather_data.columns)
    print(columns_list2)
    # 打印列名列表
    print(columns_list)
    weather_data['Time'] = pd.to_datetime(weather_data['Time'])
    weather_data['Start-hour2'] = weather_data['Time'].dt.floor('H')
    print('.............')

    weather_data['Start-hour2'] = weather_data['Start-hour2'].astype(str)

    weather_data['Start-hour2'] = pd.to_datetime(weather_data['Start-hour2'])
    year_data['Start-hour'] = pd.to_datetime(year_data['Start-hour'])


    year_data['label'] = 1

    # 根据月份对数据进行分组
    weather_data['Month'] = weather_data['Start-hour2'].dt.month
    year_data['Month'] = year_data['Start-hour'].dt.month







    # real_data_list = pd.DataFrame()
    # fake_data_list = pd.DataFrame()


    for i in range(1, 13):
        print(i)
        data_subset = year_data[year_data['Month'] == i]
        year_data_month = year_data[year_data['Month'] == i]
        data_subset = data_subset[['Start_Lat', 'Start_Lng', 'Airport_Code']]
        df_1 = data_subset.drop_duplicates(subset=['Start_Lat', 'Start_Lng', 'Airport_Code']) # find the code and (lat, longtitude) pair
        unique_pairs = year_data_month[['Airport_Code', 'Start-hour']].drop_duplicates()

        weather_data_month = weather_data[weather_data['Month'] == i]

        weather_data_month_col = weather_data_month.columns.tolist()

        filtered_weather_data = pd.merge(unique_pairs, weather_data_month, left_on=['Airport_Code', 'Start-hour'], right_on = ['Column1', 'Start-hour2'], how='inner') #keep the time when there is an accident

        filtered_weather_data = filtered_weather_data[weather_data_month_col]

        extra_hours_daily = pd.DataFrame()
        for code in unique_pairs['Airport_Code'].unique():
            # find for one code, and the time is not accident time
            code_specific_rows = weather_data_month[weather_data_month['Column1'] == code]


            code_specific_filtered = year_data_month[year_data_month['Airport_Code'] == code]
            available_hours = code_specific_rows[
                ~code_specific_rows['Start-hour2'].isin(code_specific_filtered['Start-hour'])]



            for date, group in available_hours.groupby(available_hours['Start-hour2'].dt.date):
                selected_hours = group.sample(n=min(2, len(group)))
                if not selected_hours.empty:
                    extra_hours_daily = pd.concat([extra_hours_daily, selected_hours], ignore_index=True)



        final_weather_data = pd.concat([filtered_weather_data, extra_hours_daily], ignore_index=True).drop_duplicates()

        weather_data_month_position = pd.merge(df_1, final_weather_data, how='inner', left_on=['Airport_Code'], right_on=['Column1'])
        weather_data_month_position = weather_data_month_position.drop_duplicates(
            subset=['Start_Lat', 'Start_Lng', 'Start-hour2'])



        real_data_total = pd.merge(year_data_month, weather_data_month_position, how = 'inner', left_on = ['Start_Lat', 'Start_Lng', 'Start-hour'], right_on = ['Start_Lat', 'Start_Lng', 'Start-hour2'])


        address_infor = ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
        accident_infor = ['ID', 'Source', 'Severity', 'Start_Time', 'End_Time', 'Start_Lat', 'Start_Lng', 'End_Lat', 'End_Lng', 'Distance(mi)', 'Description', 'Street', 'City', 'County', 'State', 'Zipcode', 'Country', 'Timezone']
        weather_infor_year = ['Column1', 'Column2', 'Time', 'Temperature', 'Dew Point', 'Humidity', 'Wind', 'Wind Speed','Wind Gust','Pressure', 'Precip Rate', 'Condition', 'Start-hour2']
        useful_columns = weather_infor_year + address_infor + accident_infor + ['Month_y'] + ['label']

        real_data_total_columns = real_data_total.columns
        # print(real_data_total.columns)

        real_data = real_data_total[useful_columns]
        # real_data_list = pd.concat([real_data_list, real_data])
        real_data.to_csv(file_path_real, mode='a', index=False, header= False, encoding='utf-8', line_terminator='\n')

        #print(real_data_total)

        weather_data_month_position['day'] = pd.to_datetime(weather_data_month_position['Start-hour2']).dt.day
        year_data_month['day'] = pd.to_datetime(year_data_month['Start-hour']).dt.day

        day_info = pd.merge(weather_data_month_position, year_data_month, how = 'inner', left_on = ['Start_Lat', 'Start_Lng', 'day'], right_on = ['Start_Lat', 'Start_Lng', 'day'])
        day_info2 = day_info[(day_info['Start-hour2'] == day_info['Start-hour'])]
        day_info3 = day_info[(day_info['Start-hour2'] != day_info['Start-hour'])]

        useful_columns2 = weather_infor_year + address_infor + accident_infor + ['Month_y']
        fake_data = day_info3[useful_columns2]
        fake_data['label'] = 0
        fake_data = fake_data.drop_duplicates(subset=['Start_Lat', 'Start_Lng', 'Start-hour2'])
        accident_infor2 = ['ID', 'Source', 'Severity', 'Start_Time', 'End_Time', 'Distance(mi)', 'Description', 'Street', 'City', 'County', 'State', 'Zipcode', 'Country', 'Timezone']
        fake_data[accident_infor2] = None
        #fake_data_list = pd.concat([fake_data_list, fake_data])
        fake_data.to_csv(file_path_fake, mode='a', index=False, header= False, encoding='utf-8', line_terminator='\n')




    # fake_data_list.to_csv('fake_data_list2.csv', index=False)
    # real_data_list.to_csv('real_data_list2.csv',  index=False)



def main():
    year = 2020
    file_path_real = f'./result/{year}_real_data.csv'

    address_infor = ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout',
                     'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
    accident_infor = ['ID', 'Source', 'Severity', 'Start_Time', 'End_Time', 'Start_Lat', 'Start_Lng', 'End_Lat',
                      'End_Lng', 'Distance(mi)', 'Description', 'Street', 'City', 'County', 'State', 'Zipcode',
                      'Country', 'Timezone']
    weather_infor_year = ['Column1', 'Column2', 'Time', 'Temperature', 'Dew Point', 'Humidity', 'Wind', 'Wind Speed',
                          'Wind Gust', 'Pressure', 'Precip Rate', 'Condition', 'Start-hour2']
    useful_columns = weather_infor_year + address_infor + accident_infor + ['Month_y'] + ['label']

    if not os.path.exists(file_path_real):
        with open(file_path_real, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(useful_columns)


    file_path_fake = f'./result/{year}_fake_data.csv'

    if not os.path.exists(file_path_fake):
        with open(file_path_fake, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(useful_columns)
    merge_data(year, file_path_real, file_path_fake)


if __name__ == "__main__":
    main()















































