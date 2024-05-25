import pandas as pd
import xgboost as xgb
import joblib
# from hotspots_pd import process_coordinates, add_input_info, feature_engineering_tree
import os
import numpy as np

np.random.seed(233)


def process_coordinates(data_dir, csv_files, lon, lat):
    lon_min, lon_max = lon - 0.02, lon + 0.02
    lat_min, lat_max = lat - 0.02, lat + 0.02

    dfs = []
    for file in csv_files:
        df = pd.read_csv(os.path.join(data_dir, file))
        dfs.append(df)

    merged_df = pd.concat(dfs, ignore_index=True)

    merged_df['round_lat'] = np.round(merged_df['Start_Lat'] / 0.02) * 0.02
    merged_df['round_lng'] = np.round(merged_df['Start_Lng'] / 0.02) * 0.02

    filtered_df = merged_df[(merged_df['Start_Lng'] >= lon_min) & (merged_df['Start_Lng'] <= lon_max) &
                            (merged_df['Start_Lat'] >= lat_min) & (merged_df['Start_Lat'] <= lat_max)]

    # Create a dictionary for aggregation to include additional attributes
    aggregation_dict = {
        'Start_Lat': 'first',
        'Start_Lng': 'first'
    }
    # Add additional attributes to the aggregation dictionary
    for feature in ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout',
                    'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']:
        aggregation_dict[feature] = 'first'
    # Group by rounded coordinates and aggregate
    grouped = filtered_df.groupby(['round_lat', 'round_lng']).agg(aggregation_dict).reset_index()
    grouped['num_records'] = filtered_df.groupby(['round_lat', 'round_lng']).size().values
    # Filter to get top 10 locations based on num_records
    top_locations = grouped.nlargest(10, 'num_records')
    # Select the required columns
    return_columns = ['Start_Lat', 'Start_Lng', 'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit',
                      'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
    return top_locations[return_columns]


def add_input_info(df_locations, temperature, humidity, condition, month, hour, weekday):
    df_locations['Temperature'] = temperature
    df_locations['Humidity'] = humidity
    df_locations['Condition'] = condition
    df_locations['month'] = month
    df_locations['hour'] = hour
    df_locations['weekday'] = weekday
    return df_locations


def feature_engineering_tree(df_locations):
    def process_string(s):
        words = s.split()
        return words[-1]

    df_locations['Condition'] = df_locations['Condition'].apply(process_string)
    df_encoded = pd.get_dummies(df_locations['Condition'], prefix='Condition')
    df_locations = pd.concat([df_locations, df_encoded], axis=1)
    df_locations.drop('Condition', axis=1, inplace=True)
    for item in df_encoded.columns:
        df_locations[item] = df_locations[item].astype(int)
    traffic_columns = ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout',
                       'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
    for item in traffic_columns:
        df_locations[item] = df_locations[item].astype(int)
    columns_list = ['Temperature', 'Humidity', 'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop', 'Start_Lat', 'Start_Lng', 'month', 'hour', 'weekday', 'Condition_Cloud', 'Condition_Cloudy', 'Condition_Drizzle', 'Condition_Dust', 'Condition_Fair', 'Condition_Fog', 'Condition_Hail', 'Condition_Haze', 'Condition_Mist', 'Condition_Mix', 'Condition_Nearby', 'Condition_Precipitation', 'Condition_Rain', 'Condition_Shower', 'Condition_Sleet', 'Condition_Smoke', 'Condition_Snow', 'Condition_Squalls', 'Condition_T-Storm', 'Condition_Thunder', 'Condition_Tornado', 'Condition_Vicinity', 'Condition_Whirlwinds', 'Condition_Windy', 'Condition_Grains']
    df_all = df_locations.reindex(columns=columns_list, fill_value=0)
    df_all = df_all[sorted(df_all.columns)]
    return df_all


def predict(lon, lat, temperature, humidity, condition, month, hour, weekday):
    data_dir = './Prediction/data'
    csv_files = ['2018_real_data.csv']

    pd_df = process_coordinates(data_dir, csv_files, lon, lat)
    pd_df = add_input_info(pd_df, temperature, humidity, condition, month, hour, weekday)
    pd_df = feature_engineering_tree(pd_df)
    location_df = pd_df[['Start_Lat', 'Start_Lng']]

    loaded_model = xgb.Booster()
    # loaded_model.load_model('xgboost_model.json')
    loaded_model = joblib.load('./Prediction/xgb_model.joblib')

    # dpred = xgb.DMatrix(pd_df)
    dpred = pd_df
    predictions_labels = loaded_model.predict(dpred)
    if predictions_labels.size == 0:
        return []
    predictions_proba = loaded_model.predict_proba(dpred)

    print(predictions_labels)
    print(predictions_proba[:, 1:])
    location_df = location_df.assign(New_Column=predictions_proba[:, 1])
    location_df.rename(columns={'Start_Lat': 'lat', 'Start_Lng': 'lng', 'New_Column':'prob'}, inplace=True)
    return location_df.to_json(orient='records')


if __name__ == "__main__":
    predict(-74.00, 40.70, 50, 50, 'Rain', 4, 18, 1)