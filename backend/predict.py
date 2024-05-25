import pandas as pd
import xgboost as xgb
import joblib
from hotspots_pd import process_coordinates, add_input_info, feature_engineering_tree


def predict(lon, lat, temperature, humidity, condition, month, hour, weekday):
    data_dir = '/data'
    csv_files = ['2022_real_data.csv']

    pd_df = process_coordinates(data_dir, csv_files, lon, lat)
    pd_df = add_input_info(pd_df, temperature, humidity, condition, month, hour, weekday)
    pd_df = feature_engineering_tree(pd_df)

    loaded_model = xgb.Booster()
    # loaded_model.load_model('xgboost_model.json')
    loaded_model = joblib.load('xgb_model.joblib')

    # dpred = xgb.DMatrix(pd_df)
    dpred = pd_df
    predictions_labels = loaded_model.predict(dpred)
    predictions_proba = loaded_model.predict_proba(dpred)
    print(predictions_labels)
    print(predictions_proba[1:])


if __name__ == "__main__":
    predict(-74.00, 40.70, 50, 50, 'Rain', 4, 18, 1)