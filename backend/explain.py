import xgboost as xgb
import matplotlib.pyplot as plt
import joblib
import numpy as np

columns_list = ['Temperature', 'Humidity', 'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway',
                'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop', 'Start_Lat',
                'Start_Lng', 'month', 'hour', 'weekday', 'Condition_Cloud', 'Condition_Cloudy', 'Condition_Drizzle',
                'Condition_Dust', 'Condition_Fair', 'Condition_Fog', 'Condition_Hail', 'Condition_Haze',
                'Condition_Mist', 'Condition_Mix', 'Condition_Nearby', 'Condition_Precipitation', 'Condition_Rain',
                'Condition_Shower', 'Condition_Sleet', 'Condition_Smoke', 'Condition_Snow', 'Condition_Squalls',
                'Condition_T-Storm', 'Condition_Thunder', 'Condition_Tornado', 'Condition_Vicinity',
                'Condition_Whirlwinds', 'Condition_Windy', 'Condition_Grains']
columns_list = sorted(columns_list)
loaded_model = joblib.load('xgb_model.joblib')
# xgb.plot_importance(loaded_model)
xgb.plot_importance(loaded_model, importance_type='gain', xlabel='Feature Gain', ylabel='Feature',
                    title='XGBoost Feature Importance')
# plt.xticks(range(len(columns_list)), columns_list)
# ax.set_yticklabels(columns_list)
# ax.set_yticks(np.arange(len(columns_list)))
# ax.set_yticklabels(columns_list)
plt.show()
