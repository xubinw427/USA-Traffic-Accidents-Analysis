import xgboost as xgb
import matplotlib.pyplot as plt
import joblib
import numpy as np
import pandas as pd
import os
from sklearn.model_selection import train_test_split

# Load your model
loaded_model = joblib.load('xgb_model.joblib')


rand_seed = 233
df = pd.read_csv(os.path.join('E:\\data', 'output_2016_tree.csv'))
columns_list = ['Temperature', 'Humidity', 'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit',
                'Railway', 'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop',
                'Start_Lat', 'Start_Lng', 'month', 'hour', 'weekday', 'Condition_Cloud', 'Condition_Cloudy',
                'Condition_Drizzle', 'Condition_Dust', 'Condition_Fair', 'Condition_Fog', 'Condition_Hail',
                'Condition_Haze', 'Condition_Mist', 'Condition_Mix', 'Condition_Nearby',
                'Condition_Precipitation', 'Condition_Rain', 'Condition_Shower', 'Condition_Sleet',
                'Condition_Smoke', 'Condition_Snow', 'Condition_Squalls', 'Condition_T-Storm',
                'Condition_Thunder', 'Condition_Tornado', 'Condition_Vicinity', 'Condition_Whirlwinds',
                'Condition_Windy', 'Condition_Grains'] + ['label']
df = df.reindex(columns=columns_list, fill_value=0)
df = df[sorted(df.columns)]
y = df['label'].values
X = df.drop('label', axis=1).values

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=rand_seed)

gb = loaded_model.get_booster()

import shap

# load JS visualization code to notebook
shap.initjs()

# use Tree SHAP explainer to explain the gradient boosting tree model
# you only need to explain and plot the first explaination
tree_explainer = shap.TreeExplainer(gb)
shap_values = tree_explainer.shap_values(pd.DataFrame(X_test[0, :].reshape(1, -1), columns=df.columns[:-1]))
# shap.force_plot(tree_explainer.expected_value, shap_values, pd.DataFrame(X_test[0, :].reshape(1, -1), columns=df.columns[:-1]))
shap.force_plot(tree_explainer.expected_value, shap_values[0], pd.DataFrame(X_test[0, :].reshape(1, -1), columns=df.columns[:-1]), show=True)
