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

# Load your model
loaded_model = joblib.load('xgb_model.joblib')

# Plot feature importance
fig, ax = plt.subplots(figsize=(10, 8))
xgb.plot_importance(loaded_model, max_num_features=15, importance_type='gain', xlabel='Feature Gain', ylabel='Feature',
                    title='XGBoost Feature Importance', ax=ax, show_values=False)

# This will create a list of feature names in the order they appear in the plot
# which is sorted by importance
# The get_score method returns a dictionary {'f0': importance, 'f1': importance, ...}
# We sort this dictionary by its values (importance) and extract the keys (feature names)
sorted_features = [feature for feature, importance in sorted(loaded_model.get_booster().get_score(importance_type='gain').items(), key=lambda item: item[1], reverse=True)]

# Now we map the sorted feature names (which are 'f0', 'f1', ...) to the actual names from columns_list
# Ensure columns_list is correctly ordered corresponding to the 'f' numbers
feature_names = {f'f{i}': name for i, name in enumerate(columns_list)}

# Replace the labels
# ax.set_yticklabels([feature_names.get(feature, feature) for feature in sorted_features])
ax.set_yticklabels([feature_names.get(f'f{i}', f'f{i}') for i in range(15)])

# Ensure that we display all tick labels and rotate them for better visibility if necessary
ax.figure.canvas.draw()  # This is necessary to ensure that we have all the ticks after drawing
plt.xticks(rotation=90)  # Rotate the xtick labels if they overlap

# Show plot
plt.show()
