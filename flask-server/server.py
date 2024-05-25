from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from Prediction import predict
import math
import os
import pandas as pd
from datetime import datetime


is_development = True

# app = Flask(__name__, static_folder='../react-client/build') if is_development else Flask(__name__, static_folder='../cmpt733-client/build')
# app = Flask(__name__, static_folder='../react-client/build')
app = Flask(__name__)

CORS(app)
# CORS(app, resources={r"/*": {"origins": "http://52.9.248.230:3000"}})

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
  if path != "" and os.path.exists(app.static_folder + '/' + path):
    return send_from_directory(app.static_folder, path)
  else:
    return send_from_directory(app.static_folder, 'index.html')


def get_value(string_name, df):
    return ([value for value in df[string_name]])

def get_state_abbreviations(state_postcode_path, state_abbrev_path):
    state_postcode_df = pd.read_excel(state_postcode_path, header=None, names=['State', 'Latitude', 'Longitude'])
    
    state_postcode_df['State'] = state_postcode_df['State'].str.split(',').str[0]
    
    state_abbrev_df = pd.read_csv(state_abbrev_path)
    
    merged_df = state_postcode_df.merge(state_abbrev_df, how='left', left_on='State', right_on='State')
    
    final_df = merged_df[['Abbreviation', 'Latitude', 'Longitude']]
    
    final_df.rename(columns={'Abbreviation': 'State'}, inplace=True)

    graph_df = pd.read_csv("../DataAnalyzing/CSV_Files/Graph_6_State.csv") if is_development else pd.read_csv("../cmpt733-client/DataAnalyzing/CSV_Files/Graph_6_State.csv")
    
    graph_df.rename(columns={'State': 'State'}, inplace=True)
    
    merged_df = final_df.merge(graph_df, on='State', how='left')
    
    return merged_df

@app.route('/api/location', methods=['POST'])
def get_locations():
    data = request.get_json()
    lat = data.get('lat')
    lng = data.get('lng')
    humidity = float(data.get('humidity'))
    temperature = float(data.get('temperature'))
    condition = data.get('condition')
    date = data.get('date')
    datetime_obj = datetime.strptime(date, "%Y-%m-%dT%H:%M")
    
    # Extract components
    month = datetime_obj.month
    hour = datetime_obj.hour
    weekday = datetime_obj.weekday()
    print("Lat: ", lat, 
          ", Long: ", lng, 
          ", Humidity: ", humidity, 
          ", Date: ", date, 
          ", Month: ", month, 
          ", hour: ", hour, 
          ", weekday: ", weekday, 
          ", Temperature: ", temperature, 
          ", Weather Condition: ", condition, 
          " Received.")
    # This endpoint now just responds with a set of hardcoded coordinates
    coordinates = predict.predict(lng, lat, temperature, humidity, condition, month, hour, weekday)
    print('Success:', coordinates) 
    return jsonify(coordinates)

dic_path = "../DataAnalyzing" if is_development else "../cmpt733-client/DataAnalyzing"
file_path = dic_path+"/CSV_Files/"

@app.route("/lineChartData/Year")
def getLineChartDataYear():
  path1 = "Graph_1_Year.csv"
  full_path = os.path.join(file_path, path1)
  df = pd.read_csv(full_path)
  
  return ({"label": get_value('Year', df),
          "attribute": get_value('num_accidents', df)})

@app.route("/lineChartData/Month")
def getLineChartDataMonth():
  years = range(2016, 2023)  
  data = {}

  for year in years:
        path = f"Graph_2_Month_{year}.csv"
        full_path = os.path.join(file_path, path)
        df = pd.read_csv(full_path)

        if year == 2016: 
            data["label"] = df['Month'].tolist()
        
        data[f"attribute_{year}"] = df['num_accidents'].tolist()
  return data

@app.route("/lineChartData/Week")
def getLineChartDataWeek():
  years = range(2016, 2023)  
  data = {}

  for year in years:
        path = f"Graph_3_Weekday_{year}.csv"
        full_path = os.path.join(file_path, path)
        df = pd.read_csv(full_path)

        if year == 2016: 
            data["label"] = df['Day_of_Week'].tolist()
        
        data[f"attribute_{year}"] = df['num_accidents'].tolist()
  return data

@app.route("/lineChartData/Hour")
def getLineChartDataHour():
  years = range(2016, 2023)  
  data = {}

  for year in years:
        path = f"Graph_4_Hour_{year}.csv"
        full_path = os.path.join(file_path, path)
        df = pd.read_csv(full_path)

        if year == 2016: 
            data["label"] = df['Hour'].tolist()
        
        data[f"attribute_{year}"] = df['num_accidents'].tolist()
  return data

@app.route("/barChartData/Temperature")
def getBarChartDataTemp():
  path2 = "Graph_15_Temperature.csv"
  full_path = os.path.join(file_path, path2)
  df = pd.read_csv(full_path)
  
  return ({"label": get_value('Temperature_Category', df),
          "data": get_value('num_accidents', df),
          })

@app.route("/barChartData/WindChill")
def getBarChartDataWindChill():
  path2 = "Graph_16_Wind_Chill.csv"
  full_path = os.path.join(file_path, path2)
  df = pd.read_csv(full_path)
  
  return ({"label": get_value('Wind_Chill_Category', df),
          "data": get_value('num_accidents', df),
          })

@app.route("/barChartData/Humidity")
def getBarhartDataHumidity():
  path2 = "Graph_17_Humidity.csv"
  full_path = os.path.join(file_path, path2)
  df = pd.read_csv(full_path)
  
  return ({"label": get_value('Humidity_Category', df),
          "data": get_value('num_accidents', df),
          })

@app.route("/barChartData/Visibility")
def getBarChartDataVisibility():
  path2 = "Graph_19_Visibility.csv"
  full_path = os.path.join(file_path, path2)
  df = pd.read_csv(full_path)
  
  return ({"label": get_value('Visibility_Category', df),
          "data": get_value('num_accidents', df),
          })

@app.route("/barChartData/Road")
def getBarChartDataEnvironment():
    label_list = []
    data_list = []
    file_list = ["Graph_22_Amenity.csv", "Graph_23_Bump.csv", "Graph_24_Crossing.csv", "Graph_25_GiveWay.csv", "Graph_26_Junction.csv", "Graph_27_NoExit.csv", 
                 "Graph_28_Railway.csv", "Graph_31_Stop.csv", "Graph_33_TrafficSignal.csv"]

    for file_name in file_list:
        full_path = os.path.join(file_path, file_name)
        # print(full_path)
        df = pd.read_csv(full_path)
        # print(df)
        filename_without_extension = file_name.split('.csv')[0]

        parts = filename_without_extension.split('_')
        label_list.append(parts[-1] + '0')
        label_list.append(parts[-1] + '1')
        data_list.append(get_value('num_accidents', df))
        flattened_list = [item for sublist in data_list for item in sublist]
  
        unique_labels = list(set(label[:-1] for label in label_list))
        data1 = [flattened_list[i] for i in range(len(flattened_list)) if label_list[i].endswith('0')]
        data = [flattened_list[i] for i in range(len(flattened_list)) if label_list[i].endswith('1')]

    return({
        "label": unique_labels,
        "data": data,
        })

@app.route("/graphMap/state")
def getStateGraph():
  state_df  = get_state_abbreviations("../DataAnalyzing/state_postcode.xlsx", "../DataAnalyzing/states_abbrev.csv") if is_development else get_state_abbreviations("../cmpt733-client/DataAnalyzing/state_postcode.xlsx", "../cmpt733-client/DataAnalyzing/states_abbrev.csv")
  data = []
  for index, row in state_df.iterrows():
        if not math.isnan(row['num_accidents']):
            data.append({
                'id': index + 1,
                'lat': row['Latitude'],
                'lng': row['Longitude'],
                'state': row['State'],
                'accuracy': row['num_accidents']
            })
    
  return jsonify({'data': data})


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)
