
import requests, os, configparser
import pandas as pd
import pprint
import json
from random import randint
from datetime import datetime
import matplotlib.pyplot as plt


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))

url = 'https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16.158/lat/58.5812/data.json'
# print(url)
r = requests.get(url)
jason_data = r.json()
# print(type(jason_data))
#print(jason_data)

# dict = json.loads(r.text)
# print(dict)
# intressant = dict['timeSeries']
#
# print(intressant)

# for i in range(len(intressant)):
#     element = intressant[i]
#     tidpunkt = element['validTime']
#     # print(i, element, tidpunkt)
#     # paus = input("Next?")

if r.status_code == 200:  # If connection is successful (200: http ok)
    json_data = r.json()  # Get result in json
    # print(jason_data)
    # Create a dictionary to represent the stored data
    # To view all accessible data see: https://openweathermap.org/current#current_JSON
    weather_list = []
    for i in range(len(json_data['timeSeries'])):

        weather_data = {
            "temperature": json_data['timeSeries'][i]["parameters"][10]['values'][0],
            "air pressure": json_data['timeSeries'][i]["parameters"][11]['values'][0],
            "precipitation": json_data['timeSeries'][i]["parameters"][3]['values'][0],
            "date": json_data['timeSeries'][i]['validTime']
        }
        weather_list.append(weather_data)
    # print(weather_list)
    # print(type(weather_list))
    df = pd.DataFrame(weather_list)
    print(df)
    to_day = datetime.now().strftime("%Y_%m_%d_%I_%M_%S_%p")

    df.to_csv(CURR_DIR_PATH + "/data/" + str(to_day) + "_" + ".csv", index=False)
    df.to_json(CURR_DIR_PATH + "/data/" + str(to_day) + "_" + ".json")
    # print(df)

def plot_weather():
    # reading the database
    # data = pd.read_csv(CURR_DIR_PATH + "/data/" + "2022_07_12_02_35_48_PM_Stockholm.csv")
    data = pd.read_json(CURR_DIR_PATH + "/data/" + "2022_07_12_02_35_48_PM_Stockholm.json")
    print(data)
    # Bar chart with day against tip
    plt.bar(data['main.temp_max'], data['main.temp_min'])
    #
    plt.title("Bar Chart")
    #
    # # Setting the X and Y labels
    plt.xlabel('Max_temp')
    plt.ylabel('Min_temp')
    #
    # # Adding the legends
    plt.show()

# plot_weather()

