
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

api_key = "3ab30abfa2f442f4f2ecc32a856dd88a"

city_name = "Stockholm" #you can ask for user input instead

#Let's get the city's coordinates (lat and lon)


url = f'https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}'
# print(url)
r = requests.get(url)
jason_data = r.json()
# print(type(jason_data))
# print(jason_data)


if r.status_code == 200:  # If connection is successful (200: http ok)
    json_data = r.json()  # Get result in json

    # Create a dictionary to represent the stored data
    # To view all accessible data see: https://openweathermap.org/current#current_JSON
    weather_data = {
        "weather": json_data["weather"][0],  # [0] because for some reason it's a single element list?
        "main": json_data["main"],
        "visibility": json_data["visibility"],
        "wind": json_data["wind"],
        "clouds": json_data["clouds"],
        "date_time": datetime.fromtimestamp(json_data["dt"])
    }
    # Flattens dictionaries (normalize) because a dataframe can't contain nested dictionaries
    # E.g. Internal dictionary {"weather": {"temp": 275, "max_temp": 289}}
    # becomes {"weather.temp": 275, "weather.max_temp", 289}
    weather_data = pd.json_normalize(weather_data)
    df = pd.DataFrame(weather_data)
    df["main.temp"] = df["main.temp"] - 273.15
    df["main.feels_like"] = df["main.feels_like"] - 273.15
    df["main.temp_min"] = df["main.temp_min"] - 273.15
    df["main.temp_max"] = df["main.temp_max"] - 273.15
    to_day = datetime.now().strftime("%Y_%m_%d_%I_%M_%S_%p")
    print(df)
    df2 = df[["date_time", "main.temp", "wind.speed", "main.pressure", "main.humidity"]]
    print(df2)

    # print("today", to_day)
    # df2.to_csv(CURR_DIR_PATH + "/data/" + str(to_day) + "_" + city_name + ".csv", index=False)
    # df2.to_json(CURR_DIR_PATH + "/data/" + str(to_day) + "_" + city_name + ".json")
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

