
import requests, os, configparser
import pandas as pd
import pprint
import json
from random import randint
from datetime import datetime
import matplotlib.pyplot as plt
from datetime import timedelta
import numpy as np
from pandas.io.json import json_normalize

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
# /api/category/pmp3g/version/2/geotype/multipoint/validtime/20151012T150000Z/parameter/t/leveltype/hl/level/2/data.json?with-geo=false
# Accept-Encoding: gzip
url = 'https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16.158/lat/58.5812/data.json'
print(url)
# r = requests.get(url)
# json_raw = r.json()

response = requests.get(url)
dictr = response.json()

# print(len(dictr['timeSeries']))
list_values = []
df_values = pd.json_normalize(list_values)

# itterate over the json file to but values excluding dates do pandas
for i in range(len(dictr['timeSeries'])):

    recs = dictr['timeSeries'][i]['parameters']
    print(recs)
    df_values = df_values.append(recs) #  append(recs)


recs2 = dictr['timeSeries']
df_time = pd.json_normalize(recs2)
# create dataframe with only date & time
valid_time = df_time["validTime"]


# Getting the temprature value from pandas
df_temp = df_values.loc[df_values['name'] == "t"]
# reset index for df_temp in dataframe
df_temp.reset_index(inplace=True)

# merge values with dates
df_temp_date = pd.merge(valid_time, df_temp, left_index=True, right_index=True)

# df_temp_date["values"] = df_temp_date["values"].apply(pd.to_numeric)
# print(df_temp_date)
# print(df_temp_date.dtypes)

# df_temp_date["values"] = df_temp_date["values", [float(i) for i in df_temp_date["values"]]]
# df_temp_date["values"].astype(float)
print(df_temp_date)


# print(df_temp_date)
# get todays date for saving
to_day = datetime.now().strftime("%Y_%m_%d_%I_%M_%S_%p")

# df to json
# df_temp_date.to_json(CURR_DIR_PATH + "/data/" + str(to_day) + "_" + ".json")
# df_temp_date.to_csv(CURR_DIR_PATH + "/data/" + str(to_day) + "_" + ".csv")


def get_raw():

    if response.status_code == 200:  # If connection is successful (200: http ok)
        json_data = response.json()  # Get result in json
        # print(jason_data)
        # Create a dictionary to represent the stored data
        # To view all accessible data see: https://openweathermap.org/current#current_JSON

        # print(df)
        weather_list = []
        for i in range(len(json_data['timeSeries'])):

            weather_data = {
                "temperature": json_data['timeSeries'][i]["parameters"][1]['values'][0],
                "air pressure": json_data['timeSeries'][i]["parameters"][11]['values'][0],
                "precipitation": json_data['timeSeries'][i]["parameters"][3]['values'][0],
                "date": json_data['timeSeries'][i]['validTime']
            }
            # print(json_data['timeSeries'][i]['parameters'][10])
            # print(json_data["timeSeries"][i]['parameters'][1]["name"])



            # result = [new_k for new_k in my_dictionary.items() if new_k[1] == new_val]
            # for x in json_data["timeSeries"][i]['parameters'][0]["name"]:
                # print(x)
             #print(json_data['timeSeries'][i]['parameters'][10])
            # print(json_data['timeSeries'][i]['parameters'][10])


            weather_list.append(weather_data)

        df = pd.DataFrame(weather_list)
        # print(df)
        to_day = datetime.now().strftime("%Y_%m_%d_%I_%M_%S_%p")

        # df.to_csv(CURR_DIR_PATH + "/data/" + str(to_day) + "_" + ".csv", index=False)
        # df.to_json(CURR_DIR_PATH + "/data/" + str(to_day) + "_" + ".json")



# get_raw()




def plot_weather():
    data = pd.read_json(CURR_DIR_PATH + "/data/" + "2022_07_13_03_38_33_PM_.json")
    df = pd.read_json(CURR_DIR_PATH + "/data/" + "2022_07_13_03_38_33_PM_.json")
    # print(df)
    # print(df)
    # print(type(data['date']))
    # days = data["date"].dt.day
    # days_unique = days.unique()
    # temp_mean = data["temperature"].mean()
    # print(days_unique)
    # print(days)
    # print(data["temperature"])
    # index_date = data["date"][0]
    # print(index_date)
    # dates_array = np.array(days)
    # print(dates_array)
    plt.plot(df["values"])
    # plt.plot(data["air pressure"])
    # plt.plot(days)

    # Adding Title to the Plot
    plt.title("Scatter Plot")

    # Setting the X and Y labels
    plt.xlabel('hours')
    plt.ylabel('temp')
    # plt.show()

# plot_weather()

