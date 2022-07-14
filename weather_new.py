
import requests, os, configparser
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine



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

temp_list = []
air_pressure_list = []
precipitation_list = []

# pcat
new_temp_list = []
new_air_pressure_list = []
new_precipitation_list = []

def get_values():

    # itterate over the json file to but values excluding dates do pandas
    for i in range(len(dictr['timeSeries'])):
        y = dictr['timeSeries'][i]['parameters']
        # print(y)
        for x in range(len(dictr['timeSeries'][i]['parameters'])):

            recs = dictr['timeSeries'][i]['parameters'][x]["name"]
            value_dict = dictr['timeSeries'][i]['parameters'][x]
            # the_value = dictr['timeSeries'][i]['parameters'][x]["values"]
           # print(the_value)
            #print(value_dict)
            if recs == "t":
                temp_list.append(value_dict)

            elif recs == "msl":
                air_pressure_list.append(value_dict)

            elif recs == "pcat":
                precipitation_list.append(value_dict)

    for z in range(len(temp_list)):
        q = temp_list[z]["values"]
        for item in q:
            # print(item)
            # print(float(item))
            new_temp_list.append(float(item))

    for u in range(len(air_pressure_list)):
        w = air_pressure_list[u]["values"]
        for item in w:
            # print(float(item))
            new_air_pressure_list.append(float(item))

    for u in range(len(precipitation_list)):
        w = precipitation_list[u]["values"]
        for item in w:
            # print(float(item))
            new_precipitation_list.append(float(item))




def list_to_pandas():

    # gets the date
    recs2 = dictr['timeSeries']
    df_time = pd.json_normalize(recs2)
    # create dataframe with only date & time
    df_final_date = df_time["validTime"]

    # print(len(temp_list))

    # concate list to
    df = pd.DataFrame([new_temp_list, new_air_pressure_list, new_precipitation_list])
    df = df.transpose()
    df.columns = ["temp", "air_pressure", "precipitation"]
    df_final = pd.merge(df_final_date, df, left_index=True, right_index=True)

    # print(df_final)
    to_day = datetime.now().strftime("%Y_%m_%d_%I_%M_%S_%p")
    # df.to_csv(CURR_DIR_PATH + "/data/" + str(to_day) + "_" + ".csv", index=False)
    # df.to_json(CURR_DIR_PATH + "/data/" + str(to_day) + "_" + ".json")
    return df_final


def plot_weather():
    data = pd.read_json(CURR_DIR_PATH + "/data/" + "2022_07_14_10_07_55_AM_.json")
    df = pd.read_json(CURR_DIR_PATH + "/data/" + "2022_07_14_10_07_55_AM_.json")
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
    plt.plot(df["temp"])
    plt.plot(df["air_pressure"])
    # plt.plot(days)

    # Adding Title to the Plot
    plt.title("Scatter Plot")

    # Setting the X and Y labels
    plt.xlabel('temp')
    plt.ylabel('air_pressure')
    plt.show()


def pandas_to_database(data_frame):

    engine = create_engine('postgresql://postgres:Cvmillan10!?@localhost:5432/weather_group')
    data_frame.to_sql('weathers', engine)
    # weather_group-# CREATE USER weather_user WITH PASSWORD 'weather_123";

# get_values()
# pandas_to_database(list_to_pandas())
# plot_weather()

