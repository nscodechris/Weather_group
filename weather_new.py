
import requests, os, configparser
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import text
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import psycopg2



pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

temp_list = []
air_pressure_list = []
precipitation_list = []

# pcat
new_temp_list = []
new_air_pressure_list = []
new_precipitation_list = []


CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
url = 'https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16.158/lat/58.5812/data.json'
response = requests.get(url)
dictr = response.json()



def _get_values():

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


def _list_to_pandas():

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
    df_final.to_csv(CURR_DIR_PATH + "/data/" + "weather.csv", index=False)
    df_final.to_json(CURR_DIR_PATH + "/data/" + "weather.json")
    # return df_final


def _pandas_to_database():
    df_weather = pd.read_json(CURR_DIR_PATH + "/data/" + "weather.json")
    engine = create_engine('postgresql://postgres:Cvmillan10!?@localhost:5432/weather_group')
    df_weather.to_sql('weathers', engine, if_exists='replace')
    # weather_group-# CREATE USER weather_user WITH PASSWORD 'weather_123";

with DAG("weather_group_dag", start_date=datetime(2022, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:
    get_values = PythonOperator(
        task_id="get_values",
        python_callable=_get_values
    )

    list_to_pandas = PythonOperator(
        task_id="list_to_harmonize",
        python_callable=_list_to_pandas
    )

    pandas_to_database = PythonOperator(
        task_id="pandas_to_database",
        python_callable=_pandas_to_database
    )
    get_values >> list_to_pandas >> pandas_to_database





