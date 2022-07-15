
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


class WeatherClass:
    def __init__(self):
        self.item_list = []
        self.air_pressure_list = []
        self.precipitation_list = []
        self.temp_list = []
        self.new_temp_list = []
        self.new_air_pressure_list = []
        self.new_precipitation_list = []
        self.CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
        self.url = 'https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16.158/lat/58.5812/data.json'
        self.response = requests.get(self.url)
        self.dictr = self.response.json()

    def get_values(self):

        # itterate over the json file to but values excluding dates do pandas
        for i in range(len(self.dictr['timeSeries'])):
            y = self.dictr['timeSeries'][i]['parameters']
            # print(y)
            for x in range(len(self.dictr['timeSeries'][i]['parameters'])):

                recs = self.dictr['timeSeries'][i]['parameters'][x]["name"]
                value_dict = self.dictr['timeSeries'][i]['parameters'][x]
                # the_value = dictr['timeSeries'][i]['parameters'][x]["values"]
               # print(the_value)
                #print(value_dict)
                if recs == "t":
                    self.temp_list.append(value_dict)

                elif recs == "msl":
                    self.air_pressure_list.append(value_dict)

                elif recs == "pcat":
                    self.precipitation_list.append(value_dict)

        for z in range(len(self.temp_list)):
            q = self.temp_list[z]["values"]
            for item in q:
                # print(item)
                # print(float(item))
                self.new_temp_list.append(float(item))

        for u in range(len(self.air_pressure_list)):
            w = self.air_pressure_list[u]["values"]
            for item in w:
                # print(float(item))
                self.new_air_pressure_list.append(float(item))

        for u in range(len(self.precipitation_list)):
            w = self.precipitation_list[u]["values"]
            for item in w:
                # print(float(item))
                self.new_precipitation_list.append(float(item))
        return self.new_temp_list, self.new_air_pressure_list, self.new_precipitation_list


    def list_to_pandas(self):
        WeatherClass.get_values(self)
        # gets the date
        recs2 = self.dictr['timeSeries']
        df_time = pd.json_normalize(recs2)
        # create dataframe with only date & time
        df_time["date"] = pd.to_datetime(df_time['validTime']).dt.strftime('%Y/%m/%d')
        df_time["hours"] = pd.to_datetime(df_time['validTime']).dt.time
         # print(df_time)
        # print(df_day['day'])
        # df_final_date = pd.merge(df_final_date, df, left_index=True, right_index=True)
        df_final_date = df_time[["date", "hours"]].copy()
        # print(df_final_date)

        # print(df_time['validTime'])
        # print(df_final_date)
        # print(df_final_date.dtypes)

        # print(len(temp_list))

        # concate list to
        df = pd.DataFrame([self.new_temp_list, self.new_air_pressure_list, self.new_precipitation_list])
        df = df.transpose()
        df.columns = ["temp", "air_pressure", "precipitation"]
        df_final = pd.merge(df_final_date, df, left_index=True, right_index=True)

        # print(df_final)
        df_final.to_csv(self.CURR_DIR_PATH + "/data/" + "weather.csv", index=False)
        df_final.to_json(self.CURR_DIR_PATH + "/data/" + "weather.json")
        # return df_final


    def pandas_to_database(self):
        df_weather = pd.read_json(self.CURR_DIR_PATH + "/data/" + "weather.json")
        engine = create_engine('postgresql://postgres:1234@localhost:5432/weather_group')
        df_weather.to_sql('weathers', engine, if_exists='replace')
        # weather_group-# CREATE USER weather_user WITH PASSWORD 'weather_123";


smhi = WeatherClass()
# smhi.list_to_pandas()


with DAG("weather_group_dag", start_date=datetime(2022, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:
    smhi.get_values = PythonOperator(
        task_id="get_values",
        python_callable=smhi.get_values
    )

    smhi.list_to_pandas = PythonOperator(
        task_id="list_to_pandas",
        python_callable=smhi.list_to_pandas
    )

    smhi.pandas_to_database = PythonOperator(
        task_id="pandas_to_database",
        python_callable=smhi.pandas_to_database
    )
    smhi.get_values >> smhi.list_to_pandas >> smhi.pandas_to_database




