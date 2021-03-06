
import requests, os, configparser
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy import text
# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.bash import BashOperator
import psycopg2


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class WeatherClass:
    def __init__(self, url):
        self.item_list = []
        self.air_pressure_list = []
        self.precipitation_list = []
        self.temp_list = []
        self.new_temp_list = []
        self.new_air_pressure_list = []
        self.new_precipitation_list = []
        self.CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
        self.url = url
        self.response = requests.get(self.url)
        self.dictr = self.response.json()

    # getting all values except date
    def get_values(self):

        # i gets the index in the json_file from key timeSeries, the key has a dict in a list as values
        for i in range(len(self.dictr['timeSeries'])):
            # getting all values from the key "timeSeries"
            # y = self.dictr['timeSeries'][i]['parameters']
            # x gets the index of the parameters key
            for x in range(len(self.dictr['timeSeries'][i]['parameters'])):
                # recs gets the values: like "hl", "t", "hmsl"
                recs = self.dictr['timeSeries'][i]['parameters'][x]["name"]
                # print(recs)
                # value_dict, get the values from the key parameters
                value_dict = self.dictr['timeSeries'][i]['parameters'][x]
                if recs == "t":
                    # appending the value from the parameter "parameters" with name value "t"
                    self.temp_list.append(value_dict)
                elif recs == "msl":
                    self.air_pressure_list.append(value_dict)
                elif recs == "pcat":
                    self.precipitation_list.append(value_dict)
        # break out the values in the list from key "values" converts list to float
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
        df_final_date = df_time[["date", "hours"]].copy()

        # creating new data_frame with values, from lists
        df = pd.DataFrame([self.new_temp_list, self.new_air_pressure_list, self.new_precipitation_list])
        df = df.transpose()
        df.columns = ["temp", "air_pressure", "precipitation"]
        df_final = pd.merge(df_final_date, df, left_index=True, right_index=True)
        # print(df_final)
        df_final.to_csv(self.CURR_DIR_PATH + "/data/" + "weather.csv", index=False)
        df_final.to_json(self.CURR_DIR_PATH + "/data/" + "weather.json")
        # return df_final

    # function for insert value from json, to pandas to database
    def pandas_to_database(self):
        df_weather = pd.read_json(self.CURR_DIR_PATH + "/data/" + "weather.json")
        engine = create_engine('postgresql://postgres:1234@localhost:5432/weather_group')
        df_weather.to_sql('weathers', engine, if_exists='replace')
        with engine.connect() as connection:
            connection.execute("ALTER TABLE weathers ALTER COLUMN date TYPE date;")
        # weather_group-# CREATE USER weather_user WITH PASSWORD 'weather_123";


smhi_norrkoping = WeatherClass('https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16.158/lat/58.5812/data.json')
# print(smhi_norrkoping.dictr)
smhi_norrkoping.get_values()
print(smhi_norrkoping.temp_list)

# smhi_norrkoping.list_to_pandas()
# smhi_norrkoping.pandas_to_database()


# with DAG("weather_group_dag", start_date=datetime(2022, 1, 1),
#          schedule_interval="@daily", catchup=False) as dag:
#     smhi_norrkoping.get_values = PythonOperator(
#         task_id="get_values",
#         python_callable=smhi_norrkoping.get_values
#     )
#
#     smhi_norrkoping.list_to_pandas = PythonOperator(
#         task_id="list_to_pandas",
#         python_callable=smhi_norrkoping.list_to_pandas
#     )
#
#     smhi_norrkoping.pandas_to_database = PythonOperator(
#         task_id="pandas_to_database",
#         python_callable=smhi_norrkoping.pandas_to_database
#     )
#     smhi_norrkoping.get_values >> smhi_norrkoping.list_to_pandas >> smhi_norrkoping.pandas_to_database



def line_chart():

    df = pd.read_json(smhi_norrkoping.CURR_DIR_PATH + "/data/" + "weather.json")
    # print(df)
    # reading the database
    data = pd.read_json(smhi_norrkoping.CURR_DIR_PATH + "/data/" + "weather.json")
    # print(data)
    # Scatter plot with day against tip
    collect_date = date(2022, 7, 19)
    collect_date_tmw = date(2022, 7, 20)

    plot_data = data[data['date'].dt.date == collect_date]
    plot_data_tmw = data[data['date'].dt.date == collect_date_tmw]
    # print(plot_data)
    hours = pd.to_datetime(plot_data["hours"]).dt.strftime('%H')
    hours_tmw = pd.to_datetime(plot_data_tmw["hours"]).dt.strftime('%H')

    plt.plot(hours, plot_data['temp'], label=collect_date)
    plt.plot(hours_tmw, plot_data_tmw['temp'], label=collect_date_tmw)
    plt.legend(loc="upper left")

    # plt.plot(data['precipitation'])



    # Adding Title to the Plot
    plt.title("Weather forecast")

    # Setting the X and Y labels
    plt.xlabel('hours')
    plt.ylabel('temp')

    plt.show()

line_chart()