from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta
from pymongo import MongoClient
# Get the MongoDB connection
from airflow.providers.mongo.hooks.mongo import MongoHook



API_Conn_ID = 'visual_crossing_api'
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 7),  # Equivalent to days_ago(1) for 2025-06-08
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def get_weather_data_for_date(data):
    target_date = datetime.now().strftime("%Y-%m-%d")
    for day in data['days']:
        if day['datetime'] == target_date:
            return {
                'datetime': day['datetime'],
                'tempmax': day['tempmax'],
                'tempmin': day['tempmin'],
                'temp': day['temp'],
                'feelslikemax': day['feelslikemax'],
                'feelslikemin': day['feelslikemin'],
                'feelslike': day['feelslike'],
                'dew': day['dew'],
                'humidity': day['humidity'],
                'precip': day['precip'],
                'precipprob': day['precipprob'],
                'precipcover': day['precipcover'],
                'preciptype': day['preciptype'],
                'snow': day['snow'],
                'windgust': day['windgust'],
                'windspeed': day['windspeed'],
                'winddir': day['winddir'],
                'pressure': day['pressure'],
                'cloudcover': day['cloudcover'],
                'visibility': day['visibility'],
                'sunrise': day['sunrise'],
                'sunset': day['sunset'],
                'conditions': day['conditions'],
                'description': day['description']
            }
    return None  # Return None if the date is not found

# a function for inserting document in mongodb['days']
def insert_into_mongodb(data):
    mongo_hook = MongoHook(conn_id = "mongo_default")
    client = mongo_hook.get_conn()
    # access the database:
    db = client[mongo_hook.connection.schema]
    collection = db["weather_data"]
    # if isinstance(data, list):
    result = collection.insert_one(data) 
    return str(result.inserted_id)

## Dag
with DAG(dag_id = 'etl_pipeline_mongo',
        default_args = default_args,
        schedule = '@daily',
        catchup=False
) as dags:
    
    @task()
    def extract_weather_data():
        """"Extract weather data from Open-meto API using Airflow Connection."""
        http_hook = HttpHook(http_conn_id =API_Conn_ID, method = "GET")

        endpoint =  f'/VisualCrossingWebServices/rest/services/timeline/Phnom Penh?unitGroup=us&include=current&key=Q5CV5ESU8W9ZPCELDJ8C9YEDC&'
        respond = http_hook.run(endpoint)

        if respond.status_code == 200:
            return respond.json()
        else:
            raise Exception(f"Failed to fetch weather data: {respond.status_code}")
    
    @task()
    def transform_weather_data(weather):
        filtered_data = get_weather_data_for_date(weather)

        if filtered_data:
            return filtered_data
        else:
            raise Exception(f"Data Not Found.")
        
    @task()
    def load_data(weather):
        print("tranform",weather)
        filtered_data = insert_into_mongodb(weather)

        if filtered_data:
            return filtered_data
        else:
            raise Exception(f"Data Not Found.")



    weather_data = extract_weather_data()
    transform_data= transform_weather_data(weather_data)
    data_set = load_data(transform_data)