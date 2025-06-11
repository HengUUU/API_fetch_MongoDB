# from airflow import DAG
# from airflow.providers.http.hooks.http import HttpHook
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.decorators import task
# from datetime import datetime, timedelta

# Latitude = '51.5074'
# Longtitude = '-0.1278'
# Post_Gres_Con_ID = "postgres_default"
# API_Conn_ID = 'open_meteo_api'

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2025, 6, 7),  # Equivalent to days_ago(1) for 2025-06-08
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# ## Dag
# with DAG(dag_id = 'etl_pipeline',
#         default_args = default_args,
#         schedule = '@daily',
#         catchup=False
# ) as dags:
    
#     @task()
#     def extract_weather_data():
#         """"Extract weather data from Open-meto API using Airflow Connection."""
#         http_hook = HttpHook(http_conn_id =API_Conn_ID, method = "GET")

#         endpoint =  f'/v1/forecast?latitude={Latitude}&longitude={Longtitude}&current_weather=true'
    
#         respond = http_hook.run(endpoint)

#         if respond.status_code == 200:
#             return respond.json()
#         else:
#             raise Exception(f"Failed to fetch weather data: {respond.status_code}")
        
#     @task()
#     def transform_weather_data(weather_data):
#         current_weather = weather_data['current_weather_units']
#         transform_data = {
#             'latitude': Latitude,
#             'longitude': Longtitude,
#             'temperature': str(current_weather['temperature']),
#             'windspeed': str(current_weather['windspeed']),
#             'winddirection': str(current_weather['winddirection']),
#             'weathercode': str(current_weather['weathercode'])
#         }

#         return transform_data
    
    
#     @task()
#     def load_weather_data(transform_data):
#         pg_hook = PostgresHook(postgres_conn_id = Post_Gres_Con_ID)
#         conn = pg_hook.get_conn()
#         cursor = conn.cursor()

#         # Create table if it doesn't exist
#         cursor.execute("""
#         CREATE TABLE IF NOT EXISTS weather_data (
#             latitude FLOAT,
#             longitude FLOAT,
#             temperature varchar(10),
#             windspeed varchar(10),
#             winddirection varchar(10),
#             weathercode varchar(10),
#             timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#         );
#         """)

#         # insert table
#         cursor.execute(
#             """INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
#         VALUES (%s, %s, %s, %s, %s, %s)""",
#         (
#             transform_data['latitude'],
#             transform_data['longitude'],
#             transform_data['temperature'],
#             transform_data['windspeed'],
#             transform_data['winddirection'],
#             transform_data['weathercode']
#         )
#         )

#         conn.commit()
#         cursor.close()

#     weather_data = extract_weather_data()
#     transformed_data = transform_weather_data(weather_data)
#     load_weather_data(transformed_data)