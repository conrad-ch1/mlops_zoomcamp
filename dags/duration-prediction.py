from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
from sklearn.feature_extraction import DictVectorizer

def read_dataframe(year, month):
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet'
    df = pd.read_parquet(url)
    print(f'Read {len(df)} rows from {url}')

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)
    print(f'After filtering, {len(df)} rows remain')
    
    return df

def create_X(df, dv=None):
    categorical = ['PU_DO']
    numerical = ['trip_distance']
    dicts = df[categorical + numerical].to_dict(orient='records')

    if dv is None:
        dv = DictVectorizer(sparse=True)
        X = dv.fit_transform(dicts)
    else:
        X = dv.transform(dicts)

    return X, dv

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='nyc_taxi_duration_prediction',
    default_args=default_args,
    description='A DAG to predict NYC taxi trip durations',
    start_date=datetime(2023, 10, 1),
    schedule='@daily',
    catchup=False,
    tags=['nyc_taxi', 'duration_prediction'],
) as dag:

    # Define the start task
    start_task = EmptyOperator(task_id='start')

    # Define the PythonOperator task
    read_data_task = PythonOperator(
        task_id='read_data',
        python_callable=read_dataframe,
        op_kwargs={'year': 2023, 'month': 3},
    )

    # Define the end task    
    end_task = EmptyOperator(task_id='end')

    # Set task dependencies
    (start_task >> read_data_task >> end_task)