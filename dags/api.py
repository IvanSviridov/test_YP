import requests
import  pandas as pd
from pandas.io.json import json_normalize
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import datetime
import json


templates= {
  "from": "BTC", # базовая валюта
  "to": "USD",  # Минорная валюта
  "historical": {
    "active": 0, # Подгружаем ли исторические данные? 0 - нет, 1 - да
    "dt_from": '2020-01-01', # Начало интервала для исторических данных
    "dt_to" : '2020-01-04' # Конец интервала для исторических данных
  }
}

args = {
    'owner': 'user',
    'start_date': datetime.datetime(2021, 11, 1),
    'provide_context': True
}

def extract_data(**kwargs):
    ti = kwargs['ti']
    if templates["historical"]["active"] == 0: # не подгружаем исторические данные
        url = 'https://api.exchangerate.host/convert?'
        query_params = {
            "from": templates["from"],
            "to": templates["to"]
        }
        response = requests.get(url, query_params)
    elif templates["historical"]["active"] == 1: # подгружаем исторические данные
        url = f'https://api.exchangerate.host/timeseries?' \
              f'start_date={templates["historical"]["dt_from"]}&' \
              f'end_date={templates["historical"]["dt_to"]}'
        query_params = {
            "base": templates["from"],
            "symbols": templates["to"]}
        response = requests.get(url, query_params)
    if response:
        print('Response OK')
        json_data = response.json()
    else:
        print('Response Failed')
    ti.xcom_push(key='api_json', value=json_data)


def transform_data(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(key='api_json',
                             task_ids=['extract_data'])[0]
    # Normalizing data
    if templates["historical"]["active"] == 0: # В случае, если исторические данные не подгружаются
        FIELDS = ["query_from",
                  "query_to",
                  "info_rate",
                  "date"]
        res_df = json_normalize(json_data, sep="_")[FIELDS]
    elif templates["historical"]["active"] == 1:  # В случае, если подгружены исторические данные
        res_df = pd.DataFrame.from_dict(json_data['rates'], orient="index")
        res_df['query_from'] = 'BTC'
        res_df['query_to'] = 'USD'
        res_df['info_rate'] = res_df['USD']
        res_df['date'] = res_df.index
    #res_df['query_from'] = res_df['query_from'].map(lambda x: f"'{x}'")
    now = datetime.datetime.now()
    res_df['processed_dttm'] = now.strftime("%Y-%m-%d %H:%M")

    ti.xcom_push(key='api_df', value=res_df)


with DAG('airflow_YP', description='load_rates_from_api', schedule_interval='0 */3 * * *', catchup=False,
         default_args=args) as dag: #*/1 * * * *
    extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data)
    transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data)
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="database_PG",
        sql="""                 
                                CREATE TABLE IF NOT EXISTS rates (
                                query_from varchar(4) NOT NULL,
                                query_to varchar(4) NOT NULL,
                                info_rate FLOAT NOT NULL,
                                date TIMESTAMP NOT NULL,
                                processed_dttm TIMESTAMP NOT NULL
                                );
                            """
    )
    if templates["historical"]["active"] == 0: # В случае, если исторические данные не подгружаются
        k=1 # число строк в загружаемой таблице
    elif templates["historical"]["active"] == 1: # В случае, если подгружены исторические данные
        dt_to = templates["historical"]["dt_to"]
        dt_from = templates["historical"]["dt_from"]
        dt_to = dt_to.split('-')
        dt_from= dt_from.split('-')
        dt_to_dttm = datetime.date(int(dt_to[0]), int(dt_to[1]), int(dt_to[2]))
        dt_from_dttm = datetime.date(int(dt_from[0]), int(dt_from[1]), int(dt_from[2]))
        days = (dt_to_dttm - dt_from_dttm).days
        k = days # число строк в загружаемой таблице
    insert_in_table = PostgresOperator(
        task_id="insert_table",
        postgres_conn_id="database_PG",
        sql=[f"""INSERT INTO rates values (
                             '{{{{ti.xcom_pull(key='api_df', task_ids=['transform_data'])[0].iloc[{i}]['query_from']}}}}',
                            '{{{{ti.xcom_pull(key='api_df', task_ids=['transform_data'])[0].iloc[{i}]['query_to']}}}}',
                            '{{{{ti.xcom_pull(key='api_df', task_ids=['transform_data'])[0].iloc[{i}]['info_rate']}}}}',
                            '{{{{ti.xcom_pull(key='api_df', task_ids=['transform_data'])[0].iloc[{i}]['date']}}}}',
                            '{{{{ti.xcom_pull(key='api_df', task_ids=['transform_data'])[0].iloc[{i}]['processed_dttm']}}}}'
                            )""" for i in range(k)]
    )

    extract_data >> transform_data >> create_table >> insert_in_table
