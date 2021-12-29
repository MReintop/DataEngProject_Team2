import airflow
import datetime
import urllib.request as request
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import json

default_args_dict = {
    'start_date': datetime.datetime(2020, 6, 25, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

##### D A G   D E F I N I T I O N 
# pipeline zero is just a helper to create the database
pipeline0 = DAG(
    dag_id='pipeline0',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

### T A S K _ O N E
# Create a SQL clause to create table in Postgres DB
def _create_memes_table():
    with open("/opt/airflow/dags/create_memes_table.sql", "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS MEMES (\n"
            "ID serial PRIMARY KEY,\n"
            "TITLE VARCHAR,\n"
            "URL VARCHAR,\n"
            "TIME_UPDATED TIMESTAMP NOT NULL,\n"
            "IMAGE VARCHAR,\n"
            "IMAGE_WIDTH INT,\n"
            "IMAGE_HEIGHT INT,\n"
            "SOCIAL_MEDIA_DESCRIPTION VARCHAR,\n"
            "TIME_ADDED TIMESTAMP,\n"
            "STATUS VARCHAR,\n"
            "ORIGIN VARCHAR,\n"
            "MEME_YEAR INT,\n"
            "MEME_TYPE VARCHAR,\n"
            "ABOUT_TEXT VARCHAR,\n"
            "ABOUT_LINKS VARCHAR,\n"
            "ABOUT_IMAGES VARCHAR,\n"
            "ORIGIN_TEXT VARCHAR,\n"
            "ORIGIN_LINKS VARCHAR,\n"
            "ORIGIN_IMAGES VARCHAR,\n"
            "SPREAD_TEXT VARCHAR,\n"
            "SPREAD_LINKS VARCHAR,\n"
            "SPREAD_IMAGES VARCHAR,\n"
            "NOT_EXAMPLES_TEXT VARCHAR,\n"
            "NOT_EXAMPLES_LINKS VARCHAR,\n"
            "NOT_EXAMPLES_IMAGES VARCHAR,\n"
            "SEARCH_INTR_TEXT VARCHAR,\n"
            "SEARCH_INTR_LINKS VARCHAR,\n"
            "SEARCH_INTR_IMAGES VARCHAR,\n"
            "EXTERNAL_REF_TEXT VARCHAR,\n"
            "EXTERNAL_REF_LINKS VARCHAR,\n"
            "TAGS VARCHAR,\n" 
            "MEME_REFERENCES VARCHAR,\n"
            "KEYWORDS VARCHAR,\n"
            "PARENT VARCHAR);\n"
        )

        f.close()


task_one = PythonOperator(
    task_id='create_memes_table_query',
    dag=pipeline0,
    python_callable=_create_memes_table,
    op_kwargs={},
    trigger_rule='all_success'
)

### T A S K _ T W O
task_two = PostgresOperator(
    task_id='create_memes_table',
    dag=pipeline0,
    postgres_conn_id='postgres_default',
    sql='create_memes_table.sql',
    trigger_rule='none_failed',
    autocommit=True
)

end = DummyOperator(
    task_id='end_of_db_setup',
    dag=pipeline0,
    trigger_rule='none_failed'
)

# order of tasks
task_one >> task_two >> end