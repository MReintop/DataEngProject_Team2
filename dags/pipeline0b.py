import airflow
import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args_dict = {
    'start_date': datetime.datetime(2020, 6, 25, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

##### D A G   D E F I N I T I O N 
# pipeline zero is just a helper to create the database
pipeline0b = DAG(
    dag_id='pipeline0b',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)


### T A S K _ S T A R T
start = DummyOperator(
    task_id='start_of_db_setup',
    dag=pipeline0b,
    trigger_rule='none_failed'
)


### T A S K _ C R E A T E _ T A B L E _ L I N K
# Create a SQL clause to create table LINK in Postgres DB
def _create_table_link():
    with open("/opt/airflow/dags/create_table_link.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS LINK CASCADE;\n"
            
            "CREATE TABLE LINK (\n"
            "URL VARCHAR PRIMARY KEY,\n"
            "TITLE VARCHAR,\n"
            "CATEGORY VARCHAR,\n"
            "SOCIAL_MEDIA_DESCRIPTION VARCHAR,\n"
            "PARENT VARCHAR);\n"
        )

        f.close()


task_create_table_link = PythonOperator(
    task_id='create_table_link',
    dag=pipeline0b,
    python_callable=_create_table_link,
    op_kwargs={},
    trigger_rule='all_success'
)


### T A S K _ E X E C _ C R E A T E _ T A B L E _ L I N K
exec_create_table_link = PostgresOperator(
    task_id='exec_create_table_link',
    dag=pipeline0b,
    postgres_conn_id='postgres_default',
    sql='create_table_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)



### T A S K _ C R E A T E _ T A B L E _ M E M E _ L I N K 
# Create a SQL clause to create table MEME_LINK in Postgres DB
def _create_table_meme_link():
    with open("/opt/airflow/dags/create_table_meme_link.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS MEME_ABOUT_LINK;\n"
            "DROP TABLE IF EXISTS MEME_ORIGIN_LINK;\n"
            "DROP TABLE IF EXISTS MEME_SPREAD_LINK;\n"
            "DROP TABLE IF EXISTS MEME_NOTEX_LINK;\n"
            "DROP TABLE IF EXISTS MEME_SEARCHINT_LINK;\n"
            "DROP TABLE IF EXISTS MEME_EXTREF_LINK;\n"
            "DROP TABLE IF EXISTS MEME_LINK;\n"
            
            "CREATE TABLE MEME_LINK (\n"
            "MEME_URL VARCHAR,\n"
            "LINK VARCHAR, \n"
            "LINK_TITLE VARCHAR,\n"
            "SECTION VARCHAR,\n"
            "CONSTRAINT fk_meme\n"
                "FOREIGN KEY(MEME_URL)\n"
	                "REFERENCES MEME(URL),\n"
            "CONSTRAINT fk_link\n"
                "FOREIGN KEY(LINK)\n"
	                "REFERENCES LINK(URL));\n"
        )

        f.close()


task_create_table_meme_link = PythonOperator(
    task_id='create_table_meme_link',
    dag=pipeline0b,
    python_callable=_create_table_meme_link,
    op_kwargs={},
    trigger_rule='all_success'
)



### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E _ L I N K 
exec_create_table_meme_link = PostgresOperator(
    task_id='exec_create_table_meme_link',
    dag=pipeline0b,
    postgres_conn_id='postgres_default',
    sql='create_table_meme_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ E N D
end = DummyOperator(
    task_id='end_of_db_setup',
    dag=pipeline0b,
    trigger_rule='none_failed'
)

# order of tasks
start >> task_create_table_link >> exec_create_table_link >> task_create_table_meme_link >> exec_create_table_meme_link >> end