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



### T A S K _ C R E A T E _ T A B L E _ M E M E _ A B O U T _ L I N K 
# Create a SQL clause to create table MEME_ABOUT_LINK in Postgres DB
def _create_table_meme_about_link():
    with open("/opt/airflow/dags/create_table_meme_about_link.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS MEME_ABOUT_LINK;\n"
            
            "CREATE TABLE MEME_ABOUT_LINK (\n"
            "MEME_URL VARCHAR,\n"
            "LINK VARCHAR, \n"
            "LINK_NAME VARCHAR,\n"
            "CONSTRAINT fk_meme\n"
                "FOREIGN KEY(MEME_URL)\n"
	                "REFERENCES MEME(URL),\n"
            "CONSTRAINT fk_link\n"
                "FOREIGN KEY(LINK)\n"
	                "REFERENCES LINK(URL));\n"
        )

        f.close()


task_create_table_meme_about_link = PythonOperator(
    task_id='create_table_meme_about_link',
    dag=pipeline0b,
    python_callable=_create_table_meme_about_link,
    op_kwargs={},
    trigger_rule='all_success'
)


### T A S K _ C R E A T E _ T A B L E _ M E M E _ O R I G I N _ L I N K 
# Create a SQL clause to create table MEME_ORIGIN_LINK in Postgres DB
def _create_table_meme_origin_link():
    with open("/opt/airflow/dags/create_table_meme_origin_link.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS MEME_ORIGIN_LINK;\n"
            
            "CREATE TABLE MEME_ORIGIN_LINK (\n"
            "MEME_URL VARCHAR,\n"
            "LINK VARCHAR, \n"
            "LINK_NAME VARCHAR,\n"
            "CONSTRAINT fk_meme\n"
                "FOREIGN KEY(MEME_URL)\n"
	                "REFERENCES MEME(URL),\n"
            "CONSTRAINT fk_link\n"
                "FOREIGN KEY(LINK)\n"
	                "REFERENCES LINK(URL));\n"
        )

        f.close()


task_create_table_meme_origin_link = PythonOperator(
    task_id='create_table_meme_origin_link',
    dag=pipeline0b,
    python_callable=_create_table_meme_origin_link,
    op_kwargs={},
    trigger_rule='all_success'
)


### T A S K _ C R E A T E _ T A B L E _ M E M E _ S P R E A D _ L I N K 
# Create a SQL clause to create table MEME_SPREAD_LINK in Postgres DB
def _create_table_meme_spread_link():
    with open("/opt/airflow/dags/create_table_meme_spread_link.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS MEME_SPREAD_LINK;\n"
            
            "CREATE TABLE MEME_SPREAD_LINK (\n"
            "MEME_URL VARCHAR,\n"
            "LINK VARCHAR, \n"
            "LINK_NAME VARCHAR,\n"
            "CONSTRAINT fk_meme\n"
                "FOREIGN KEY(MEME_URL)\n"
	                "REFERENCES MEME(URL),\n"
            "CONSTRAINT fk_link\n"
                "FOREIGN KEY(LINK)\n"
	                "REFERENCES LINK(URL));\n"
        )

        f.close()


task_create_table_meme_spread_link = PythonOperator(
    task_id='create_table_meme_spread_link',
    dag=pipeline0b,
    python_callable=_create_table_meme_spread_link,
    op_kwargs={},
    trigger_rule='all_success'
)



### T A S K _ C R E A T E _ T A B L E _ M E M E _ N O T E X _ L I N K 
# Create a SQL clause to create table MEME_NOTEX_LINK in Postgres DB
def _create_table_meme_notex_link():
    with open("/opt/airflow/dags/create_table_meme_notex_link.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS MEME_NOTEX_LINK;\n"
            
            "CREATE TABLE MEME_NOTEX_LINK (\n"
            "MEME_URL VARCHAR,\n"
            "LINK VARCHAR, \n"
            "LINK_NAME VARCHAR,\n"
            "CONSTRAINT fk_meme\n"
                "FOREIGN KEY(MEME_URL)\n"
	                "REFERENCES MEME(URL),\n"
            "CONSTRAINT fk_link\n"
                "FOREIGN KEY(LINK)\n"
	                "REFERENCES LINK(URL));\n"
        )

        f.close()


task_create_table_meme_notex_link = PythonOperator(
    task_id='create_table_meme_notex_link',
    dag=pipeline0b,
    python_callable=_create_table_meme_notex_link,
    op_kwargs={},
    trigger_rule='all_success'
)



### T A S K _ C R E A T E _ T A B L E _ M E M E _ S E A R C H I N T_ L I N K 
# Create a SQL clause to create table MEME_SEARCHINT_LINK in Postgres DB
def _create_table_meme_searchint_link():
    with open("/opt/airflow/dags/create_table_meme_searchint_link.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS MEME_SEARCHINT_LINK;\n"
            
            "CREATE TABLE MEME_SEARCHINT_LINK (\n"
            "MEME_URL VARCHAR,\n"
            "LINK VARCHAR, \n"
            "LINK_NAME VARCHAR,\n"
            "CONSTRAINT fk_meme\n"
                "FOREIGN KEY(MEME_URL)\n"
	                "REFERENCES MEME(URL),\n"
            "CONSTRAINT fk_link\n"
                "FOREIGN KEY(LINK)\n"
	                "REFERENCES LINK(URL));\n"
        )

        f.close()


task_create_table_meme_searchint_link = PythonOperator(
    task_id='create_table_meme_searchint_link',
    dag=pipeline0b,
    python_callable=_create_table_meme_searchint_link,
    op_kwargs={},
    trigger_rule='all_success'
)



### T A S K _ C R E A T E _ T A B L E _ M E M E _ E X T R E F _ L I N K 
# Create a SQL clause to create table MEME_EXTREF_LINK in Postgres DB
def _create_table_meme_extref_link():
    with open("/opt/airflow/dags/create_table_meme_extref_link.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS MEME_EXTREF_LINK;\n"
            
            "CREATE TABLE MEME_EXTREF_LINK (\n"
            "MEME_URL VARCHAR,\n"
            "LINK VARCHAR, \n"
            "LINK_NAME VARCHAR,\n"
            "CONSTRAINT fk_meme\n"
                "FOREIGN KEY(MEME_URL)\n"
	                "REFERENCES MEME(URL),\n"
            "CONSTRAINT fk_link\n"
                "FOREIGN KEY(LINK)\n"
	                "REFERENCES LINK(URL));\n"
        )

        f.close()


task_create_table_meme_extref_link = PythonOperator(
    task_id='create_table_meme_extref_link',
    dag=pipeline0b,
    python_callable=_create_table_meme_extref_link,
    op_kwargs={},
    trigger_rule='all_success'
)




### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E _ A B O U T _ L I N K 
exec_create_table_meme_about_link = PostgresOperator(
    task_id='exec_create_table_meme_about_link',
    dag=pipeline0b,
    postgres_conn_id='postgres_default',
    sql='create_table_meme_about_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E _ O R I G I N _ L I N K 
exec_create_table_meme_origin_link = PostgresOperator(
    task_id='exec_create_table_meme_origin_link',
    dag=pipeline0b,
    postgres_conn_id='postgres_default',
    sql='create_table_meme_origin_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E _ S P R E A D _ L I N K 
exec_create_table_meme_spread_link = PostgresOperator(
    task_id='exec_create_table_meme_spread_link',
    dag=pipeline0b,
    postgres_conn_id='postgres_default',
    sql='create_table_meme_spread_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)


### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E _ N O T E X _ L I N K 
exec_create_table_meme_notex_link = PostgresOperator(
    task_id='exec_create_table_meme_notex_link',
    dag=pipeline0b,
    postgres_conn_id='postgres_default',
    sql='create_table_meme_notex_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E _ S E A R C H I N T _ L I N K 
exec_create_table_meme_searchint_link = PostgresOperator(
    task_id='exec_create_table_meme_searchint_link',
    dag=pipeline0b,
    postgres_conn_id='postgres_default',
    sql='create_table_meme_searchint_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E _ E X T R E F _ L I N K 
exec_create_table_meme_extref_link = PostgresOperator(
    task_id='exec_create_table_meme_extref_link',
    dag=pipeline0b,
    postgres_conn_id='postgres_default',
    sql='create_table_meme_extref_link.sql',
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
start >> task_create_table_link >> exec_create_table_link

exec_create_table_link >> task_create_table_meme_about_link >> task_create_table_meme_origin_link >> task_create_table_meme_spread_link
task_create_table_meme_spread_link >> task_create_table_meme_notex_link >> task_create_table_meme_searchint_link >> task_create_table_meme_extref_link

task_create_table_meme_about_link >> exec_create_table_meme_about_link
task_create_table_meme_origin_link >> exec_create_table_meme_origin_link
task_create_table_meme_spread_link >> exec_create_table_meme_spread_link
task_create_table_meme_notex_link >> exec_create_table_meme_notex_link
task_create_table_meme_searchint_link >> exec_create_table_meme_searchint_link
task_create_table_meme_extref_link >> exec_create_table_meme_extref_link

[exec_create_table_meme_about_link,
 exec_create_table_meme_origin_link,
 exec_create_table_meme_spread_link,
 exec_create_table_meme_notex_link,
 exec_create_table_meme_searchint_link,
 exec_create_table_meme_extref_link] >> end