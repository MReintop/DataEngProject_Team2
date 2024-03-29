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
pipeline0a = DAG(
    dag_id='pipeline0a',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)


### T A S K _ S T A R T
start = DummyOperator(
    task_id='start_of_db_setup',
    dag=pipeline0a,
    trigger_rule='none_failed'
)

### T A S K _ C R E A T E _ T A B L E _ M E M E
# Create a SQL clause to create table MEME in Postgres DB
def _create_table_meme():
    with open("/opt/airflow/dags/create_table_meme.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS MEME CASCADE;\n"
            
            "CREATE TABLE MEME (\n"
            "URL VARCHAR PRIMARY KEY,\n"
            "TITLE VARCHAR,\n"
            "TIME_UPDATED TIMESTAMP NOT NULL,\n"
            "IMAGE VARCHAR,\n"
            "SOCIAL_MEDIA_DESCRIPTION VARCHAR,\n"
            "TIME_ADDED TIMESTAMP,\n"
            "STATUS VARCHAR,\n"
            "ORIGIN VARCHAR,\n"
            "MEME_YEAR INT,\n"
            "KEYWORDS VARCHAR,\n"
            "PARENT VARCHAR);\n"
        )

        f.close()


task_create_table_meme = PythonOperator(
    task_id='create_table_meme',
    dag=pipeline0a,
    python_callable=_create_table_meme,
    op_kwargs={},
    trigger_rule='all_success'
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
    dag=pipeline0a,
    python_callable=_create_table_link,
    op_kwargs={},
    trigger_rule='all_success'
)


### T A S K _ C R E A T E _ T A B L E _ M E M E _ T E X T 
# Create a SQL clause to create table MEME_TEXT in Postgres DB
def _create_table_meme_text():
    with open("/opt/airflow/dags/create_table_meme_text.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS MEME_TEXT;\n"
            
            "CREATE TABLE MEME_TEXT (\n"
            "MEME_URL VARCHAR,\n"
            "ABOUT_TEXT VARCHAR,\n"
            "ORIGIN_TEXT VARCHAR,\n"
            "SPREAD_TEXT VARCHAR,\n"
            "NOT_EXAMPLES_TEXT VARCHAR,\n"
            "SEARCH_INTR_TEXT VARCHAR,\n"
            "EXTERNAL_REF_TEXT VARCHAR,\n"
            "CONSTRAINT fk_meme\n"
                "FOREIGN KEY(MEME_URL)\n"
	                "REFERENCES MEME(URL));\n"
        )

        f.close()


task_create_table_meme_text = PythonOperator(
    task_id='create_table_meme_text',
    dag=pipeline0a,
    python_callable=_create_table_meme_text,
    op_kwargs={},
    trigger_rule='all_success'
)
            

### T A S K _ C R E A T E _ T A B L E _ M E M E _ T A G
# Create a SQL clause to create table MEME_TAG in Postgres DB
def _create_table_meme_tag():
    with open("/opt/airflow/dags/create_table_meme_tag.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS MEME_TAG;\n"
            
            "CREATE TABLE MEME_TAG (\n"
            "MEME_URL VARCHAR,\n"
            "TAG VARCHAR,\n"
            "CONSTRAINT fk_meme\n"
                "FOREIGN KEY(MEME_URL)\n"
	                "REFERENCES MEME(URL));\n"
        )

        f.close()


task_create_table_meme_tag = PythonOperator(
    task_id='create_table_meme_tag',
    dag=pipeline0a,
    python_callable=_create_table_meme_tag,
    op_kwargs={},
    trigger_rule='all_success'
)



### T A S K _ C R E A T E _ T A B L E _ M E M E _ T Y P E
# Create a SQL clause to create table MEME_TYPE in Postgres DB
def _create_table_meme_type():
    with open("/opt/airflow/dags/create_table_meme_type.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS MEME_TYPE;\n"
            
            "CREATE TABLE MEME_TYPE (\n"
            "MEME_URL VARCHAR,\n"
            "TYPE VARCHAR,\n"
            "CONSTRAINT fk_meme\n"
                "FOREIGN KEY(MEME_URL)\n"
	                "REFERENCES MEME(URL));\n"
        )

        f.close()


task_create_table_meme_type = PythonOperator(
    task_id='create_table_meme_type',
    dag=pipeline0a,
    python_callable=_create_table_meme_type,
    op_kwargs={},
    trigger_rule='all_success'
)



### T A S K _ C R E A T E _ T A B L E _ M E M E _ R E F
# Create a SQL clause to create table MEME_TEXT in Postgres DB
def _create_table_meme_ref():
    with open("/opt/airflow/dags/create_table_meme_ref.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS MEME_REFERENCE;\n"
            
            "CREATE TABLE MEME_REFERENCE (\n"
            "MEME_URL VARCHAR,\n"
            "REFERENCE_NAME VARCHAR, \n"
            "REFERENCE_LINK VARCHAR,\n"
            "CONSTRAINT fk_meme\n"
                "FOREIGN KEY(MEME_URL)\n"
	                "REFERENCES MEME(URL));\n"
        )

        f.close()


task_create_table_meme_ref = PythonOperator(
    task_id='create_table_meme_ref',
    dag=pipeline0a,
    python_callable=_create_table_meme_ref,
    op_kwargs={},
    trigger_rule='all_success'
)


### T A S K _ C R E A T E _ T A B L E _ M E M E _ I M G
# Create a SQL clause to create table MEME_IMAGE in Postgres DB
def _create_table_meme_img():
    with open("/opt/airflow/dags/create_table_meme_img.sql", "w") as f:
        f.write(
            "DROP TABLE IF EXISTS MEME_IMAGE;\n"
            
            "CREATE TABLE MEME_IMAGE (\n"
            "MEME_URL VARCHAR,\n"
            "IMAGE_LINK VARCHAR, \n"
            "SECTION VARCHAR,\n"
            "CONSTRAINT fk_meme\n"
                "FOREIGN KEY(MEME_URL)\n"
	                "REFERENCES MEME(URL));\n"
        )

        f.close()


task_create_table_meme_img = PythonOperator(
    task_id='create_table_meme_img',
    dag=pipeline0a,
    python_callable=_create_table_meme_img,
    op_kwargs={},
    trigger_rule='all_success'
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
    dag=pipeline0a,
    python_callable=_create_table_meme_link,
    op_kwargs={},
    trigger_rule='all_success'
)



### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E
exec_create_table_meme = PostgresOperator(
    task_id='exec_create_table_meme',
    dag=pipeline0a,
    postgres_conn_id='postgres_default',
    sql='create_table_meme.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ E X E C _ C R E A T E _ T A B L E _ L I N K
exec_create_table_link = PostgresOperator(
    task_id='exec_create_table_link',
    dag=pipeline0a,
    postgres_conn_id='postgres_default',
    sql='create_table_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E _ T E X T
exec_create_table_meme_text = PostgresOperator(
    task_id='exec_create_table_meme_text',
    dag=pipeline0a,
    postgres_conn_id='postgres_default',
    sql='create_table_meme_text.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E _ T A G 
exec_create_table_meme_tag = PostgresOperator(
    task_id='exec_create_table_meme_tag',
    dag=pipeline0a,
    postgres_conn_id='postgres_default',
    sql='create_table_meme_tag.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E _ T Y P E
exec_create_table_meme_type = PostgresOperator(
    task_id='exec_create_table_meme_type',
    dag=pipeline0a,
    postgres_conn_id='postgres_default',
    sql='create_table_meme_type.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E _ R E F
exec_create_table_meme_ref = PostgresOperator(
    task_id='exec_create_table_meme_ref',
    dag=pipeline0a,
    postgres_conn_id='postgres_default',
    sql='create_table_meme_ref.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E _ I M G
exec_create_table_meme_img = PostgresOperator(
    task_id='exec_create_table_meme_img',
    dag=pipeline0a,
    postgres_conn_id='postgres_default',
    sql='create_table_meme_img.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ E X E C _ C R E A T E _ T A B L E _ M E M E _ L I N K 
exec_create_table_meme_link = PostgresOperator(
    task_id='exec_create_table_meme_link',
    dag=pipeline0a,
    postgres_conn_id='postgres_default',
    sql='create_table_meme_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)


### T A S K _ E N D
end = DummyOperator(
    task_id='end_of_db_setup',
    dag=pipeline0a,
    trigger_rule='none_failed'
)

# order of tasks
start >> task_create_table_meme >> exec_create_table_meme >> task_create_table_link >> exec_create_table_link >> task_create_table_meme_text
task_create_table_meme_text >> task_create_table_meme_tag >> task_create_table_meme_type >> task_create_table_meme_ref >> task_create_table_meme_img
task_create_table_meme_img >> task_create_table_meme_link 

task_create_table_meme_text >> exec_create_table_meme_text
task_create_table_meme_tag >> exec_create_table_meme_tag
task_create_table_meme_type >> exec_create_table_meme_type
task_create_table_meme_ref >> exec_create_table_meme_ref
task_create_table_meme_img >> exec_create_table_meme_img
task_create_table_meme_link >> exec_create_table_meme_link

[exec_create_table_meme,
 exec_create_table_link,
 exec_create_table_meme_text,
 exec_create_table_meme_tag,
 exec_create_table_meme_type,
 exec_create_table_meme_ref,
 exec_create_table_meme_img,
 exec_create_table_meme_link] >> end