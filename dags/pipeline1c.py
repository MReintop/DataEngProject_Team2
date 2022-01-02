# imports
import airflow
import datetime
import urllib.request as request
from urllib.request import Request, urlopen
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import glob


default_args_dict = {
    'start_date': datetime.datetime(2020, 6, 25, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

##### D A G   D E F I N I T I O N 
# pipeline c
# imports all categories from staging area
# cleanses data
# transforms the data into correct formats: int, string, timestamp, etc.
pipeline1c = DAG(
    dag_id='pipeline1c',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

# get the latest flattened.csv file
def get_latest_flattened_csv():
    csv_files = glob.glob("/opt/airflow/dags/*_flattened.csv")
    times = []
    for file in csv_files:
        times.append(int(file.split("/opt/airflow/dags/")[1].split("_flattened.csv")[0]))
    latest_time = max(times)
    latest_file = "/opt/airflow/dags/" + str(latest_time) + "_flattened.csv"
    return latest_file



### T A S K _ S E L E C T _ L I N K 
# select all categories and delete high level duplicates
def _select_link(epoch: int, output_folder: str):
    df = pd.read_csv(get_latest_flattened_csv())
    #df = df[df.Category!="Meme"]
    df = df[['Title', 'URL', 'TimeUpdated', 'Category', 'Image', 'TimeAdded',
             'Keywords', 'Parent', 'SocialMediaDescription', 'Status', 'Origin', 'Year']]
    df = df.drop_duplicates(subset='Title', keep='first')
    df.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_link.csv',index=False)

task_select_link = PythonOperator(
    task_id='select_link', 
    dag=pipeline1c,
    python_callable=_select_link,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
)




### T A S K _ F I V E
# to be repeatable(?) lets select only new rows if there are in the file
task_five = DummyOperator(
    task_id='select_new_links',
    dag=pipeline1c,
    trigger_rule='none_failed'
)



### T A S K _ E M P T I N E S S _ C H K  
# check if no new rows (if no new rows then go forward to the end)
def _emptiness_check(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_link.csv')
    length = len(df.index)
    if length == 0:
        return 'end'
    else:
        return 'format_fields'


task_emptiness_chk = BranchPythonOperator(
    task_id='emptiness_check',
    dag=pipeline1c,
    python_callable=_emptiness_check,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)


### T A S K _ F O R M A T
# format the time fields (time_added,time_updated)
# format the string and int fields (.astype)

# convert epoch time to readable date time
def format_time(col_name):
   formatted_date_time=pd.to_datetime(col_name.apply(datetime.datetime.fromtimestamp))
   return formatted_date_time

def repair_string(df, col_name):
    df[col_name] = df[col_name].astype("str")
    df[col_name] = df.apply(
        lambda row: 
            row[col_name].replace('"','').replace("'","").replace("`","").replace("\\","").replace("\n","").replace("{{","{").replace("}}","}").replace("’","").replace("{#","(#").strip(),axis=1)
    return df[col_name]

def _format_fields(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_link.csv')
    df["Title"] = df["Title"].astype("str")
    df["URL"] = df["URL"].astype("str")
    # time:
    df['TimeUpdated'] = df['TimeUpdated'].fillna(-2208988800)
    df['TimeUpdated'] = format_time(df['TimeUpdated'])
    df['TimeAdded'] = df['TimeAdded'].fillna(-2208988800)
    df['TimeAdded'] = format_time(df['TimeAdded'])
    # int:
    df['Year'] = df['Year'].fillna(1000).astype("int64")
    # remove bad symbols from string objects:
    columns = ['Title', 'URL', 'Category', 'Image',
             'Keywords', 'Parent', 'SocialMediaDescription', 'Status', 'Origin']
    for col in columns: df[col] = repair_string(df,col)
    df = df[['URL','Title', 'Category', 'TimeUpdated', 'Image', 'TimeAdded', 
            'Keywords', 'Parent', 'SocialMediaDescription', 'Status', 'Origin', 'Year']]
    df["Keywords"] = df.apply(lambda row: str(row["Keywords"]).replace("[","").replace("]","").strip(),axis=1)
    df = df.drop_duplicates()
    df.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_link.csv',index=False)

task_format = PythonOperator(
    task_id='format_fields',
    dag=pipeline1c,
    python_callable=_format_fields,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)


### T A S K _ L I N K _ Q U E R Y
# Create a SQL query for inserting LINK categories data to Postgres DB
def _link_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_link.csv')
    with open("/opt/airflow/dags/insert_link.sql", "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            title = row['Title']
            url = row['URL']
            category = row['Category']
            time_updated = row['TimeUpdated']
            image = row['Image']
            time_added = row['TimeAdded']
            keywords = row['Keywords']
            parent = row['Parent']
            social_media_description = row['SocialMediaDescription']
            status = row['Status']
            origin = row['Origin']
            year = row['Year']

            f.write(
                "INSERT INTO LINK VALUES ("
                f""" '{url}', '{title}', '{category}', '{social_media_description}', '{parent}'
                );\n"""
            )

        f.close()

task_link_query = PythonOperator(
    task_id='link_query',
    dag=pipeline1c,
    python_callable=_link_query,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)

### T A S K _ I N S E R T _ L I N K  
task_insert_link = PostgresOperator(
    task_id='insert_link',
    dag=pipeline1c,
    postgres_conn_id='postgres_default',
    sql='insert_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ I N S E R T _ M E M E _ A B O U T _ L I N K
task_insert_meme_about_link = PostgresOperator(
    task_id='insert_meme_about_link',
    dag=pipeline1c,
    postgres_conn_id='postgres_default',
    sql='insert_meme_about_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ I N S E R T _ M E M E _ O R I G I N _ L I N K
task_insert_meme_origin_link = PostgresOperator(
    task_id='insert_meme_origin_link',
    dag=pipeline1c,
    postgres_conn_id='postgres_default',
    sql='insert_meme_origin_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ I N S E R T _ M E M E _ S P R E A D _ L I N K
task_insert_meme_spread_link = PostgresOperator(
    task_id='insert_meme_spread_link',
    dag=pipeline1c,
    postgres_conn_id='postgres_default',
    sql='insert_meme_spread_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ I N S E R T _ M E M E _ N O T E X _ L I N K
task_insert_meme_notex_link = PostgresOperator(
    task_id='insert_meme_notex_link',
    dag=pipeline1c,
    postgres_conn_id='postgres_default',
    sql='insert_meme_notex_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ I N S E R T _ M E M E _ S E A R C H I N T _ L I N K
task_insert_meme_searchint_link = PostgresOperator(
    task_id='insert_meme_searchint_link',
    dag=pipeline1c,
    postgres_conn_id='postgres_default',
    sql='insert_meme_searchint_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ I N S E R T _ M E M E _ E X T R E F _ L I N K
task_insert_meme_extref_link = PostgresOperator(
    task_id='insert_meme_extref_link',
    dag=pipeline1c,
    postgres_conn_id='postgres_default',
    sql='insert_meme_extref_link.sql',
    trigger_rule='none_failed',
    autocommit=True
)


### E N D _ T A S K 
end = DummyOperator(
    task_id='end',
    dag=pipeline1c,
    trigger_rule='none_failed'
)


# order of tasks
task_select_link >> task_five >> task_emptiness_chk >> task_format >> task_link_query >> task_insert_link
task_insert_link >> task_insert_meme_about_link >> task_insert_meme_origin_link >> task_insert_meme_spread_link >> task_insert_meme_notex_link
task_insert_meme_notex_link >> task_insert_meme_searchint_link >> task_insert_meme_extref_link >> end