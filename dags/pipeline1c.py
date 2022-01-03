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
import re
import numpy as np


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
def get_latest_csv(name):
    csv_files = glob.glob(f"/opt/airflow/dags/*_{name}.csv")
    times = []
    for file in csv_files:
        times.append(int(file.split("/opt/airflow/dags/")[1].split(f"_{name}.csv")[0]))
    latest_time = max(times)
    latest_file = f"/opt/airflow/dags/" + str(latest_time) + f"_{name}.csv"
    return latest_file



### T A S K _ S E L E C T _ L I N K 
# select all categories and delete high level duplicates
def _select_link(epoch: int, output_folder: str):
    df = pd.read_csv(get_latest_csv("flattened"))
    #df = df[df.Category!="Meme"]
    df = df[['Title', 'URL', 'Category', 'Parent', 'SocialMediaDescription']]
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
# format the string and int fields (.astype)

def repair_string(df, col_name):
    #df[col_name] = df[col_name].astype("str")
    df[col_name] = df.apply(
        lambda row: 
            str(row[col_name]).replace('"','').replace("'","").replace("`","").replace("\\","").replace("\n","").replace("{{","{").replace("}}","}").replace("’","").replace("{#","(#").strip(),axis=1)
    return df[col_name]

def _format_fields(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_link.csv')
    df["Title"] = df["Title"].astype("str")
    df["URL"] = df["URL"].astype("str")
    # remove bad symbols from string objects:
    columns = ['Title', 'URL', 'Category', 'Parent', 'SocialMediaDescription']
    for col in columns: df[col] = repair_string(df,col)
    df = df[["URL", "Title", "Category", "Parent", "SocialMediaDescription"]]
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


### T A S K _ C O L L E C T _ L I N K S
def matched_string(pattern,string):
    result = re.match(pattern,string)
    if result: return(string)
    else: return("")

def find_cat(string):
    categories = {"culture()": "Culture", "event()": "Event", "meme()": "Meme", 
                  "person()": "Person", "site()": "Site", "subculture()": "Subculture"}
    for pattern in categories.keys():
        result = re.match(pattern,string)
        if result: return(categories[pattern])
        else: return("Other")

def get_one_value(df,URL,value_col):
    all = df[df["URL"]==URL][value_col]
    return all.iloc[0]

def _collect_links(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_link.csv')
    df = df[["URL","Title","Category","Parent","SocialMediaDescription"]]
    links = pd.read_csv(get_latest_csv("meme_link"))
    links = links[["Link","LinkName"]]
    links = links.drop_duplicates()
    missing_links = links[~links["Link"].isin(df["URL"])]
    missing_links.columns = ["URL","Title"]
    missing_links["Parent"] = np.NAN
    missing_links["SocialMediaDescription"] = np.NAN
    missing_links["Category2"] = links.apply(lambda row: str(row["Link"]).split("https://knowyourmeme.com/")[-1].split("memes/")[-1].split("/")[0].strip(),axis=1)
    missing_links["Category"] = missing_links.apply(lambda row: find_cat(str(row["Category2"])),axis=1)
    missing_links = missing_links[["URL","Title","Category","Parent","SocialMediaDescription"]]
    df = df.append(missing_links)
    df = df.drop_duplicates()
    ## URLs must be unique
    stat = df.groupby(by="URL").count().sort_values(by="Title",ascending=False)
    urls = list(stat[stat["Title"]>1].index) + list(stat[stat["Category"]>1].index)
    repeating_urls = df[df["URL"].isin(urls)].sort_values(by="URL")
    repeating_urls = repeating_urls[["URL","Parent","SocialMediaDescription"]]
    repeating_urls = repeating_urls.drop_duplicates()
    ## find one title and one category for URLs which had repeating titles
    repeating_urls["Title"] = repeating_urls.apply(lambda row: get_one_value(df,str(row["URL"]),"Title"),axis=1)
    repeating_urls["Category"] = repeating_urls.apply(lambda row: get_one_value(df,str(row["URL"]),"Category"),axis=1)
    repeating_urls = repeating_urls[["URL","Title","Category","Parent","SocialMediaDescription"]]
    # remove repeating titles and join fixed rows
    df = df[~df["URL"].isin(urls)]
    df = df.append(repeating_urls)
    df = df.drop_duplicates()
    df.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_links.csv',index=False)

task_collect_links = PythonOperator(
    task_id='collect_links',
    dag=pipeline1c,
    python_callable=_collect_links,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)


### T A S K _ L I N K _ Q U E R Y
# Create a SQL query for inserting LINK categories data to Postgres DB
def _link_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_links.csv')
    with open("/opt/airflow/dags/insert_link.sql", "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            title = row['Title']
            url = row['URL']
            category = row['Category']
            parent = row['Parent']
            social_media_description = row['SocialMediaDescription']

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



### T A S K _ I N S E R T _ M E M E _ L I N K
task_insert_meme_link = PostgresOperator(
    task_id='insert_meme_link',
    dag=pipeline1c,
    postgres_conn_id='postgres_default',
    sql='insert_meme_link.sql',
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
task_select_link >> task_five >> task_emptiness_chk >> task_format >> task_collect_links >> task_link_query >> task_insert_link
task_insert_link >> task_insert_meme_link >> end