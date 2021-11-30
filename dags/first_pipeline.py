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
# first pipeline is importing data from source and making the first rough selection
# we should include here also the cleansing part:
# removing sensitive/impropriate content
# transform the data into correct formats: int, string, timestamp, etc.
first_pipeline = DAG(
    dag_id='first_pipeline',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)


### T A S K _ O N E 
# Get the source file from url: https://owncloud.ut.ee/owncloud/index.php/s/g4qB5DZrFEz2XLm/download/kym.json
def _get_source_file(epoch, url, output_folder):
    request.urlretrieve(url=url, filename=f"{output_folder}/{epoch}.json")


task_one = PythonOperator(
    task_id='get_source_file',
    dag=first_pipeline,
    python_callable=_get_source_file,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}",
        "url": "https://owncloud.ut.ee/owncloud/index.php/s/g4qB5DZrFEz2XLm/download/kym.json"
    },
    trigger_rule='all_success',
    depends_on_past=False,
)


### T A S K _ T W O 
# check if no new rows (if no new rows then go forward to the end)
def _emptiness_check(previous_epoch: int, output_folder: str):
    df = pd.read_json(f'{output_folder}/{str(previous_epoch)}.json')
    length = len(df.index)
    if length == 0:
        return 'end'
    else:
        return 'select_data'


task_four = BranchPythonOperator(
    task_id='emptiness_check',
    dag=first_pipeline,
    python_callable=_emptiness_check,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)


### T A S K _ T H R E E 
# select only wanted data features, flatten the data
def _select_data(epoch: int, output_folder: str):
    with open(f'{output_folder}/{str(epoch)}.json','r') as f:
        data = json.loads(f.read())
    
    
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_filtered.csv')
    title = df[['title']].rename({'title':'Title'})
    url = df[['url']].rename({'url':'URL'})
    last_update_source = df[['last_update_source']].rename({'last_update_source':'TimeUpdated'})
    template_image_url = df[['template_image_url']].rename({'template_image_url':'Image'})
    meta = _meta_to_df(df)
    added = df[['added']].rename({'added':'TimeAdded'})
    details = _details_to_df(df)
    content = _content_to_df(df)
    tags = df[['tags']].rename({'tags':'Tags'})
    additional_references = df[['additional_references']].rename({'additional_references':'References'})
    search_keywords = df[['search_keywords']]
    parent = df[['parent']].rename({'parent':'Parent'})
    df = pd.concat([title,url,last_update_source,template_image_url,meta,added,details,tags,additional_references,search_keywords,parent],axis=1) #df
    df.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_flattened.csv')

task_five = PythonOperator(
    task_id='select_data',
    dag=first_pipeline,
    python_callable=_select_data,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# 

# select only category memes and delete high level duplicates
def _sel_instances(epoch: int, output_folder: str):
    df = pd.read_json(f'{output_folder}/{str(epoch)}.json')
    df_memes = df[df.category=="Meme"]
    df_memes_filtered=df_memes.drop_duplicates(subset='title', keep='first')
    df_filtered = df_memes_filtered.drop("category",axis=1)
    df_filtered.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_filtered.csv')

task_two = PythonOperator(
    task_id='select_rows', 
    dag=first_pipeline,
    python_callable=_sel_instances,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# to be repeatable(?) lets select only new rows if there are in the file
task_three = DummyOperator(
    task_id='select_new_memes',
    dag=first_pipeline,
    trigger_rule='none_failed'
)

### T A S K _ F O U R
# check if no new rows (if no new rows then go forward to the end)
def _emptiness_check(previous_epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(previous_epoch)}_filtered.csv')
    length = len(df.index)
    if length == 0:
        return 'end'
    else:
        return 'split'


task_four = BranchPythonOperator(
    task_id='emptiness_check',
    dag=first_pipeline,
    python_callable=_emptiness_check,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)

### T A S K _ F I V E


### T A S K _ S I X
# format the time fields (time_added,time_updated)
task_six = DummyOperator(
    task_id='format_time',
    dag=first_pipeline,
    trigger_rule='none_failed'
)

### T A S K _ S E V E N
# format the string and int fields (.astype)
task_seven = DummyOperator(
    task_id='format_fields',
    dag=first_pipeline,
    trigger_rule='none_failed'
)

### T A S K _ E I G H T
# remove sensitive/impropriate data
task_eight = DummyOperator(
    task_id='remove_nsfw',
    dag=first_pipeline,
    trigger_rule='none_failed'
)

### T A S K _ N I N E
task_nine = DummyOperator(
    task_id='create_sql',
    dag=first_pipeline,
    trigger_rule='none_failed'
)

### T A S K _ T E N 
task_ten = DummyOperator(
    task_id='insert_to_db',
    dag=first_pipeline,
    trigger_rule='none_failed'
)

end = DummyOperator(
    task_id='end_of_clensing',
    dag=first_pipeline,
    trigger_rule='none_failed'
)


# order of tasks
task_one >> task_two >> task_three >> task_four 
task_four >> [task_five,end] 
task_five >> task_six >> task_seven >> task_eight >> task_nine >> task_ten
[task_four,task_ten] >> end