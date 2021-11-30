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
pipeline1 = DAG(
    dag_id='pipeline1',
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
    dag=pipeline1,
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


task_two = BranchPythonOperator(
    task_id='emptiness_check',
    dag=pipeline1,
    python_callable=_emptiness_check,
    op_kwargs={
        'previous_epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)


### T A S K _ T H R E E 
# flatten the data and select only wanted data features
def _select_data(epoch: int, output_folder: str):
    with open(f'{output_folder}/{str(epoch)}.json','r') as f:
        data = json.loads(f.read())
    df0 = pd.json_normalize(data,max_level=0) # additional_references need this level!
    df1 = pd.json_normalize(data,max_level=1)
    columns0 = ['title','url','last_update_source','category','template_image_url','added','tags','search_keywords','parent','additional_references']
    columns1 = [
        'meta.og:image:width','meta.og:image:height','meta.og:description',
        'details.status','details.origin','details.year','details.type',
        'content.about','content.origin','content.spread','content.notable examples','content.search interest','content.external references']
    df0 = df0[columns0]
    df1 = df1[columns1]
    df = pd.concat([df0,df1],axis=1)
    df.columns = ['Title','URL','TimeUpdated','Category','Image','TimeAdded','Tags','Keywords','Parent','References',
                'ImageWidth','ImageHeight','SocialMediaDescription','Status','Origin','Year','Type',
                'About','Origin','Spread','NotExamples','SearchInt','ExtReferences']
    df.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_flattened.csv',index=False)

task_three = PythonOperator(
    task_id='select_data',
    dag=pipeline1,
    python_callable=_select_data,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
)


### T A S K _ F O U R
# select only category memes and delete high level duplicates
def _select_memes(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_flattened.csv')
    df = df[df.Category=="Meme"]
    df = df.drop_duplicates(subset='Title', keep='first')
    df = df.drop("Category",axis=1)
    df.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_filtered.csv',index=False)

task_four = PythonOperator(
    task_id='select_memes', 
    dag=pipeline1,
    python_callable=_select_memes,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
)


### T A S K _ F I V E
# to be repeatable(?) lets select only new rows if there are in the file
task_five = DummyOperator(
    task_id='select_new_memes',
    dag=pipeline1,
    trigger_rule='none_failed'
)

### T A S K _ S I X 
# check if no new rows (if no new rows then go forward to the end)
def _emptiness_check2(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_filtered.csv')
    length = len(df.index)
    if length == 0:
        return 'end'
    else:
        return 'format'


task_six = BranchPythonOperator(
    task_id='emptiness_check2',
    dag=pipeline1,
    python_callable=_emptiness_check2,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)


### T A S K _ S E V E N
task_seven = DummyOperator(
    task_id='extract_content_fields',
    dag=pipeline1,
    trigger_rule='none_failed'
)


### T A S K _ E I G H T
# format the time fields (time_added,time_updated)
# format the string and int fields (.astype)
task_eight = DummyOperator(
    task_id='format_fields',
    dag=pipeline1,
    trigger_rule='none_failed'
)

### T A S K _ N I N E
# remove sensitive/impropriate data
task_nine = DummyOperator(
    task_id='remove_nsfw',
    dag=pipeline1,
    trigger_rule='none_failed'
)

### T A S K _ T E N 
task_ten = DummyOperator(
    task_id='create_sql',
    dag=pipeline1,
    trigger_rule='none_failed'
)

### T A S K _ E L E V E N
task_eleven = DummyOperator(
    task_id='insert_to_db',
    dag=pipeline1,
    trigger_rule='none_failed'
)

end = DummyOperator(
    task_id='end_of_clensing',
    dag=pipeline1,
    trigger_rule='none_failed'
)


# order of tasks
task_one >> task_two >> [task_three,end] 
task_three >> task_four >> task_five >> task_six >> [task_seven,end] 
task_seven >> task_eight >> task_nine >> task_ten >> task_eleven
[task_two,task_six,task_eleven] >> end