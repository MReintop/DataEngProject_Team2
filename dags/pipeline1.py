import airflow
import datetime
from datetime import datetime
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
def _emptiness_check(epoch: int, output_folder: str):
    df = pd.read_json(f'{output_folder}/{str(epoch)}.json')
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
        'epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)


### T A S K _ T H R E E 
# flatten the data and select only wanted data features
def _select_data(epoch: int, output_folder: str):
    with open(f'{output_folder}/{str(epoch)}.json','r') as f:
        data = json.loads(f.read())
    df0 = pd.json_normalize(data,max_level=0) # content, additional references need this level!
    df2 = pd.json_normalize(data,max_level=2)
    columns0 = ['title','url','last_update_source','category','template_image_url','added','tags','search_keywords','parent','additional_references']
    columns2 = [
        'meta.og:image:width','meta.og:image:height','meta.og:description',
        'details.status','details.origin','details.year','details.type',
        'content.about.text','content.about.images','content.about.links','content.origin.text','content.origin.images','content.origin.links',
        'content.spread.text','content.spread.images','content.spread.links','content.notable examples.text','content.notable examples.images','content.notable examples.links',
        'content.search interest.text','content.search interest.images','content.search interest.links','content.external references.text','content.external references.links']
    df0 = df0[columns0]
    df2 = df2[columns2]
    df = pd.concat([df0,df2],axis=1)
    df.columns = ['Title','URL','TimeUpdated','Category','Image','TimeAdded','Tags','Keywords','Parent','References',
                'ImageWidth','ImageHeight','SocialMediaDescription','Status','Origin','Year','Type',
                'AboutText', 'AboutImages', 'AboutLinks', 'OriginText', 'OriginImages', 'OriginLinks',
                'SpreadText', 'SpreadImages', 'SpreadLinks', 'NotExamplesText', 'NotExamplesImages', 'NotExamplesLinks',
                'SearchIntText', 'SearchIntImages', 'SearchIntLinks', 'ExtRefText', 'ExtRefLinks']
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
        return 'format_fields'


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
# format the time fields (time_added,time_updated)
# format the string and int fields (.astype)

# convert epoch time to readable date time
def format_time(col_name):
   formatted_date_time=pd.to_datetime(col_name.apply(datetime.fromtimestamp))
   return formatted_date_time

def _format_fields(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_filtered.csv')
    df["Title"] = df["Title"].astype("str")
    df["URL"] = df["URL"].astype("str")
    # time:
    df['TimeUpdated'] = df['TimeUpdated'].fillna(-2208988800)
    df['TimeUpdated'] = format_time(df['TimeUpdated'])
    df['TimeAdded'] = df['TimeAdded'].fillna(-2208988800)
    df['TimeAdded'] = format_time(df['TimeAdded'])
    # int:
    df['ImageWidth'] = df['ImageWidth'].fillna(0).astype("int64")
    df['ImageHeight'] = df['ImageHeight'].fillna(0).astype("int64")
    df['Year'] = df['Year'].fillna(1900).astype("int64")
    df.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_filtered.csv',index=False)

task_seven = PythonOperator(
    task_id='format_fields',
    dag=pipeline1,
    python_callable=_format_fields,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)


### T A S K _ E I G H T
# remove sensitive/impropriate data
def tags(df):
    # create intermediate table Tags_exploded 
    # where the initial column Tags is exploded into pieces and saved into new column Tag
    Tags_exploded = df[['URL','Tags']]
    Tags_exploded["Tags"] = Tags_exploded["Tags"].astype('str')
    t1 = Tags_exploded.apply(lambda row: row["Tags"].split('[')[-1], axis=1)
    Tags_exploded["t1"]=t1
    t2 = Tags_exploded.apply(lambda row: row["t1"].split(']')[0], axis=1)
    Tags_exploded["t2"]=t2
    SingleTag = Tags_exploded.apply(lambda row: row["t2"].split(','), axis=1).explode()
    Tags_exploded = Tags_exploded.join(pd.DataFrame(SingleTag,columns=["Tag"]))
    Tags_exploded = Tags_exploded.drop(["Tags","t1","t2"],axis=1)
    return Tags_exploded

def _remove_nsfw(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_filtered.csv')
    
task_eight = DummyOperator(
    task_id='remove_nsfw',
    dag=pipeline1,
    trigger_rule='none_failed'
)

### T A S K _ N I N E
task_nine = DummyOperator(
    task_id='create_sql',
    dag=pipeline1,
    trigger_rule='none_failed'
)

### T A S K _ T E N
task_ten = DummyOperator(
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
task_seven >> task_eight >> task_nine >> task_ten
[task_two,task_six,task_ten] >> end