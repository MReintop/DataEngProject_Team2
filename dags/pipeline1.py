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
   formatted_date_time=pd.to_datetime(col_name.apply(datetime.datetime.fromtimestamp))
   return formatted_date_time

def repair_string(df, col_name):
    df[col_name] = df[col_name].astype("str")
    df[col_name] = df.apply(lambda row: row[col_name].replace('"','').replace("'",""),axis=1)
    return df[col_name]

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
    # remove " and ' from string objects:
    columns = ["Title","URL","Image","Tags","Keywords","Parent","References","SocialMediaDescription",
               "Status","Origin","Type","AboutText","AboutImages","AboutLinks","OriginText","OriginImages",
               "OriginLinks","SpreadText","SpreadImages","SpreadLinks","NotExamplesText",
               "NotExamplesImages","NotExamplesLinks","SearchIntText","SearchIntImages","SearchIntLinks",
               "ExtRefText","ExtRefLinks"]
    for col in columns: df[col] = repair_string(df,col)
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
    # indexes of nsfw
    Tags_exploded = tags(df)
    values = list(Tags_exploded[Tags_exploded.Tag.str.contains('nsfw')].index)
    # drop rows that contain nsfw tag
    df = df[df.index.isin(values) == False]
    df.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_filtered.csv',index=False)
    
task_eight = PythonOperator(
    task_id='remove_nsfw',
    dag=pipeline1,
    python_callable=_remove_nsfw,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)

def _create_meme_query(previous_epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/1638468896_filtered.csv')
    with open("/opt/airflow/dags/meme_inserts.sql", "w") as f:
        df_iterable = df.iterrows()
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
            "MEME_ID VARCHAR,\n" # not in csv
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
            "EXTERNAL_REF_IMAGES VARCHAR,\n" # not in csv
            "TAGS VARCHAR,\n" 
            "MEME_REFERENCES VARCHAR,\n"
            "KEYWORDS VARCHAR,\n"
            "PARENT VARCHAR);\n"
        )
        for index, row in df_iterable:
            title = row['Title']
            url = row['URL']
            time_updated = row['TimeUpdated']
            image = row['Image']
            time_added = row['TimeAdded']
            tags = row['Tags']
            keywords = row['Keywords']
            parent = row['Parent']
            meme_references = row['References']
            image_width = row['ImageWidth']
            image_height = row['ImageHeight']
            social_media_description = row['SocialMediaDescription']
            status = row['Status']
            origin = row['Origin']
            year = row['Year']
            meme_type = row['Type']
            about_text = row['AboutText']
            about_images = row['AboutImages']
            about_links = row['AboutLinks']
            origin_text = row['OriginText']
            origin_images = row['OriginImages']
            origin_links = row['OriginLinks']
            spread_text = row['SpreadText']
            spread_images = row['SpreadImages']
            spread_links = row['SpreadLinks']
            not_examples_text = row['NotExamplesText']
            not_examples_images = row['NotExamplesImages']
            not_examples_links = row['NotExamplesLinks']
            search_intr_text = row['SearchIntText']
            search_intr_images = row['SearchIntImages']
            search_intr_links = row['SearchIntLinks']
            external_ref_text = row['ExtRefText']
            external_ref_links = row['ExtRefLinks']

            # CURRENTLY MISSING FROM _filtered.csv #
            # MEME_ID
            meme_id = ' ' #row['']

            # EXTERNAL_REF_IMAGES
            external_ref_images = ' ' # row['']

            f.write(
                "INSERT INTO MEMES VALUES ("
                f"""DEFAULT, '{title}', '{url}', '{time_updated}', '{image}', {image_width}, {image_height}, 
                '{social_media_description}', '{time_added}', '{status}', '{origin}', {year}, '{meme_type}', '{meme_id}', 
                '{about_text}', '{about_links}', '{about_images}', '{origin_text}', '{origin_links}', '{origin_images}', 
                '{spread_text}', '{spread_links}', '{spread_images}', '{not_examples_text}', '{not_examples_links}', 
                '{not_examples_images}', '{search_intr_text}', '{search_intr_links}', '{search_intr_images}', '{external_ref_text}', 
                '{external_ref_links}', '{external_ref_images}' ,'{tags}', '{meme_references}', '{keywords}', '{parent}'
                );\n"""
            )

        f.close()

### T A S K _ N I N E
task_nine = PythonOperator(
    task_id='create_meme_query',
    dag=pipeline1,
    python_callable=_create_meme_query,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)

### T A S K _ T E N
## Add connection in Airflow
# Admin -> Connections -> [+] (Add a new record)
# Conn Id: postgres_default
# Conn Type: Postgres
# Host: postgres
# Schema: postgres
# Login: airflow
# Password: airflow
# Port: 5432
task_ten = PostgresOperator(
    task_id='insert_meme_query',
    dag=pipeline1,
    postgres_conn_id='postgres_default',
    sql='meme_inserts.sql',
    trigger_rule='none_failed',
    autocommit=True
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