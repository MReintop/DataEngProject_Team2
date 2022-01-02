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
import re
import glob

## Add connection in Airflow
# Admin -> Connections -> [+] (Add a new record)
# Conn Id: postgres_default
# Conn Type: Postgres
# Host: postgres
# Schema: postgres
# Login: airflow
# Password: airflow
# Port: 5432


default_args_dict = {
    'start_date': datetime.datetime(2020, 6, 25, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

##### D A G   D E F I N I T I O N 
# pipeline b
# splits the data into tables
# transports the data into database
pipeline1b = DAG(
    dag_id='pipeline1b',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

def get_latest_filtered_csv():
    csv_files = glob.glob("/opt/airflow/dags/*_filtered.csv")
    times = []
    for file in csv_files:
        times.append(int(file.split("/opt/airflow/dags/")[1].split("_filtered.csv")[0]))
    latest_time = max(times)
    latest_file = "/opt/airflow/dags/" + str(latest_time) + "_filtered.csv"
    return latest_file


### S T A R T _ T A S K 
start = DummyOperator(
    task_id='start',
    dag=pipeline1b,
    trigger_rule='none_failed'
)



### T A S K  M E M E 
# Save memes to table Meme - first save to csv file _meme.csv
def _meme(epoch: int, output_folder: str):
    df = pd.read_csv(get_latest_filtered_csv())
    Meme = df[['URL','Title','TimeUpdated', 'Image', 'TimeAdded', 
            'Keywords', 'Parent', 'SocialMediaDescription', 'Status', 'Origin', 'Year']]
    Meme["Keywords"] = Meme.apply(lambda row: str(row["Keywords"]).replace("[","").replace("]","").strip(),axis=1)
    Meme = Meme.drop_duplicates()
    Meme.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_meme.csv',index=False)

task_meme = PythonOperator(
    task_id='meme',
    dag=pipeline1b,
    python_callable=_meme,
    op_kwargs={
        "epoch": '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)



### T A S K  M E M E _ T E X T 
# Save meme texts (about, origin, spread, notable examples, external ref) to table MemeText - first save to csv file _meme_text.csv
def _meme_text(epoch: int, output_folder: str):
    df = pd.read_csv(get_latest_filtered_csv())
    MemeText = df[["URL","AboutText","OriginText","SpreadText","NotExamplesText","SearchIntText", "ExtRefText"]]
    # this part should be in format part
    MemeText["AboutText"] = MemeText.apply(lambda row: str(row["AboutText"]).replace("[","").replace("]","").strip(),axis=1)
    MemeText["OriginText"] = MemeText.apply(lambda row: str(row["OriginText"]).replace("[","").replace("]","").strip(),axis=1)
    MemeText["SpreadText"] = MemeText.apply(lambda row: str(row["SpreadText"]).replace("[","").replace("]","").strip(),axis=1)
    MemeText["NotExamplesText"] = MemeText.apply(lambda row: str(row["NotExamplesText"]).replace("[","").replace("]","").strip(),axis=1)
    MemeText["SearchIntText"] = MemeText.apply(lambda row: str(row["SearchIntText"]).replace("[","").replace("]","").strip(),axis=1)
    MemeText["ExtRefText"] = MemeText.apply(lambda row: str(row["ExtRefText"]).replace("[[","[").replace("]]","]").strip(),axis=1)
    # end here
    MemeText = MemeText.drop_duplicates()
    MemeText.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_meme_text.csv',index=False)

task_meme_text = PythonOperator(
    task_id='meme_text',
    dag=pipeline1b,
    python_callable=_meme_text,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)



### T A S K  M E M E _ T A G
# Save meme and tag relations to table Tag - first save to csv file _meme_tag.csv
def _meme_tag(epoch: int, output_folder: str):
    df = pd.read_csv(get_latest_filtered_csv())
    Tag = df[['URL','Tags']]
    tags = Tag.apply(lambda row: row['Tags'].replace("[","").replace("]","").split(","),axis=1).explode()
    tags = pd.DataFrame(tags,columns=['Tag'])
    tags["Tag"]=tags.apply(lambda row: row["Tag"].strip(),axis=1)
    tags = tags[tags["Tag"]!=""]
    Tag = Tag.join(tags)
    Tag = Tag.drop(labels=Tag[Tag["Tag"].isna()].index)
    Tag = Tag.drop_duplicates()
    Tag = Tag[["URL","Tag"]]
    Tag.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_meme_tag.csv',index=False)

task_meme_tag = PythonOperator(
    task_id='meme_tag',
    dag=pipeline1b,
    python_callable=_meme_tag,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)



### T A S K  M E M E _ T Y P E 
# Save meme and type relations to table Type - first save to csv file _meme_type.csv
def _meme_type(epoch: int, output_folder: str):
    df = pd.read_csv(get_latest_filtered_csv())
    Type = df[["URL","Type"]]
    Type.columns = ["URL","Types"]
    types = Type.apply(lambda row: str(row['Types']).replace("[","").replace("]","").split(","),axis=1).explode()
    types = pd.DataFrame(types,columns=["Type"])
    types["Type"] = types.apply(lambda row: str(row["Type"]).strip(),axis=1)
    Type = Type.join(types)
    Type = Type[Type["Type"]!='nan']
    Type = Type[["URL","Type"]]
    Type.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_meme_type.csv',index=False)

task_meme_type = PythonOperator(
    task_id='meme_type',
    dag=pipeline1b,
    python_callable=_meme_type,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)



### T A S K  M E M E _ R E F E R E N C E
# Save meme and reference relations to table Refs - first save to csv file _meme_ref.csv
def pat_pos(pattern,string):
    match = re.search(pattern,string)
    if match: return match.start()
    else: return -1

def _meme_ref(epoch: int, output_folder: str):
    df = pd.read_csv(get_latest_filtered_csv())
    Refs = df[["URL","References"]]
    Refs["References"] = Refs.apply(lambda row: str(row["References"]).replace("{","").replace("}","").replace(",:",":"),axis=1)
    refers = Refs.apply(lambda row: str(row["References"]).split(", "),axis=1).explode()
    refers = pd.DataFrame(refers,columns=["Ref"])
    refers["RefLink"] = refers.apply(lambda row: str(row["Ref"])[pat_pos('http(...)',str(row["Ref"])):],axis=1)
    refers["RefName"] = refers.apply(lambda row: str(row["Ref"]).split(": http")[0].split(":")[0],axis=1)
    refers["RefLink"] = refers.apply(lambda row: str(row["RefLink"]).strip(),axis=1)
    refers["RefName"] = refers.apply(lambda row: str(row["RefName"]).strip(),axis=1)
    refers["RefLink"] = refers.apply(lambda row: matched_string('https(...)',str(row["RefLink"])),axis=1)
    refers = refers[refers["RefLink"]!=""]
    Refs = Refs.join(refers)
    Refs = Refs.drop(labels=Refs[Refs["RefLink"].isna()].index)
    Refs = Refs[["URL","RefName","RefLink"]]
    Refs.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_meme_ref.csv',index=False)

task_meme_reference = PythonOperator(
    task_id='meme_ref',
    dag=pipeline1b,
    python_callable=_meme_ref,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)



### T A S K  M E M E _ I M A G E S
# Save meme and image link relations to table MemeImage - first save to csv file _meme_img.csv
def matched_string(pattern,string):
    result = re.match(pattern,string)
    if result: return(string)
    else: return("")

def explode_img_links(df,ImageColumn):
    Images = df[["URL",ImageColumn]]
    Images.columns=["URL","Images"]
    imglinks = Images.apply(lambda row: str(row["Images"]).replace("[","").replace("[","").split(","),axis=1).explode()
    imglinks = pd.DataFrame(imglinks,columns=["Link"])
    imglinks[imglinks["Link"]!='nan']
    imglinks["Link"] = imglinks.apply(lambda row: matched_string('http(...)',row["Link"]),axis=1)
    imglinks = imglinks[imglinks["Link"] != ""]
    Images = Images.join(imglinks)
    Images = Images.drop(labels=Images[Images["Link"].isna()].index)
    Images = Images[["URL","Link"]]
    return Images

def _meme_img(epoch: int, output_folder: str):
    df = pd.read_csv(get_latest_filtered_csv())
    AboutImg = explode_img_links(df,'AboutImages')
    AboutImg["Type"] = "About"
    OriginImg = explode_img_links(df,'OriginImages')
    OriginImg["Type"] = "Origin"
    SpreadImg = explode_img_links(df,'SpreadImages')
    SpreadImg["Type"] = "Spread"
    NotExImg = explode_img_links(df,'NotExamplesImages')
    NotExImg["Type"] = "Notable Example"
    SearchIntImg = explode_img_links(df,'SearchIntImages')
    SearchIntImg["Type"] = "Search Interest"
    MemeImage = AboutImg.append(OriginImg).append(SpreadImg).append(NotExImg).append(SearchIntImg)
    MemeImage.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_meme_img.csv',index=False)

task_meme_image = PythonOperator(
    task_id='meme_img',
    dag=pipeline1b,
    python_callable=_meme_img,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)



### T A S K  M E M E _ L I N K S
# Save meme and link relations to tableS MemeAboutLinks, MemeSpreadLinks, etc - first save to csv file _meme_*_link.csv
def explode_links(df,LinksColumn):
    Table = df[["URL",LinksColumn]]
    Table.columns = ["URL","TabelLinks"]
    Table["TabelLinks"] = Table.apply(lambda row: str(row["TabelLinks"]).replace("[","").replace("]","").replace(", http",": http"),axis=1)
    Table = Table[Table["TabelLinks"]!='nan']
    links = Table.apply(lambda row: str(row["TabelLinks"]).split(","),axis=1).explode()
    links = pd.DataFrame(links,columns=["Links"])
    links["LinkName"] = links.apply(lambda row: str(row["Links"]).split(": ")[0].strip(),axis=1)
    links["Link"] = links.apply(lambda row: str(row["Links"]).split(": ")[-1].strip(),axis=1)
    links["Link"] = links.apply(lambda row: matched_string('http(...)',row["Link"]),axis=1)
    links = links[links["Link"] != ""]
    Table = Table.join(links)
    Table = Table.drop(labels=Table[Table["Links"].isna()].index)
    Table = Table[["URL","LinkName","Link"]]
    Table = Table.drop_duplicates()
    return Table

#'NotExamplesLinks','SearchIntLinks'
def _meme_link(epoch: int, output_folder: str):
    df = pd.read_csv(get_latest_filtered_csv())
    MemeAboutLink = explode_links(df,"AboutLinks")
    MemeAboutLink.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_meme_about_link.csv',index=False)
    MemeOriginLink = explode_links(df,"OriginLinks")
    MemeOriginLink.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_meme_origin_link.csv',index=False)
    MemeSpreadLink = explode_links(df,"SpreadLinks")
    MemeSpreadLink.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_meme_spread_link.csv',index=False)
    MemeNotExLink = explode_links(df,"NotExamplesLinks")
    MemeNotExLink.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_meme_notex_link.csv',index=False)
    MemeSearchIntLink = explode_links(df,"SearchIntLinks")
    MemeSearchIntLink.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_meme_searchint_link.csv',index=False)
    MemeExtRefLink = explode_links(df,"ExtRefLinks")
    MemeExtRefLink.to_csv(path_or_buf=f'{output_folder}/{str(epoch)}_meme_extref_link.csv',index=False)

task_meme_link = PythonOperator(
    task_id='meme_link',
    dag=pipeline1b,
    python_callable=_meme_link,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        "output_folder": "/opt/airflow/dags"
    },
    trigger_rule='all_success',
)



#_meme.csv
#_meme_text.csv
#_meme_tag.csv
#_meme_type.csv
#_meme_ref.csv
#_meme_img.csv
#_meme_about_link.csv
#_meme_origin_link.csv
#_meme_spread_link.csv
#_meme_notex_link.csv
#_meme_searchint_link.csv
#_meme_extref_link.csv

### T A S K _ M E M E _ Q U E R Y
# Create a SQL query for inserting MEME data to Postgres DB
def _meme_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_meme.csv')
    with open("/opt/airflow/dags/insert_meme.sql", "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            title = row['Title']
            url = row['URL']
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
                "INSERT INTO MEME VALUES ("
                f""" '{url}', '{title}', '{time_updated}', '{image}', '{social_media_description}', 
                '{time_added}', '{status}', '{origin}', {year}, '{keywords}', '{parent}'
                );\n"""
            )

        f.close()

task_meme_query = PythonOperator(
    task_id='meme_query',
    dag=pipeline1b,
    python_callable=_meme_query,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)


### T A S K _ I N S E R T _ M E M E S
# Inserting the MEME data to the DB, creating also MEME ID
task_insert_meme = PostgresOperator(
    task_id='insert_meme',
    dag=pipeline1b,
    postgres_conn_id='postgres_default',
    sql='insert_meme.sql',
    trigger_rule='none_failed',
    autocommit=True
)



### T A S K _ M E M E _ T E X T _ Q U E R Y
# Create a SQL query for inserting MEMETEXT data to Postgres DB
def _meme_text_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_meme_text.csv')
    with open(f'{output_folder}/insert_meme_text.sql', "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            url = row['URL']
            about_text = row['AboutText']
            origin_text = row['OriginText']
            spread_text = row['SpreadText']
            not_examples_text = row['NotExamplesText']
            search_intr_text = row['SearchIntText']
            external_ref_text = row['ExtRefText']

            f.write(
                "INSERT INTO MEME_TEXT VALUES ("
                f""" '{url}', '{about_text}', '{origin_text}', '{spread_text}', 
                '{not_examples_text}', '{search_intr_text}', '{external_ref_text}'
                );\n"""
            )

        f.close()

task_meme_text_query = PythonOperator(
    task_id='meme_text_query',
    dag=pipeline1b,
    python_callable=_meme_text_query,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)



### T A S K _ M E M E _ T A G _ Q U E R Y
# Create a SQL query for inserting MEMETAG data to Postgres DB
def _meme_tag_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_meme_tag.csv')
    with open(f'{output_folder}/insert_meme_tag.sql', "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            url = row['URL']
            tag = row['Tag']

            f.write(
                "INSERT INTO MEME_TAG VALUES ("
                f""" '{url}', '{tag}'
                );\n"""
            )

        f.close()

task_meme_tag_query = PythonOperator(
    task_id='meme_tag_query',
    dag=pipeline1b,
    python_callable=_meme_tag_query,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)




### T A S K _ M E M E _ T Y P E _ Q U E R Y
# Create a SQL query for inserting MEME_TYPE data to Postgres DB
def _meme_type_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_meme_type.csv')
    with open(f'{output_folder}/insert_meme_type.sql', "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            url = row['URL']
            type = row['Type']

            f.write(
                "INSERT INTO MEME_TYPE VALUES ("
                f""" '{url}', '{type}'
                );\n"""
            )

        f.close()

task_meme_type_query = PythonOperator(
    task_id='meme_type_query',
    dag=pipeline1b,
    python_callable=_meme_type_query,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)



### T A S K _ M E M E _ R E F _ Q U E R Y
# Create a SQL query for inserting MEME_REFERENCE data to Postgres DB
def _meme_ref_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_meme_ref.csv')
    with open(f'{output_folder}/insert_meme_ref.sql', "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            url = row['URL']
            refname = row['RefName']
            reflink = row['RefLink']

            f.write(
                "INSERT INTO MEME_REFERENCE VALUES ("
                f""" '{url}', '{refname}', '{reflink}'
                );\n"""
            )

        f.close()

task_meme_ref_query = PythonOperator(
    task_id='meme_ref_query',
    dag=pipeline1b,
    python_callable=_meme_ref_query,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)




### T A S K _ M E M E _ I M G _ Q U E R Y
# Create a SQL query for inserting MEME_IMAGE data to Postgres DB
def _meme_img_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_meme_img.csv')
    with open(f'{output_folder}/insert_meme_img.sql', "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            url = row['URL']
            link = row['Link']
            type = row['Type']

            f.write(
                "INSERT INTO MEME_IMAGE VALUES ("
                f""" '{url}', '{link}', '{type}'
                );\n"""
            )

        f.close()

task_meme_img_query = PythonOperator(
    task_id='meme_img_query',
    dag=pipeline1b,
    python_callable=_meme_img_query,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)




### T A S K _ M E M E _ A B O U T _ L I N K _ Q U E R Y
# Create a SQL query for inserting MEME_ABOUT_LINK data to Postgres DB
def _meme_about_link_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_meme_about_link.csv')
    with open(f"{output_folder}/insert_meme_about_link.sql", "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            url = row['URL']
            linkname = row['LinkName']
            link = row['Link']

            f.write(
                "INSERT INTO MEME_ABOUT_LINK VALUES ("
                f""" '{url}', '{link}', '{linkname}'
                );\n"""
            )

        f.close()

task_meme_about_link_query = PythonOperator(
    task_id='meme_about_link_query',
    dag=pipeline1b,
    python_callable=_meme_about_link_query,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)




### T A S K _ M E M E _ O R I G I N _ L I N K _ Q U E R Y
# Create a SQL query for inserting MEME_ORIGIN_LINK data to Postgres DB
def _meme_origin_link_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_meme_origin_link.csv')
    with open(f"{output_folder}/insert_meme_origin_link.sql", "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            url = row['URL']
            linkname = row['LinkName']
            link = row['Link']

            f.write(
                "INSERT INTO MEME_ORIGIN_LINK VALUES ("
                f""" '{url}', '{link}', '{linkname}'
                );\n"""
            )

        f.close()

task_meme_origin_link_query = PythonOperator(
    task_id='meme_origin_link_query',
    dag=pipeline1b,
    python_callable=_meme_origin_link_query,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)




### T A S K _ M E M E _ S P R E A D _ L I N K _ Q U E R Y
# Create a SQL query for inserting MEME_SPREAD_LINK data to Postgres DB
def _meme_spread_link_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_meme_spread_link.csv')
    with open(f"{output_folder}/insert_meme_spread_link.sql", "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            url = row['URL']
            linkname = row['LinkName']
            link = row['Link']

            f.write(
                "INSERT INTO MEME_SPREAD_LINK VALUES ("
                f""" '{url}', '{link}', '{linkname}'
                );\n"""
            )

        f.close()

task_meme_spread_link_query = PythonOperator(
    task_id='meme_spread_link_query',
    dag=pipeline1b,
    python_callable=_meme_spread_link_query,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)




### T A S K _ M E M E _ N O T E X _ L I N K _ Q U E R Y
# Create a SQL query for inserting MEME_NOTEX_LINK data to Postgres DB
def _meme_notex_link_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_meme_notex_link.csv')
    with open(f"{output_folder}/insert_meme_notex_link.sql", "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            url = row['URL']
            linkname = row['LinkName']
            link = row['Link']

            f.write(
                "INSERT INTO MEME_NOTEX_LINK VALUES ("
                f""" '{url}', '{link}', '{linkname}'
                );\n"""
            )

        f.close()

task_meme_notex_link_query = PythonOperator(
    task_id='meme_notex_link_query',
    dag=pipeline1b,
    python_callable=_meme_notex_link_query,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)




### T A S K _ M E M E _ S E A R C H I N T _ L I N K _ Q U E R Y
# Create a SQL query for inserting MEME_SEARCHINT_LINK data to Postgres DB
def _meme_searchint_link_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_meme_searchint_link.csv')
    with open(f"{output_folder}/insert_meme_searchint_link.sql", "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            url = row['URL']
            linkname = row['LinkName']
            link = row['Link']

            f.write(
                "INSERT INTO MEME_SEARCHINT_LINK VALUES ("
                f""" '{url}', '{link}', '{linkname}'
                );\n"""
            )

        f.close()

task_meme_searchint_link_query = PythonOperator(
    task_id='meme_searchint_link_query',
    dag=pipeline1b,
    python_callable=_meme_searchint_link_query,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)




### T A S K _ M E M E _ E X T R E F _ L I N K _ Q U E R Y
# Create a SQL query for inserting MEME_EXTREF_LINK data to Postgres DB
def _meme_extref_link_query(epoch: int, output_folder: str):
    df = pd.read_csv(f'{output_folder}/{str(epoch)}_meme_extref_link.csv')
    with open(f"{output_folder}/insert_meme_extref_link.sql", "w") as f:
        df_iterable = df.iterrows()

        for index, row in df_iterable:
            url = row['URL']
            linkname = row['LinkName']
            link = row['Link']

            f.write(
                "INSERT INTO MEME_EXTREF_LINK VALUES ("
                f""" '{url}', '{link}', '{linkname}'
                );\n"""
            )

        f.close()

task_meme_extref_link_query = PythonOperator(
    task_id='meme_extref_link_query',
    dag=pipeline1b,
    python_callable=_meme_extref_link_query,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success'
)

### J O I N _ T A S K 
#join = DummyOperator(
#    task_id='join',
#    dag=pipeline1b,
#    trigger_rule='none_failed'
#)


#insert_meme_text
#insert_meme_tag
#insert_meme_type
#insert_meme_ref
#insert_meme_img
#insert_meme_about_link
#insert_meme_origin_link
#insert_meme_spread_link
#insert_meme_notex_link
#insert_meme_searchint_link
#insert_meme_extref_link

### T A S K _ I N S E R T _ M E M E _ T E X T
task_insert_meme_text = PostgresOperator(
    task_id='insert_meme_text',
    dag=pipeline1b,
    postgres_conn_id='postgres_default',
    sql='insert_meme_text.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ I N S E R T _ M E M E _ T A G
task_insert_meme_tag = PostgresOperator(
    task_id='insert_meme_tag',
    dag=pipeline1b,
    postgres_conn_id='postgres_default',
    sql='insert_meme_tag.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ I N S E R T _ M E M E _ T Y P E
task_insert_meme_type = PostgresOperator(
    task_id='insert_meme_type',
    dag=pipeline1b,
    postgres_conn_id='postgres_default',
    sql='insert_meme_type.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ I N S E R T _ M E M E _ R E F
task_insert_meme_ref = PostgresOperator(
    task_id='insert_meme_ref',
    dag=pipeline1b,
    postgres_conn_id='postgres_default',
    sql='insert_meme_ref.sql',
    trigger_rule='none_failed',
    autocommit=True
)

### T A S K _ I N S E R T _ M E M E _ I M G
task_insert_meme_img = PostgresOperator(
    task_id='insert_meme_img',
    dag=pipeline1b,
    postgres_conn_id='postgres_default',
    sql='insert_meme_img.sql',
    trigger_rule='none_failed',
    autocommit=True
)




### E N D _ T A S K 
end = DummyOperator(
    task_id='end',
    dag=pipeline1b,
    trigger_rule='none_failed'
)


# order of tasks

start >> task_meme
task_meme >> task_meme_query >> task_meme_text >> task_meme_text_query >> task_meme_tag >> task_meme_tag_query
task_meme_tag_query >> task_meme_type >> task_meme_type_query >> task_meme_reference >> task_meme_ref_query
task_meme_ref_query >> task_meme_image >> task_meme_img_query >> task_meme_link
task_meme_link >> task_meme_about_link_query >> task_meme_origin_link_query >> task_meme_spread_link_query
task_meme_spread_link_query >> task_meme_notex_link_query >> task_meme_searchint_link_query >> task_meme_extref_link_query
task_meme_extref_link_query >> task_insert_meme >> task_insert_meme_text >> task_insert_meme_tag >> task_insert_meme_type >> task_insert_meme_ref
task_insert_meme_ref >> task_insert_meme_img >> end