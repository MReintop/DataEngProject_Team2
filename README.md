# DataEngProject_Team2

09.10

Familiarize yourself with the data.
Come up with different end queries which would be intereting to present. 1-5 per person.
Next meeting will be 15.10 at 19:30.


15.10
 An initial pipeline where data are loaded from file this is part one  (provided) and cleaned this is second : 
1. Create airflow project with postgres db.
2. Remove duplicates while importing data. MEMORY IS EXPENSIVE
3. Selecting the needed data. Everyone looks into what data is needed. But how do we gather the info about this.
4. Load the file into the database. NO JSON
5. If needed we can make many tables.

19.10 Add all the columns here we do not want to keep

1.week schedule (22.10)
COLUMNS WE DO NOT NEED (Duplicates and not informative ones): 
- Heidi will maybe make a file with the columns : 
- https://docs.google.com/spreadsheets/d/1jWBTh5St-vOnWT7O3VP2jGK0lJSdr-hh/edit?usp=sharing&ouid=108325435939815487261&rtpof=true&sd=true
- Lets conclude with this then. Everyone downvote the columns. Yes
- Let's select columns with 4-5 votes. 2-3 votes let's discuss. 1 vote - what about them? 0 votes leave out! 

 2 week schedule. 29.10 due (TBD)
This will be specified on friday (22.10).

Mariam : Airflow + postgres project. I willl try.
Heidi : Remove duplicates.
Mart : Mart gathers the data we keep
Kertu: Uploads the data into the database.
Vera: Date formatting and also help with summary of columns with python
statics and unique words for tags

Next session : 22.10 19:30

Worked on this : 
https://docs.google.com/spreadsheets/d/1jWBTh5St-vOnWT7O3VP2jGK0lJSdr-hh/edit#gid=2063405520

Next session : 29.10 19:30

05.11 Implementation of First pipeline in Airflow:
https://github.com/MReintop/DataEngProject_Team2/blob/main/dags/first_pipeline.py

Tasks divided between members, so basically everyone finilize his/her work and replaces the DummyOperators in pipeline:
- task_one: PythonOperator: get_source_file: **Heidi**
- task_two: PythonOperator: select_rows: **Heidi**
- task_three: PythonOperator: select_new_memes: skip currently
- task_four: BranchPythonOperator: emptiness_check: **Heidi**
- task_five: PythonOperator: select_features: **Mart**
- task_six: PythonOperator:format_time: **Vera**
- task_seven: PythonOperator:remove_nsfw: **Mariam**
- task_eight: PythonOperator:remove_nsfw: **Heidi**
- task_nine: PythonOperator: create_sql: **Kertu**
- task_ten: PostgresOperator: insert_to_db: **Kertu**

**Important Question**: Where to store the intermediate file? In Airflow DB?

Other conclusions:
- Importing data from source file and cleansing in one pipeline.
- In second pipeline maybe to create logical views and augment the data?
- Next time the first pipeline will be reviewed and next pipeline discussed.

Next meeting 12.11 19:30

 A second pipeline where data are loaded into an ingestion system and then cleaned and processed :

---- part 2
1. Filter only category : Memes TAGS : Memes have content tags and additional referrences.
2. Remove memes with bad words. "Sensitive tag in the data ? Or NSFW!"
3. Date into a readable date? They might not all be the same.
4. Uniforming structure. 
5. Search keywords meaning? https://www.wikidata.org/wiki/Property:P646 Filter those to readable data ? 
6. We need to upload the data here ? No we need to make airflow project and open the data there. 
8.uniforming content structure, e.g, clustering similar tags
* We need to define the relations betweeen memes , how is useful to store then?
* Also map urls to IDs
* Add parent to each meme and they are Ids
...
-----
Lets cleanse first.


Questions :

- How to remove first high-level duplicates? Got it
- Is ld (LD) necessary? NO
- What does positio mean? List for subcultures? Categories? Not needed
- Queries in two different query languages. What is the deal with this? How do we convert ?


Query ideas :

How many different origins and which are the most popular means from these origins?
Can we though?


 A third pipeline where a relational view is built on the data to perform some analysis
 A fourth pipeline where data are enriched (use your creativity)
 A fifth pipeline where a graph view is built on the data to facilitate some analysis
Natural language analyses will be provided to be implemented at point 3 and 5, a base example using the images (which are not stored) will be included in 4



## Requirements

* To have docker *and* docker-compose installed.
* Install docker and docker-compose exactly as it is described in the website.
* **do not do do apt install docker or docker-compose**

## How to spin the webserver up

### Prepping

First, get your **id**:
```sh
id -u
```

Now edit the **.env** file and swap out 501 for your own.

Run the following command to creat the volumes needed in order to send data to airflow:
```sh
mkdir -p ./dags ./logs ./plugins
```

And this **once**:
```sh
docker-compose up airflow-init
```
If the exit code is 0 then it's all good.

### Running

```sh
docker-compose up
```

After it is up, add a new connection:

* Name - postgres_default
* Conn type - postgres
* Host - postgres
* Port - 5432
* Database - airflow
* Username - airflow
* Password - airflow
