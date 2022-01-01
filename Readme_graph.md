# Neo4j Graph modelling

## [Neo4j Desktop use](https://neo4j.com/developer/neo4j-desktop/)
## [Install plugins](https://medium.com/neo4j/explore-new-worlds-adding-plugins-to-neo4j-26e6a8e5d37e)
## Load csv 
### from published csv data in google drive for use locally
* LOAD CSV WITH HEADERS FROM 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRuk8x8ddkeJ158rdHnPwYWsomvI30UYsyTdeIbb5f5QYO0biQdvpn1ut4BF0Uq4Tdt8VTMshMV9BjP/pub?gid=1900621182&single=true&output=csv' AS row
RETURN row 
LIMIT 1000;
* can limit amount being loaded or an error occurs with connection reset since taking long load for viewing 
### [Load using Call Apoc() for large data](https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/)
**configure settings file**
dbms.security.allow_csv_import_from_file_urls=true
dbms.memory.heap.initial_size=1G  
dbms.memory.heap.max_size=4G 
dbms.memory.pagecache.size=1512m
**Add to config file apoc.conf in desktop neo4j project config file**
apoc.import.file.enabled=true in your apoc.conf

**import the graph.csv file**
CALL apoc.load.csv('file:///graph.csv')
YIELD lineNo, map, list
RETURN *;



## Create constraint for each node
> to avoid node duplication
* CREATE CONSTRAINT memeIdConstraint FOR (meme:Meme) REQUIRE meme.id IS UNIQUE
* CREATE CONSTRAINT refIdConstraint FOR (ref:Ref) REQUIRE ref.id IS UNIQUE
* CREATE CONSTRAINT parentIdConstraint FOR (parent:Parent) REQUIRE parent.id IS UNIQUE
* CREATE CONSTRAINT keywordsIdConstraint FOR (keywords:Keywords) REQUIRE keyword.id IS UNIQUE
* CREATE CONSTRAINT originIdConstraint FOR (origin:Origin) REQUIRE origin.id IS UNIQUE
* CREATE CONSTRAINT searchIdConstraint FOR (search:Search) REQUIRE search.id IS UNIQUE

### Create index for each node
* CREATE INDEX FOR (meme:Meme) ON (meme.title)
* CREATE INDEX FOR (ref:Ref) ON (ref.references)
* CREATE INDEX FOR (parent:Parent) ON (parent.url)
* CREATE INDEX FOR (keywords:Keywords) ON (keywords.keywords)
* CREATE INDEX FOR  (origin:Origin) ON (origin.originLinks)
* CREATE INDEX FOR  (search:Search) ON (search.SearchIntLinks)


### to edit
> inplace of constraints can use index on a property of a node to ensure fast lookups.
* CREATE CONSTRAINT ON (meme:Meme) ASSERT meme.title IS UNIQUE;
* CREATE CONSTRAINT ON (ref:Ref) ASSERT ref.references IS UNIQUE;
* CREATE CONSTRAINT ON (parent:Parent) ASSERT parent.url IS UNIQUE;
* CREATE CONSTRAINT ON (keywords:Keywords) ASSERT keywords.keywords IS UNIQUE;
* CREATE CONSTRAINT ON (origin:Origin) ASSERT origin.originLinks IS UNIQUE;
* CREATE CONSTRAINT ON (search:Search) ASSERT search.SearchIntLinks IS UNIQUE;
* ![image](https://user-images.githubusercontent.com/572088/146229492-0a96de9c-c34f-45ec-b3a1-5b91109cf89c.png)

## Created Node and properties
1. **Node Meme properties:** Title,Image,Tags,SocialMediaDescription,ExtRefText,ExtRefLinks,SpreadText,
                            SpreadImages,SpreadLinks,NotExamplesText,Status,Origin,Type,AboutText,AboutImages,
						    AboutLinks,NotExamplesImages,NotExamplesLinks,
2. **Node Ref properties :**  References
3. **Node Parent properties :**  URL
4. **Node Keywords properties:** Keywords
5. **Node Origin properties:** OriginText,OriginImages,OriginLinks
6. **Node Search properties:** SearchIntText,SearchIntImages,SearchIntLinks

## Create meme node 
CREATE 
(meme:Meme {title:'Title',image:'Image',tags:'Tags',socialMediaDescription:'SocialMediaDescription',extRefText: 'ExtRefText',
extRefLinks:'ExtRefLinks',spreadText:'SpreadText',spreadImages:'SpreadImages',spreadLinks:'SpreadLinks',notExamplesText:'NotExamplesText',
status:'Status',origin:'Origin',type:'Type',aboutText:'AboutText',aboutImages:'AboutImages',aboutLinks:'AboutLinks',
notExamplesImages:'NotExamplesImages',notExamplesLinks:'NotExamplesLinks'});

## Create Ref node
CREATE (ref:Ref {reference:'References'});

## Create parent node
Create (parent:Parent {url:'URL'});

## Create keywords node
Create (keywords:Keywords {keywords:'Keywords'});

## Create origin node
Create (origin:Origin {originText:'OriginText', originImages:'OriginImages', originLinks: 'OriginLinks'});

## Create search node
Create (search:Search {searchIntText:'SearchIntText', searchIntImages:'SearchIntImages', searchIntLinks:'SearchIntLinks'});
![image](https://user-images.githubusercontent.com/572088/146229085-23e9064b-b469-4741-ae4f-3da3e020256a.png)

## Relationships between nodes
1. Meme-[:Refered by]->Ref
2. Meme-[:Child of]->Parent
3. Meme-[:Found by]->Keywords
4. Meme-[: Originates from]->Origin
5. Meme-[: searched by]->Search

## Create relationship between nodes
* Create (meme)-[:REFERED_BY]->(ref);
* Create (meme)-[:CHILD_OF]->(parent);
* Create (meme)-[:FOUND_BY]->(keywords);
* Create (meme)-[:ORIGINATES_FROM]->(origin);
* Create (meme)-[:SEARCHED_BY]->(search);
![image](https://user-images.githubusercontent.com/572088/146229703-a61262b4-1ffe-4914-8b4e-706940d0e0b0.png)
### Create combined multiple relationships


