# Neo4j Destkop 
## [Download neo4j desktop](https://neo4j.com/download/)
## SetUp neo4j desktop
### [Install plugins](https://medium.com/neo4j/explore-new-worlds-adding-plugins-to-neo4j-26e6a8e5d37e)
### configure following files in conf folder
#### neo4j.conf
> uncomment or edit following lines in the .Neo4jDesktop\relate-data\dbmss\your-dbms-folder\conf folder edit neo4j.conf
* dbms.security.allow_csv_import_from_file_urls=true
* dbms.memory.heap.initial_size=1G
* dbms.memory.heap.max_size=4G
* dbms.memory.pagecache.size=1512m
* Add to config file apoc.conf in desktop neo4j project config file

### apoc.conf
> in the .Neo4jDesktop\relate-data\dbmss\your-dbms-folder\conf folder add apoc.conf with line below
* apoc.import.file.enabled=true in your apoc.conf

### import the graph csv to create nodes and relationships
> in the .Neo4jDesktop\relate-data\dbmss\your-dbms-folder\import folder 
* add the csv file want to analyse

## Import csv and create nodes and relationships
> load csv with headers from "file:///graph.csv" as memes CREATE (meme:Meme {title:memes.Title,image:memes.Image,tags:memes.Tags,socialMediaDescription:memes.SocialMediaDescription,extRefText: memes.ExtRefText, extRefLinks:memes.ExtRefLinks,spreadText:memes.SpreadText,spreadImages:memes.SpreadImages,spreadLinks:memes.SpreadLinks,notExamplesText:memes.NotExamplesText, status:memes.Status,origin:memes.Origin,type:memes.Type,aboutText:memes.AboutText,aboutImages:memes.AboutImages,aboutLinks:memes.AboutLinks, notExamplesImages:memes.NotExamplesImages,notExamplesLinks:memes.NotExamplesLinks}), (ref:Ref {reference: memes.References}), (parent:Parent {url: memes.URL}),(keyword:Keywords {keywords: memes.keywords}), (origin:Origin {originText: memes.OriginText,originImages:memes.OriginImages,originLinks:memes.OriginLinks}),(search:Search {searchIntText: memes.SearchIntText,searchIntImages:memes.SearchIntImages,searchIntLinks:memes.SearchIntLinks}),(meme)-[:REFERED_BY]->(ref),(meme)-[:CHILD_OF]->(parent),(meme)-[:FOUND_BY]->(keywords), (meme)-[:ORIGINATES_FROM]->(origin), (meme)-[:SEARCHED_BY]->(search)

## Create Index for each node fast lookup of nodes and relationships
* CREATE INDEX FOR (meme:Meme) ON (meme.title)
* CREATE INDEX FOR (ref:Ref) ON (ref.references)
* CREATE INDEX FOR (parent:Parent) ON (parent.url)
* CREATE INDEX FOR (keywords:Keywords) ON (keywords.keywords)
* CREATE INDEX FOR  (origin:Origin) ON (origin.originLinks)
* CREATE INDEX FOR  (search:Search) ON (search.SearchIntLinks)

## to delete all nodes and relationships
* MATCH (n)
DETACH DELETE n

## To view all the nodes and their relationships
match (n) return (n)
