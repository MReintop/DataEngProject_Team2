# Neo4j Graph modelling
## Created Node and properties
1. **Node Meme properties:** Title,Image,Tags,SocialMediaDescription,ExtRefText,ExtRefLinks,SpreadText,
                            SpreadImages,SpreadLinks,NotExamplesText,Status,Origin,Type,AboutText,AboutImages,
						    AboutLinks,NotExamplesImages,NotExamplesLinks,
2. **Node Ref properties :**  References
3. **Node Parent properties :**  URL
4. **Node Keywords properties:** Keywords
5. **Node Origin properties:** OriginText,OriginImages,OriginLinks
6. **Node Search properties:** SearchIntText,SearchIntImages,SearchIntLinks

## Relationships between nodes
1. Meme-[:Refered by]->Ref
2. Meme-[:Child of]->Parent
3. Meme-[:Found by]->Keywords
4. Meme-[: Originates from]->Origin
5. Meme-[: searched by]->Search


## Create constraint for each node
> to avoid node duplication
* CREATE CONSTRAINT ON (meme:Meme) ASSERT meme.title IS UNIQUE
* CREATE CONSTRAINT ON (ref:Ref) ASSERT ref.references IS UNIQUE
* CREATE CONSTRAINT ON (parent:Parent) ASSERT parent.url IS UNIQUE
* CREATE CONSTRAINT ON (keywords:Keywords) ASSERT keywords.keywords IS UNIQUE
* CREATE CONSTRAINT ON (origin:Origin) ASSERT origin.originLinks IS UNIQUE
* CREATE CONSTRAINT ON (search:Search) ASSERT search.SearchIntLinks IS UNIQUE

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



## [Neo4j Desktop use](https://neo4j.com/developer/neo4j-desktop/)
## [Install plugins](https://medium.com/neo4j/explore-new-worlds-adding-plugins-to-neo4j-26e6a8e5d37e)
## Load csv 
### from published csv data in google drive for use locally
* LOAD CSV WITH HEADERS FROM 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRuk8x8ddkeJ158rdHnPwYWsomvI30UYsyTdeIbb5f5QYO0biQdvpn1ut4BF0Uq4Tdt8VTMshMV9BjP/pub?gid=1900621182&single=true&output=csv' AS row
RETURN row 
LIMIT 1000;
* can limit amount being loaded or an error occurs with connection reset since taking long load for viewing 
### [Load using Call Apoc() for large data](https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/)
