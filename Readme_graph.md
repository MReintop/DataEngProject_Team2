# Neo4j Node data structure

## Created Node and properties
1. **Node Meme properties:** Title,Image,Tags,SocialMediaDescription,ExtRefText,ExtRefLinks,SpreadText,
                            SpreadImages,SpreadLinks,NotExamplesText,Status,Origin,Type,AboutText,AboutImages,
						    AboutLinks,NotExamplesImages,NotExamplesLinks,
2. **Node Ref properties :**  References
3. **Node Parent properties :** Parent URL
4. **Node Keywords properties:** Keywords
5. **Node Origin properties:** OriginText,OriginImages,OriginLinks
6. **Node Search properties:** SearchIntText,SearchIntImages,SearchIntLinks

## Relationships between nodes
1. Meme-[:Refered by]->Ref
2. Meme-[:Child of]->Parent
3. Meme-[:Found by]->Keywords
4. Meme-[: Originates from]->Origin
5. Meme-[: searched by]->Search
