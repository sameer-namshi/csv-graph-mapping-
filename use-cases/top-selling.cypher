// top selling by category
MATCH (p:Product)-[r:BOUGHT]-()
MATCH (p:Product)-[:BELONGS_TO]->(sc:Subcategory{name:'Dresses'})
WITH p, count(r) AS totalBought
ORDER BY totalBought DESC
LIMIT 10
RETURN p.name, totalBought


// top selling by category
MATCH (p:Product)-[r:BOUGHT]-()
MATCH (p:Product)-[:BY_BRAND]->(br:Brand{name:'Ella Plus'})
WITH p, count(r) AS totalBought
ORDER BY totalBought DESC
LIMIT 10
RETURN p.name, totalBought