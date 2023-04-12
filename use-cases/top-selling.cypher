// top selling by category
MATCH (p:Product)<-[r:BOUGHT]-()
MATCH (p:Product)-[:IN_SUBCATEGORY]->(sc:Subcategory{name:'Dresses'})
WITH p, count(r) AS totalBought
ORDER BY totalBought DESC
LIMIT 10
RETURN p.name, totalBought


// top selling by category
MATCH (p:Product)<-[r:BOUGHT]-()
MATCH (p:Product)-[:IN_BRAND]->(br:Brand{name:'Nike'})
WITH p, count(r) AS totalBought
ORDER BY totalBought DESC
LIMIT 10
RETURN p.name, totalBought