MATCH (p1:Product)-[:PART_OF]->(o1:Order)<-[:PART_OF]-(p2:Product)
WHERE p1 <> p2
WITH p1, p2, count(o1) AS orderCount
WHERE orderCount > 1
MERGE (p1)-[fbw:FREQUENTLY_BOUGHT_WITH]->(p2)
SET fbw.count = orderCount

MATCH p=()-[r:FREQUENTLY_BOUGHT_WITH]->() RETURN p LIMIT 25;
