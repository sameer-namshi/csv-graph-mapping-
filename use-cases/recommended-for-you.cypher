// RECOMMENDED FOR YOU BY SCORE, COMBINATION
------------------------------------------
// FETCH ALL SKUs AND COMBINATIONS PURCHASED BY CUSTOMER 
// 14309080, 9996808, 9997167

// WITH toString(date() - duration({days: 15})) AS dateRange
// MATCH (cust:Customer {id:"9996808"} )-[r:BOUGHT]->(purchasedProduct:Product)
// ===================================================
// WITH toString(date() - duration({days: 30})) AS dateRange
// MATCH (cust:Customer {id:"9997167"})-[r:BOUGHT]->(purchasedProduct:Product)
// ===================================================

// Get all SKUs purchased by customer in a specific dateRange
WITH toString(date() - duration({days: 60})) AS dateRange
MATCH (cust:Customer {id:$customerId})-[r:BOUGHT]->(purchasedProduct:Product)
WHERE r.order_date >= dateRange
MATCH (purchasedProduct)-[:IN_SUBCATEGORY]->(sub:Subcategory)
MATCH (purchasedProduct)-[:IN_BRAND]->(br:Brand)
MATCH (purchasedProduct)-[:IN_PRICE_LEVEL]->(pl:PriceLevel)
MATCH (cust)-[:BOUGHT]-(simple:ProductSimple)<-[:HAS_SIMPLE]-(purchasedProduct)

// Construct the combinations 
// purchasedCombinations - (subcategory,brand,priceLevel) - ["Sweaters,Seventy Five,pl3", "Cardigans,Defacto,pl3", "Pants,Puma,pl4"]
// purchasedBrandSubCombinations - (subcategory,brand) - ["Sweaters,Seventy Five", "Cardigans,Defacto", "Pants,Puma"]
// categoryGenderPreferences - (subcategory,gender) - ["Sweaters,Male", "Shoes,Female"]
// preferredGenders - [Male] 
// purchasedSKUs - ["11136AT14TAP","11136ATBIHMP"]
// catBrSizePreferences - ["Sports Bras,Nike,xl", "Sweaters,Seventy Five,xl"]
// categorySizePreferences - ["Sports Bras,xl", "Sweaters,xl"]
WITH cust, collect( distinct COALESCE(purchasedProduct.subcategory,"")+','+ COALESCE(purchasedProduct.brand,"")+','+ COALESCE(purchasedProduct.priceLevel,"")) as purchasedCombinations,
collect( distinct  COALESCE(purchasedProduct.subcategory,"")+','+ COALESCE(purchasedProduct.brand,"")) as purchasedBrandSubCombinations,
collect(purchasedProduct.sku_config) as purchasedSKUs,
collect( distinct  COALESCE(purchasedProduct.subcategory,"")+','+ COALESCE(purchasedProduct.gender,"")) as categoryGenderPreferences, 
collect( distinct  COALESCE(purchasedProduct.subcategory,"")+','+ COALESCE(purchasedProduct.ageGroup,"")) as categoryAgeGroupPreferences, 
collect(distinct purchasedProduct.gender) as preferredGenders,
collect(distinct COALESCE(purchasedProduct.subcategory, "") + ',' + COALESCE(purchasedProduct.brand,"")+ ',' + COALESCE(simple.size,"")) as catBrSizePreferences,
collect(distinct COALESCE(purchasedProduct.subcategory, "") + ',' + COALESCE(simple.size,"")) as categorySizePreferences

// Get all Subcategories, Brands and Price Levels that the customer is interested in
MATCH (cust)-[l:INTERESTED_SUBCATEGORY]->(:Subcategory)
WITH sum(l.count) as totalLikes, cust, purchasedCombinations, purchasedBrandSubCombinations, purchasedSKUs, categoryGenderPreferences, categoryAgeGroupPreferences, preferredGenders, catBrSizePreferences, categorySizePreferences
MATCH (cust)-[subCatInterests:INTERESTED_SUBCATEGORY]->(sub:Subcategory)
MATCH (cust)-[brandInterests:INTERESTED_BRAND]->(br:Brand)
MATCH (cust)-[priceLevelInterests:INTERESTED_PRICE_LEVEL]->(pl:PriceLevel)

// CALCULATE SCORES FOR EACH SUBCATEGORY, BRAND AND PRICE LEVEL THAT THE CUSTOMER IS INTERESTED IN
//  apoc.map.fromLists([subcategory1,subcategory2, subcategory3], [{subcategory: subcategory1, subCatCount:30, like: 0.07142857142857},{subcategory: subcategory2, subCatCount:15, like: 0.142857142857}])
WITH apoc.map.fromLists(collect(distinct sub.name), collect(distinct {subcategory: sub.name, subCatCount:subCatInterests.count, like: toFloat(subCatInterests.count)/toFloat(totalLikes)})) as catScore,
apoc.map.fromLists(collect(distinct br.name), collect(distinct {brand: br.name, brandCount:brandInterests.count, like: toFloat(brandInterests.count)/toFloat(totalLikes)})) as brScore, 
apoc.map.fromLists(collect(distinct pl.name), collect(distinct {priceLevel: pl.name, proiceLevelCount: priceLevelInterests.count, like: toFloat(priceLevelInterests.count)/toFloat(totalLikes)})) as plScore, 
collect(distinct br.name) AS brands, collect(distinct sub.name) AS categories,
collect(distinct pl.name) AS priceLevels,
totalLikes, purchasedCombinations, purchasedBrandSubCombinations, purchasedSKUs, categoryGenderPreferences, categoryAgeGroupPreferences, preferredGenders, catBrSizePreferences, categorySizePreferences


// GET RECOMMENDED PRODUCTS BY SUBCATEGORY, BRAND, PRICE LEVEL, VISIBILITY, AND SIZE PREFERENCES
MATCH (recommendedProducts :Product)-[:HAS_SIMPLE]->(simple:ProductSimple)
WHERE recommendedProducts.subcategory IN categories
AND recommendedProducts.brand IN brands
AND recommendedProducts.priceLevel IN priceLevels
AND recommendedProducts.gender IN preferredGenders
// DO NOT INCLUDE PRODUCTS THAT WERE ALREADY PURCHASED
AND NOT recommendedProducts.sku_config IN purchasedSKUs
AND recommendedProducts.isVisible = true
AND (
  ((COALESCE(recommendedProducts.subcategory ,"") + ',' + COALESCE(recommendedProducts.brand ,"")+ ',' + COALESCE(simple.size ,"")) IN catBrSizePreferences )
  OR 
  ((COALESCE(recommendedProducts.subcategory ,"") + ',' + COALESCE(simple.size ,"")) IN categorySizePreferences)
)

// Get VISIBILITY and STOCK of products based on the country in which the customer is purchasing right now
WITH collect({sku_config: recommendedProducts.sku_config, sku:simple.sku, size:simple.size,
 subcategory:recommendedProducts.subcategory,
  name: recommendedProducts.name, brand: recommendedProducts.brand, ageGroup:recommendedProducts.ageGroup,
  priceLevel: recommendedProducts.priceLevel, gender: recommendedProducts.gender })[..1] AS recommendations, 
  recommendedProducts.sku_config AS sku_config,
  apoc.map.get(brScore, recommendedProducts.brand, 0, false) as brScore,
  apoc.map.get(catScore, recommendedProducts.subcategory, 0, false) as catScore,
  apoc.map.get(plScore, recommendedProducts.priceLevel, 0, false) as plScore,
  purchasedCombinations, purchasedBrandSubCombinations, purchasedSKUs, categoryGenderPreferences, categoryAgeGroupPreferences,
  preferredGenders,
  CASE $country
    WHEN "AE" THEN recommendedProducts.country_visibility_ae
    WHEN "BH" THEN recommendedProducts.country_visibility_bh
    WHEN "KW" THEN recommendedProducts.country_visibility_kw
    WHEN "OM" THEN recommendedProducts.country_visibility_om
    WHEN "QA" THEN recommendedProducts.country_visibility_qa
    WHEN "SA" THEN recommendedProducts.country_visibility_sa
    WHEN "US" THEN recommendedProducts.country_visibility_us
  END AS productVisibility,
  CASE $country
    WHEN "SA" THEN recommendedProducts.ksaUnits
    ELSE recommendedProducts.uaeUnits
  END AS stock

UNWIND recommendations as recommendedProducts

// CALCULATE SCORES FOR EACH PRODUCT BASED ON BRAND, SUBCATEGORY AND PRICE LEVEL SCORES
// FILTER products which are not available based on visibility and stocks
WITH recommendedProducts,(5 * catScore.like + 3 * brScore.like + plScore.like) as score, 
 recommendedProducts.brand as brandName, recommendedProducts.subcategory as subCategoryName, recommendedProducts.priceLevel as priceLevel, recommendedProducts.gender as gender,
 (COALESCE(recommendedProducts.subcategory ,"") + ',' + COALESCE(recommendedProducts.gender ,"")) AS genderSubCombination,
 (COALESCE(recommendedProducts.subcategory ,"") + ',' + COALESCE(recommendedProducts.ageGroup ,"")) AS productAgeCategoryCombination,
 (COALESCE(recommendedProducts.subcategory ,"") + ',' + COALESCE(recommendedProducts.brand ,"")) AS brandSubcombination,
 (COALESCE(recommendedProducts.subcategory ,"") + ',' + COALESCE(recommendedProducts.brand ,"")+ ',' + COALESCE(recommendedProducts.priceLevel ,"")) AS combination,
 purchasedCombinations, purchasedBrandSubCombinations, purchasedSKUs, categoryGenderPreferences, categoryAgeGroupPreferences, productVisibility, stock
WHERE stock > 0 AND productVisibility = true

// BOOST SCORES OF PRODUCTS UNDER THE PURCHASED COMBINATIONS
WITH 
 recommendedProducts.sku_config as sku_config, recommendedProducts.name as productName, recommendedProducts.sku as sku, recommendedProducts.size as size, productVisibility, stock,
  CASE
    WHEN combination IN purchasedCombinations THEN score * 9
    WHEN brandSubcombination IN purchasedBrandSubCombinations THEN score * 7
    ELSE score
  END AS productScore, brandName, subCategoryName, priceLevel, combination,brandSubcombination,categoryGenderPreferences, categoryAgeGroupPreferences,genderSubCombination,productAgeCategoryCombination
  ORDER BY productScore DESC


WITH 
 sku_config, productName,size, sku, productVisibility, stock,
  CASE
    WHEN genderSubCombination IN categoryGenderPreferences THEN productScore * 5
    WHEN productAgeCategoryCombination IN categoryAgeGroupPreferences THEN productScore * 3
    ELSE productScore
  END AS productScore, brandName, subCategoryName, priceLevel, combination,brandSubcombination,genderSubCombination, productAgeCategoryCombination as ageCatagoryCombination
  ORDER BY productScore DESC

// Collecting SKUs by combination
WITH collect({ sku_config:sku_config, 
 sku: sku,
 size: size,
 stock: stock,
 productVisibility : productVisibility,
 productName:productName, productScore:productScore,
 brandName:brandName, combination:combination,
 brandSubcombination: brandSubcombination,
 subCategoryName:subCategoryName, 
 ageCatagoryCombination:ageCatagoryCombination,
 genderSubCombination: genderSubCombination,
 priceLevel: priceLevel}) as recommendedProducts, brandSubcombination, RANGE(1, COUNT(sku_config)) AS indexes

// Creating layers of recommendations by calculating a sort value for each recommendation
UNWIND indexes AS i
CALL {
  WITH recommendedProducts, i, brandSubcombination, indexes
  RETURN apoc.map.setKey(recommendedProducts[i-1], "sortVal", indexes[i-1]) AS recommendations
}

// Calculating maxScore for score normalization
WITH collect(recommendations) as rps, max(recommendations.productScore) as maxScore

UNWIND rps as recommendations

RETURN recommendations.sku_config AS sku_config, recommendations.productName AS productName, recommendations.stock as stock,  recommendations.productVisibility as isVisible, recommendations.size as size,  recommendations.sku as sku,
recommendations.subCategoryName as category, recommendations.brandName as brand,recommendations.ageCatagoryCombination as acc, recommendations.genderSubCombination as gsc, recommendations.priceLevel as priceLevel,
(recommendations.productScore/maxScore) as score, recommendations.brandSubcombination as brandSubCombination, recommendations.combination as combination, 
recommendations.sortVal as index
ORDER BY recommendations.sortVal,recommendations.productScore DESC
LIMIT 10000