const { driver } = require('./db');
const {formatProductDetails, readAndLoadCSV} = require('./utils');

const session = driver.session();

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

let totalRecords = 0;
let filesuffix = "";

async function extractAndLoad() {
    try {
        const dirPath = '../csv/catalog-data';
        const filePrefix = 'item_d'

        const data = await readAndLoadCSV(dirPath, filePrefix, load)

        return data;
    } catch (error) {
        console.error(error);
    }
}

const createProductNodes = async (tx, productDetails) => {

    productDetails = formatProductDetails(productDetails);
    const {
        sku,sku_config,
        activated_at,
        size,ksa_stocks,uae_stocks,
        is_visible,
        ae_visibility,bh_visibility,kw_visibility,om_visibility,
        qa_visibility,sa_visibility,us_visibility,
        brand,name,season,color,department,
        category,subcategory,
        product_gender,age_group,price,price_level,merch_type,
        occasion,short_description,namshi_description,
        special_type,world_tag
    } = productDetails;

    if(!category || !subcategory || category.length <= 0 || subcategory.length <=0){
        console.log("Not merging this sku due to empty cat, subcat", category,subcategory);
        return false;
    }

    const result = await tx.run(`
    MERGE (product:Product {sku_config: $skuConfig})
        ON CREATE 
            SET product.name = $productName, product.activated_at = $activatedAt, 
            product.short_description = $shortDescription, product.description = $namshiDescription, product.gender = $productGender, 
            product.ageGroup = $ageGroup, product.subcategory = $subCategory, product.priceLevel = $priceLevel, product.brand = $brand,
            product.isVisible = $isVisible, product.country_visibility_ae = $aeVisibility, product.country_visibility_bh = $bhVisibility,
            product.country_visibility_kw = $kwVisibility, product.country_visibility_om = $omVisibility, 
            product.country_visibility_qa = $qaVisibility, product.country_visibility_sa = $saVisibility, 
            product.country_visibility_us = $usVisibility
        ON MATCH 
            SET product.isVisible = $isVisible, product.country_visibility_ae = $aeVisibility, product.country_visibility_bh = $bhVisibility,
            product.country_visibility_kw = $kwVisibility, product.country_visibility_om = $omVisibility, 
            product.country_visibility_qa = $qaVisibility, product.country_visibility_sa = $saVisibility, 
            product.country_visibility_us = $usVisibility
    
    MERGE (brand:Brand {name:$brand})
    MERGE (dept:Department {name: $dept})
    MERGE (season:Season {name: $season})
    MERGE (category:Category {name: $category})
    MERGE (subcategory:Subcategory {name: $subCategory})
    MERGE (gender:Gender {name: $productGender})
    MERGE (ageGroup:AgeGroup {name: $ageGroup})
    MERGE (priceLevel:PriceLevel {name: $priceLevel})
    MERGE (merchType:MerchType {name: $merchType})
    
    MERGE(product)-[:IN_CATEGORY]->(category)
    MERGE(product)-[:IN_SUBCATEGORY]->(subcategory)
    MERGE(category)-[:HAS_CHILD]->(subcategory)
    MERGE(product)-[:IN_BRAND]->(brand)
    MERGE(product)-[:IN_GENDER]->(gender)
    MERGE(product)-[:IN_DEPARTMENT]->(dept)
    MERGE(product)-[:BELONGS_TO]->(season)
    MERGE(product)-[:IN_PRICE_LEVEL]->(priceLevel)
    MERGE(product)-[:IN_MERCH_TYPE]->(merchType)
    MERGE(product)-[:IN_AGE_GROUP]->(ageGroup)

    `,{
        sku:sku,
        skuConfig: sku_config,
        productName: name,
        activatedAt: activated_at,
        shortDescription: short_description,
        namshiDescription: namshi_description,
        brand: brand,
        dept: department,
        season: season,
        category:category,
        subCategory: subcategory,
        productGender: product_gender,
        ageGroup: age_group,
        priceLevel: price_level,
        merchType: merch_type,
        isVisible:eval(is_visible), 
        aeVisibility: eval(ae_visibility), 
        bhVisibility: eval(bh_visibility),
        kwVisibility: eval(kw_visibility),
        omVisibility: eval(om_visibility), 
        qaVisibility: eval(qa_visibility),
        saVisibility: eval(sa_visibility), 
        usVisibility: eval(us_visibility),
        size: size, ksaUnits: ksa_stocks, uaeUnits: uae_stocks
    })

    if(color){
        await tx.run(`
        MATCH (p:Product {sku_config: $skuConfig})
        MERGE (co:Color {name: $color})
        MERGE(p)-[:HAS_COLOR]->(co)
        `,{
            skuConfig:sku_config,
            color: color
        })   
    }

    if(special_type) {
        await tx.run(`
            MATCH (p:Product {sku_config: $skuConfig})
            MERGE (st:SpecialType {name: $specialType})
            MERGE (p)-[:HAS_TYPE]->(st)
        `,{
            skuConfig: sku_config,
            specialType: special_type
        })
    }

    if(occasion) {
        await tx.run(`
            MATCH (p:Product {sku_config: $skuConfig})
            MERGE (occasion:Occasion {name: $occasion})
            MERGE(product)-[:FOR_OCCASION]->(occasion)
        `,{
            skuConfig: sku_config,
            occasion: occasion
        })
    }

    if(world_tag) {
        await tx.run(`
        MATCH (p:Product {sku_config: $skuConfig})
        MERGE (wt:WorldTag {name: $worldTag})
        MERGE (p)-[:TAGGED_AS]->(wt)
        `, {
            skuConfig: sku_config,
            worldTag: world_tag,
        })
    }

   

    return true;

}

async function createProductSimpleNode (tx, product){
 
    const {
        sku, sku_config, size, ksa_stocks, uae_stocks
    } = product

    await tx.run(`

    MERGE (product:Product {sku_config: $skuConfig})    
    MERGE (simple:ProductSimple {sku: $sku})
        ON CREATE
         SET simple.sku_config= $skuConfig, simple.size= $size, simple.ksaUnits = $ksaUnits, simple.uaeUnits = $uaeUnits
        ON MATCH 
         SET simple.ksaUnits = $ksaUnits, simple.uaeUnits = $uaeUnits
    MERGE(product)-[:HAS_SIMPLE]->(simple)
    `,{
        sku:sku,
        skuConfig:sku_config,
        size:size,
        ksaUnits: ksa_stocks,
        uaeUnits:uae_stocks
    })

     await tx.run(`
        MATCH (p:Product {sku_config: $skuConfig})-[:HAS_SIMPLE]->(s:ProductSimple)
        WITH p, sum(toInteger(s.ksaUnits)) as totalKSAUnits, sum(toInteger(s.uaeUnits)) as totalUAEUnits
        SET p.ksaUnits = totalKSAUnits, p.uaeUnits = totalUAEUnits
        `, {
            skuConfig: sku_config
        })

    return true;
}


async function load(products, totalRecords = 0, filesuffix = "") {
    try {
        const tx = session.beginTransaction();
        for (const product of products) {
            
            await createProductNodes(tx,product)                
            await createProductSimpleNode(tx,product)
            
            totalRecords = totalRecords + 1;
            console.log(product.sku, product.sku_config, totalRecords+' - '+filesuffix);

        }
        await tx.commit();
        console.log("Commited transaction", totalRecords);
    } catch (error) {
        console.error('Error creating product nodes and relationships:', error);
    } 
}


(async () => {
    console.log('Starting process')
    await extractAndLoad();
    await session.close();
    console.log('Stopping process ' + totalRecords)
    process.exit(1);
})();

