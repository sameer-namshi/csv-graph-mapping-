const { driver } = require('./db');

const session = driver.session();

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const {readAndLoadCSV} = require('./utils');

let totalRecords = 0;
let filesuffix = "";

const customerWishlistProductMap = {};

const extractAndLoad = async() => {

    try {
        const dirPath = '../csv/sales-data';
        const filePrefix = 'item-sales'

        const data = await readAndLoadCSV(dirPath, filePrefix, load)

        return data;
    } catch (error) {
        console.error(error);
    }

}

const createCustomerPurchaseRelationship = async (tx, salesDetails) => {

    const {order_date_key, order_nr, fk_customer, 
        sku, sku_config, id_sales_order_item, unit_price} = salesDetails

    const result = await tx.run(`
    MATCH (product:Product {sku_config: $skuConfig})
    MATCH (simple:ProductSimple {sku_config: $sku})
    MATCH (customer:Customer {id: $customerId})
    MERGE (order:Order {order_date: $orderDateKey, order_nr: $orderNr})
    MERGE(customer)-[:BOUGHT {order_date: $orderDateKey, id: $orderNr}]->(product)
    MERGE(customer)-[r:BOUGHT {order_date: $orderDateKey, id: $orderNr, sales_order_item_id:$salesOrderItemId}]->(simple)
            ON CREATE 
                SET r.sale_price = $salePrice
    MERGE (customer)-[rel:INTERESTED_IN]->(product)
        ON CREATE
            SET rel.count = 1
        ON MATCH
            SET rel.count = r.count + 1
    MERGE (customer)-[relation:INTERESTED_IN]->(simple)
        ON CREATE
            SET relation.count = 1
        ON MATCH
            SET relation.count = r.count + 1
    MERGE (product)-[:PART_OF]->(order)
    MERGE (simple)-[:PART_OF {sales_order_item_id: $salesOrderItemId}]->(order)
    MERGE (customer)-[:ORDERED]->(order)
    RETURN customer, product
    `, 
    {
        skuConfig: sku_config,
        sku: sku,
        salesOrderItemId: id_sales_order_item,
        orderDateKey: order_date_key,
        orderNr: order_nr,
        salePrice: unit_price,
        customerId: fk_customer
    })
    return true;
}

const createCustomerWishlistRelationship = async (tx, salesDetails) => {
    const {
        sku_config, wishlist_updated_at, fk_customer
    } = salesDetails
    
    const result = await tx.run(`
        MATCH (product:Product {sku_config: $skuConfig})
        MATCH (customer:Customer {id: $customerId})

        MERGE(product)-[wishlistRel:IN_WISHLIST]->(customer)
            ON CREATE SET wishlistRel.count = 1, wishlistRel.updated_at = $updatedAt
        `, 
        {
            skuConfig: sku_config,
            customerId: fk_customer,
            updatedAt: wishlist_updated_at
        })
    return true;
}

const createCustomerProductAttrRel = async (tx, customerId, sku_config) => {

    await tx.run(`
        MATCH (customer:Customer {id: $customerId})
        MATCH (product:Product {sku_config: $skuConfig})
        MATCH (product)-[:IN_BRAND]->(brand:Brand)
        MATCH (product)-[:IN_DEPARTMENT]->(dept:Department)
        MATCH (product)-[:IN_CATEGORY]->(category:Category)
        MATCH (product)-[:IN_SUBCATEGORY]->(subcategory:Subcategory)
        MATCH (product)-[:IN_PRICE_LEVEL]->(priceLevel:PriceLevel)
        MATCH (product)-[:IN_MERCH_TYPE]->(merchType:MerchType)

        MERGE(customer)-[catRel:INTERESTED_IN]->(category)
            ON CREATE SET catRel.count = 1
            ON MATCH SET catRel.count = catRel.count + 1
        MERGE(customer)-[subcatRel:INTERESTED_IN]->(subcategory)
            ON CREATE SET subcatRel.count = 1
            ON MATCH SET subcatRel.count = subcatRel.count + 1
        MERGE(customer)-[brandRel:LIKES]->(brand)
            ON CREATE SET brandRel.count = 1
            ON MATCH SET brandRel.count = brandRel.count + 1
        MERGE(customer)-[deptRel:INTERESTED_IN]->(dept)
            ON CREATE SET deptRel.count = 1
            ON MATCH SET deptRel.count = deptRel.count + 1
        MERGE(customer)-[plRel:BOUGHT_PRICE_LEVEL]->(priceLevel)
            ON CREATE SET plRel.count = 1
            ON MATCH SET plRel.count = plRel.count + 1
        MERGE(customer)-[mtRel:INTERESTED_IN]->(merchType)
            ON CREATE SET mtRel.count = 1
            ON MATCH SET mtRel.count = mtRel.count + 1
    `, {
        customerId: customerId,
        skuConfig: sku_config
    })

    if(!customerWishlistProductMap[customerId]){
        customerWishlistProductMap[customerId] = {}
    }
    customerWishlistProductMap[customerId][sku_config] = true;

    return true;
}


const load = async (buffer, totalRecords = 0, filesuffix = "") => {

    try {
        const tx = session.beginTransaction();
        
        for ( let salesDetails of buffer) {

            if (salesDetails.product_mapping_type == "purchase_history") {
                await createCustomerPurchaseRelationship(tx, salesDetails)
                await createCustomerProductAttrRel(tx, salesDetails.fk_customer, salesDetails.sku_config)
            }
            else if (salesDetails.product_mapping_type == "wishlist") {
                await createCustomerWishlistRelationship(tx, salesDetails)
                await createCustomerProductAttrRel(tx, salesDetails.fk_customer, salesDetails.sku_config)
                
            }

            totalRecords = totalRecords + 1;
            console.log(salesDetails.fk_customer, salesDetails.sku_config, totalRecords+' - '+filesuffix);

        }
        await tx.commit();
        console.log("Commited transaction", totalRecords);
        

    } catch (err) {
        console.error('Error creating product, customer, wishlist nodes and relationships:',totalRecords, err);
    }   
}

(async() => {
    console.log('Starting process')
    await extractAndLoad();
    await session.close();
    console.log('Stopping process ' + totalRecords)
    process.exit(1);
})();