const { driver } = require('./db');
const {formatProductDetails} = require('./utils');

const session = driver.session();

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

let totalRecords = 0;
let filesuffix = "";

let customerMap = {}
let productMap = {}
const customerWishlistProductMap = {}

const extractAndLoad = async() => {

    try {
        const dirPath = 'csv';
        const files = fs.readdirSync(dirPath);

        const data = [];
        for (const file of files) {
            if (file.startsWith('customer_wishlist') && file.endsWith('.csv')) {
                const filePath = path.join(dirPath, file);
                filesuffix = filePath.slice(-7)
                totalRecords = 0;
                const stream = fs.createReadStream(filePath).pipe(csv());

                let buffer = [];
                for await (const row of stream) {
                    buffer.push(row);

                    if (buffer.length == 3000) {
                        // Process the buffer
                        await load(buffer)
                        // Clear the buffer
                        buffer = [];
                    }
                }

                // Process any remaining rows in the buffer
                if (buffer.length > 0) {
                    await load(buffer)
                }
            }
        }

        return data;
    } catch (error) {
        console.error(error);
    }

}

const createProductNodes = async (tx, fk_customer, sku, name, activated_at, short_description, namshi_description, brand, department, season, category, subcategory, product_gender, age_group, price_level, merch_type, occasion, special_type, color, world_tag = null) => {

    if(productMap[sku]){
        return true;
    }
    const result = await tx.run(`
    MERGE (product:Product {sku_config: $skuConfig, name: $productName, activated_at: $activatedAt, short_description: $shortDescription, description: $namshiDescription, gender:$productGender, ageGroup:$ageGroup, subcategory:$subCategory, priceLevel:$priceLevel, brand:$brand})
    MERGE (brand:Brand {name:$brand})
    MERGE (dept:Department {name: $dept})
    MERGE (season:Season {name: $season})
    MERGE (category:Category {name: $category})
    MERGE (subcategory:Subcategory {name: $subCategory})
    MERGE (gender:Gender {name: $productGender})
    MERGE (ageGroup:AgeGroup {name: $ageGroup})
    MERGE (priceLevel:PriceLevel {name: $priceLevel})
    MERGE (merchType:MerchType {name: $merchType})
    MERGE (occasion:Occasion {name: $occasion})
    
    MERGE(product)-[:IN_CATEGORY]->(category)
    MERGE(product)-[:IN_SUBCATEGORY]->(subcategory)
    MERGE(category)-[:HAS_CHILD]->(subcategory)
    MERGE(product)-[:IN_BRAND]->(brand)
    MERGE(product)-[:IN_GENDER]->(gender)
    MERGE(product)-[:IN_DEPARTMENT]->(dept)
    MERGE(product)-[:BELONGS_TO]->(season)
    MERGE(product)-[:FOR_OCCASION]->(occasion)
    MERGE(product)-[:IN_PRICE_LEVEL]->(priceLevel)
    MERGE(product)-[:IN_MERCH_TYPE]->(merchType)
    MERGE(product)-[:IN_AGE_GROUP]->(ageGroup)

    `,{
        skuConfig: sku,
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
        occasion: occasion
    })

    productMap[sku] = true;

    if(color){
        await tx.run(`
        MATCH (c:Customer {id: $customerId})
        MATCH (p:Product {sku_config: $sku})
        MERGE (co:Color {name: $color})
        MERGE(c)-[:LIKES]->(co)
        MERGE(p)-[:HAS_COLOR]->(co)
        `,{
            customerId: fk_customer,
            sku:sku,
            color: color
        })   
    }

    if(special_type) {
        await tx.run(`
            MATCH (p:Product {sku_config: $skuConfig})
            MERGE (st:SpecialType {name: $specialType})
            MERGE (p)-[:HAS_TYPE]->(st)
        `,{
            skuConfig: sku,
            specialType: special_type
        })
    }

    if(world_tag) {
        await tx.run(`
        MATCH (p:Product {sku_config: $skuConfig})
        MERGE (wt:WorldTag {name: $worldTag})
        MERGE (p)-[:TAGGED_AS]->(wt)
        `, {
            skuConfig: sku,
            worldTag: world_tag,
        })
    }
    return true;

}

const createCustomerNodes = async (tx, customerId, RPCGross, RPCNet, TPCNet, countryName, gender, generationName, generationYear ) => {

    if(customerMap[customerId]){
        return true;
    }
    const result = await tx.run(`
        MERGE (c:Customer {id: $customerId, gross_rpc_per_month: $RPCGross , net_rpc_per_month: $RPCNet, tpc_per_month: $TPC})
    `,{
        customerId: customerId,
        RPCGross: RPCGross?RPCGross:0.00,
        RPCNet: RPCNet?RPCNet:0.00,
        TPC: TPCNet?TPCNet:0.00
    })
    customerMap[customerId] = true;

    if(gender) {
        await tx.run(`
        MATCH (c:Customer {id: $customerId})
        MERGE (g:Gender {name: $gender})
        MERGE (c)-[:BELONGS_TO]->(g)
        `,{
            customerId: customerId,
            gender: gender
        })
    }


    if(generationName) {
        await tx.run(`
        MATCH (c:Customer {id: $customerId})
        MERGE (gen:Generation {name: $generationName, year: $generationYear})
        MERGE (c)-[:IN_GENERATION]->(gen)
        `, {
            customerId: customerId,
            generationName: generationName,
            generationYear: generationYear,
        })
    }
    
    if(countryName) {
        await tx.run(`
        MATCH (c:Customer {id: $customerId})
        MERGE (co:Country {name: $countryName})
        MERGE (c)-[:IN_COUNTRY]->(co)
        `, {
            customerId: customerId,
            countryName: countryName
        })
    }
    return true;

}

const createCustomerPurchaseRelationship = async (tx, customerId, sku, orderDate, orderNr, salePrice) => {

    const result = await tx.run(`
    MATCH (product:Product {sku_config: $skuConfig})
    MATCH (customer:Customer {id: $customerId})
    MERGE (order:Order {order_date: $orderDateKey, order_nr: $orderNr})
    MERGE(customer)-[r:BOUGHT {order_date: $orderDateKey, id: $orderNr}]->(product)
        ON CREATE 
            SET r.sale_price = $salePrice
    MERGE (customer)-[rel:INTERESTED_IN]->(product)
        ON CREATE
            SET rel.count = 1
        ON MATCH
            SET rel.count = r.count + 1
    MERGE (product)-[:PART_OF]->(order)
    MERGE (customer)-[:ORDERED]->(order)
    RETURN customer, product
    `, 
    {
        skuConfig: sku,
        orderDateKey: orderDate,
        orderNr: orderNr,
        salePrice: salePrice,
        customerId: customerId
    })
    return true;
}

const createCustomerWishlistRelationship = async (tx, customerId, sku, updatedAt) => {
    const result = await tx.run(`
        MATCH (product:Product {sku_config: $skuConfig})
        MATCH (customer:Customer {id: $customerId})

        MERGE(product)-[wishlistRel:IN_WISHLIST {updated_at: $updatedAt}]->(customer)
            ON CREATE SET wishlistRel.count = 1
        `, 
        {
            skuConfig: sku,
            customerId: customerId,
            updatedAt: updatedAt
        })
    return true;
}

const createCustomerProductAttrRel = async (tx, customerId, sku, category, subcategory, brand, priceLevel, department, merchType) => {

    await tx.run(`

        MATCH (customer:Customer {id: $customerId})
        MATCH (brand:Brand {name: $brand})
        MATCH (dept:Department {name: $dept})
        MATCH (category:Category {name: $category})
        MATCH (subcategory:Subcategory {name: $subCategory})
        MATCH (priceLevel:PriceLevel {name: $priceLevel})
        MATCH (merchType:MerchType {name: $merchType})

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
        brand,
        dept: department,
        category,
        subCategory: subcategory,
        priceLevel,
        merchType
    })

    if(!customerWishlistProductMap[customerId]){
        customerWishlistProductMap[customerId] = {}
    }
    customerWishlistProductMap[customerId][sku] = true;

    return true;
}


const load = async (buffer) => {

    try {
        const tx = session.beginTransaction();
        
        for ( let productDetails of buffer) {

            productDetails = formatProductDetails(productDetails);
            const {
                sku_config, fk_customer, 
                order_date_key, order_nr, sale_price, wishlist_updated_at,
                product_mapping_type,
                brand, name, activated_at,
                season, color, department,
                category, subcategory,
                product_gender, age_group,
                price, price_level, merch_type,
                occasion, short_description, namshi_description,
                special_type, world_tag,
                gender, customer_generation_name, customer_generation_year,
                country_name,
                RPC_gross_per_month, RPC_gross_per_month_1, RPC_net_per_month, TPC_per_month
            } = productDetails

            if (!customerMap[fk_customer]) {
                await createCustomerNodes(tx,fk_customer,RPC_gross_per_month,(RPC_net_per_month?RPC_net_per_month : RPC_gross_per_month_1),TPC_per_month, country_name, gender, customer_generation_name, customer_generation_year)
            } 

            if (!productMap[sku_config]) {
                await createProductNodes(tx, fk_customer, sku_config, name, activated_at, short_description, namshi_description, brand, department, season, category, subcategory, product_gender, age_group, price_level, merch_type, occasion, special_type, color, world_tag)        
            } 
              
            if (product_mapping_type == "purchase_history") {
                await createCustomerPurchaseRelationship(tx, fk_customer, sku_config, order_date_key, order_nr, sale_price)
                await createCustomerProductAttrRel(tx, fk_customer, sku_config, category, subcategory, brand, price_level, department, merch_type)
            }
            else if (product_mapping_type == "wishlist") {

                if(customerWishlistProductMap[fk_customer] && customerWishlistProductMap[fk_customer][sku_config]){
                    console.log("customer wishlist sku mapping exists", fk_customer, sku_config)
                } else {
                    await createCustomerWishlistRelationship(tx, fk_customer, sku_config, wishlist_updated_at)
                    await createCustomerProductAttrRel(tx, fk_customer, sku_config, category, subcategory, brand, price_level, department, merch_type)
                }
            }

            totalRecords = totalRecords + 1;
            console.log(fk_customer, sku_config, totalRecords+' - '+filesuffix);

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