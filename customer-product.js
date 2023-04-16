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
const customerProductMap = {}
const customerWishlistProductMap = {}
const genderMap = {}
const brandMap = {}
const categoryMap = {}
const subcategoryMap = {}
const departmentMap = {}
const ageGroupMap = {}
const generationMap = {}
const priceLevelMap = {}
const merchTypeMap = {}
const worldTagMap = {}
const occasionMap = {}
const specialTypeMap = {}
const countryMap = {}
const colorMap = {}

const extractAndLoad = async() => {

    try {
        const dirPath = 'csv';
        const files = fs.readdirSync(dirPath);

        const data = [];
        for (const file of files) {
            if (file.startsWith('product_customer_') && file.endsWith('.csv')) {
                const filePath = path.join(dirPath, file);
                console.log(filePath)
                filesuffix = filePath.slice(-7)
                console.log(filesuffix)
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
        MATCH (customer:Customer {id: $customerId})
        MERGE (product:Product {sku_config: $skuConfig, name: $productName, activated_at: $activatedAt, short_description: $shortDescription, description: $namshiDescription})
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
        MERGE (specialtype:SpecialType {name: $specialType})
        
        MERGE(product)-[:IN_CATEGORY]->(category)
        MERGE(product)-[:IN_SUBCATEGORY]->(subcategory)
        MERGE(category)-[:HAS_CHILD]->(subcategory)
        MERGE(product)-[:IN_BRAND]->(brand)
        MERGE(product)-[:IN_GENDER]->(gender)
        MERGE(product)-[:IN_DEPARTMENT]->(dept)
        MERGE(product)-[:BELONGS_TO]->(season)
        MERGE(product)-[:HAS_TYPE]->(specialtype)
        MERGE(product)-[:FOR_OCCASION]->(occasion)
        MERGE(product)-[:IN_PRICE_LEVEL]->(priceLevel)
        MERGE(product)-[:IN_MERCH_TYPE]->(merchType)
        MERGE(product)-[:IN_AGE_GROUP]->(ageGroup)

        MERGE(customer)-[:INTERESTED_IN]->(category)
        MERGE(customer)-[:INTERESTED_IN]->(subcategory)
        MERGE(customer)-[:LIKES]->(brand)
        MERGE(customer)-[:INTERESTED_IN]->(dept)
        MERGE(customer)-[:INTERESTED_IN]->(specialtype)
        MERGE(customer)-[:BOUGHT_PRICE_LEVEL]->(priceLevel)
        MERGE(customer)-[:INTERESTED_IN]->(merchType)
        MERGE(customer)-[:INTERESTED_IN]->(ageGroup)
        `,{
            customerId: fk_customer,
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
            occasion: occasion,
            specialType: special_type,
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
        RPCGross: RPCGross,
        RPCNet: RPCNet,
        TPC: TPCNet
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

const load = async (buffer) => {

    try {
        const tx = session.beginTransaction();
        
        for ( let productDetails of buffer) {

            let customerExistFlag = false;
            let productExistFlag = false;
            let wlProductExistFlag = false;

            productDetails = formatProductDetails(productDetails);
            const {
                sku_config,fk_customer,order_date_key,order_nr,
                brand,name,activated_at,
                season,color,department,
                category,subcategory,
                product_gender,age_group,
                price,price_level,merch_type,
                occasion,short_description,namshi_description,
                special_type,world_tag,
                customer_gender,customer_generation_name, customer_generation_year,
                customer_country_name,
                RPC_gross_per_month,RPC_net_per_month,TPC_per_month,
                wishlist_customer,wishlist_sku,wishlist_updated_at,
                wl_p_brand,wl_p_name,wl_p_activated_at,
                wl_p_season,wl_p_color,wl_p_department,
                wl_p_category,wl_p_subcategory,
                wl_p_product_gender,wl_p_age_group,
                wl_p_price,wl_p_price_level,wl_p_merch_type,
                wl_p_occasion,wl_p_short_description,wl_p_namshi_description,
                wl_p_special_type,
            } = productDetails

            if(!customerMap[fk_customer]){
                await createCustomerNodes(tx,fk_customer,RPC_gross_per_month,RPC_net_per_month,TPC_per_month, customer_country_name, customer_gender, customer_generation_name, customer_generation_year)
            } else { customerExistFlag = true}

            if(!productMap[sku_config]){
                await createProductNodes(tx, fk_customer, sku_config, name, activated_at, short_description, namshi_description, brand, department, season, category, subcategory, product_gender, age_group, price_level, merch_type, occasion, special_type, color, world_tag)        
            } else { productExistFlag = true}

            if(!productMap[wishlist_sku]){
                await createProductNodes(tx, fk_customer, wishlist_sku, wl_p_name, wl_p_activated_at, wl_p_short_description, wl_p_namshi_description, wl_p_brand, wl_p_department, wl_p_season, wl_p_category, wl_p_subcategory, wl_p_product_gender, wl_p_age_group, wl_p_price_level, wl_p_merch_type, wl_p_occasion, wl_p_special_type, wl_p_color)
            }else { wlProductExistFlag = true}
                          
            await tx.run(`
                MATCH (product:Product {sku_config: $skuConfig})
                MATCH (customer:Customer {id: $customerId})
                MATCH (wishlistProduct:Product {sku_config:$wishlistSku})
                MERGE (order:Order {id: $orderNr, order_date: $orderDateKey})

                MERGE(product)-[:PART_OF]->(order)
                
                MERGE(customer)-[:BOUGHT {order_date:$orderDateKey}]->(product)
                MERGE(customer)-[:ORDERED]->(order)

                MERGE(wishlistProduct)-[:IN_WISHLIST {updated_at: $updatedAt}]->(customer)
                `, 
                {
                    skuConfig: sku_config,
                    orderDateKey: order_date_key,
                    customerId: fk_customer,
                    orderNr: order_nr,
                    wishlistSku: wishlist_sku,
                    updatedAt: wishlist_updated_at
                })

            totalRecords = totalRecords + 1;
            console.log(fk_customer, sku_config,wishlist_sku, totalRecords+' - '+filesuffix, customerExistFlag, productExistFlag, wlProductExistFlag);

        }
        await tx.commit();
        console.log("Commited transaction");
        

    } catch (err) {
        console.error('Error creating product, customer, wishlist nodes and relationships:',err);
    }   
}

(async() => {
    console.log('Starting process')
    await extractAndLoad();
    await session.close();
    console.log('Stopping process ' + totalRecords)
    process.exit(1);
})();