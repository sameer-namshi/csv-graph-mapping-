const { driver } = require('./db');
const {formatProductDetails} = require('./utils');

const session = driver.session();

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

const extractAndLoad = async() => {

    try {
        const dirPath = 'csv';
        const files = fs.readdirSync(dirPath);

        const data = [];
        for (const file of files) {
            if (file.startsWith('bq-results-') && file.endsWith('.csv')) {
                const filePath = path.join(dirPath, file);
                console.log(filePath)
                const stream = fs.createReadStream(filePath).pipe(csv());

                let buffer = [];
                for await (const row of stream) {
                    buffer.push(row);

                    if (buffer.length === 1000) {
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

const load = async (buffer) => {

    try {
        const tx = session.beginTransaction();
        
        for ( let productDetails of buffer) {

            productDetails = formatProductDetails(productDetails);
            const {
                sku_config,
                fk_customer,
                order_date_key,
                order_nr,
                wishlist_customer,
                wishlist_sku,
                wishlist_updated_at,
                brand,name,
                activated_at,
                season,color,
                department,category,
                subcategory,
                product_gender,
                age_group,price,
                price_level,merch_type,
                occasion,
                short_description,namshi_description,
                special_type,world_tag,
                customer_gender,
                customer_generation_name, customer_generation_year,
                customer_country_name,
                RPC_gross_per_month,RPC_net_per_month,TPC_per_month,
                wishlist_customer_gender,
                wishlist_customer_generation_name,wishlist_customer_generation_year,
                wishlist_customer_country,
                wishlist_rpc_gross,wishlist_rpc_net,wishlist_tpc

            } = productDetails

            await tx.run(`
                MERGE (p:Product {sku_config: $skuConfig, name: $productName, activated_at: $activatedAt, short_description: $shortDescription, description: $namshiDescription})
                MERGE (o:Order {id: $orderNr, order_date: $orderDateKey})
                MERGE (c:Customer {id: $customerId, gross_rpc_per_month: $RPCGross , net_rpc_per_month: $RPCNet, tpc_per_month: $TPC})
                MERGE (wc:Customer {id: $wishlistCustomer, gross_rpc_per_month: $wsRPCGross , net_rpc_per_month: $wsRPCNet, tpc_per_month: $wsTPC})
                MERGE (b:Brand {name:$brand})
                MERGE (d:Department {name: $dept})
                MERGE (s:Season {name: $season})
                MERGE (cat:Category {name: $category})
                MERGE (sc:Subcategory {name: $subCategory})
                MERGE (g:Gender {name: $productGender})
                MERGE (ag:AgeGroup {name: $ageGroup})
                MERGE (pl:PriceLevel {name: $priceLevel})
                MERGE (mt:MerchType {name: $merchType})
                MERGE (oc:Occasion {name: $occasion})
                MERGE (st:SpecialType {name: $specialType})
            `,{
                skuConfig: sku_config,
                productName: name,
                activatedAt: activated_at,
                shortDescription: short_description,
                namshiDescription: namshi_description,
                orderNr: order_nr,
                orderDateKey: order_date_key,
                customerId: fk_customer,
                RPCGross: RPC_gross_per_month,
                RPCNet: RPC_net_per_month,
                TPC: TPC_per_month,
                wishlistCustomer: wishlist_customer,
                wsRPCGross: wishlist_rpc_gross,
                wsRPCNet: wishlist_rpc_net,
                wsTPC: wishlist_tpc,
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
                                    
            await tx.run(`
                MATCH (product:Product {sku_config: "${sku_config}"})
                MATCH (category:Category {name: $category})
                MATCH (subcategory:Subcategory {name: $subcategory})
                MATCH (brand:Brand {name: $brand})
                MATCH (season:Season {name: $season})
                MATCH (color:Color {name: $color})
                MATCH (dept:Department {name: $department})
                MATCH (occasion:Occasion {name: $occasion})
                MATCH (specialtype:SpecialType {name: $specialType})
                MATCH (priceLevel:PriceLevel {name: $priceLevel})
                MATCH (merchType:MerchType {name: $merchType})
                MATCH (ageGroup:AgeGroup {name: $ageGroup})
                MATCH (customer:Customer {id: $customerId})
                MATCH (order:Order {id: $orderNr})
                MATCH (wishlistCust:Customer {id:$wishlistCustomerId})

                MERGE(product)-[:IN_CATEGORY]->(category)
                MERGE(product)-[:IN_SUBCATEGORY]->(subcategory)
                MERGE(category)-[:HAS_CHILD]->(subcategory)
                MERGE(product)-[:IN_BRAND]->(brand)
                MERGE(product)-[:IN_DEPARTMENT]->(dept)
                MERGE(product)-[:BELONGS_TO]->(season)
                MERGE(product)-[:HAS_TYPE]->(specialtype)
                MERGE(product)-[:FOR_OCCASION]->(occasion)
                MERGE(product)-[:IN_PRICE_LEVEL]->(priceLevel)
                MERGE(product)-[:IN_MERCH_TYPE]->(merchType)
                MERGE(product)-[:IN_AGE_GROUP]->(ageGroup)
                MERGE(product)-[:PART_OF]->(order)
                
                MERGE(customer)-[:BOUGHT {order_date:$orderDateKey}]->(product)
                MERGE(customer)-[:ORDERED]->(order)
                MERGE(customer)-[:INTERESTED_IN]->(category)
                MERGE(customer)-[:INTERESTED_IN]->(subcategory)
                MERGE(customer)-[:LIKES]->(brand)
                MERGE(customer)-[:INTERESTED_IN]->(dept)
                MERGE(customer)-[:BOUGHT_PRICE_LEVEL]->(priceLevel)
                MERGE(customer)-[:INTERESTED_IN]->(merchType)
                MERGE(customer)-[:INTERESTED_IN]->(ageGroup)

                MERGE(product)-[:IN_WISHLIST {updated_at: $updatedAt}]->(wishlistCust)
                `, 
                {
                    skuConfig: sku_config,
                    orderDateKey: order_date_key,
                    category: category,
                    subcategory: subcategory,
                    brand:brand,
                    season: season,
                    color: color,
                    department: department,
                    occasion: occasion,
                    specialType:special_type,
                    merchType: merch_type,
                    priceLevel: price_level,
                    ageGroup:age_group,
                    customerId: fk_customer,
                    orderNr: order_nr,
                    wishlistCustomerId: wishlist_customer,
                    updatedAt: wishlist_updated_at
                })

            if(color){
                await tx.run(`
                MATCH (c:Customer {id: $customerId})
                MATCH (p:Product {sku_config: $sku})
                MERGE (co:Color {name: $color})
                MERGE(c)-[:LIKES]->(co)
                MERGE(p)-[:HAS_COLOR]->(co)
                `,{
                    customerId: fk_customer,
                    sku:sku_config,
                    color: color
                })                
            }

            if(customer_gender) {
                await tx.run(`
                MATCH (c:Customer {id: $customerId})
                MERGE (g:Gender {name: $gender})
                MERGE (c)-[:BELONGS_TO]->(g)
                `,{
                    customerId: fk_customer,
                    gender: customer_gender
                })
            }

            if(wishlist_customer_gender) {
                await tx.run(`
                MATCH (c:Customer {id: $wsCustomer})
                MERGE (g:Gender {name: $gender})
                MERGE (c)-[:BELONGS_TO]->(g)
                `, {
                    wsCustomer: wishlist_customer,
                    gender: wishlist_customer_gender
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

            if(customer_generation_name) {
                await tx.run(`
                MATCH (c:Customer {id: $customerId})
                MERGE (gen:Generation {name: $generationName, year: $generationYear})
                MERGE (c)-[:IN_GENERATION]->(gen)
                `, {
                    customerId: fk_customer,
                    generationName: customer_generation_name,
                    generationYear: customer_generation_year,
                })
            }

            if(wishlist_customer_generation_name) {
                await tx.run(`
                MATCH (c:Customer {id: $customerId})
                MERGE (gen:Generation {name: $generationName, year: $generationYear})
                MERGE (c)-[:IN_GENERATION]->(gen)
                `, {
                    customerId: wishlist_customer,
                    generationName: wishlist_customer_generation_name,
                    generationYear: wishlist_customer_generation_year,
                })
            }
            
            if(customer_country_name) {
                await tx.run(`
                MATCH (c:Customer {id: $customerId})
                MERGE (co:Country {name: $countryName})
                MERGE (c)-[:IN_COUNTRY]->(co)
                `, {
                    customerId: fk_customer,
                    countryName: customer_country_name
                })
            }

            if(wishlist_customer_country) {
                await tx.run(`
                MATCH (c:Customer {id: $customerId})
                MERGE (co:Country {name: $countryName})
                MERGE (c)-[:IN_COUNTRY]->(co)
                `, {
                    customerId: fk_customer,
                    countryName: wishlist_customer_country
                })
            }
        }
        await tx.commit();

    } catch (err) {
        console.error('Error creating product, customer, wishlist nodes and relationships:', err);
    }   
}

(async() => {
    console.log('Starting process')
    await extractAndLoad();
    await session.close();
    console.log('Stopping process')
    process.exit(1);
})();