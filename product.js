const { driver } = require('./db');
const { readCSVFile } = require('./utils')
const { productCols } = require('./config')

const session = driver.session();

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const genderMap = {};
const categoryMap = {};
const colorMap = {};
const brandMap = {};
const customerMap = {};
const subcategoryMap = {};
const seasonMap = {};
const departmentMap = {};
const occasionMap = {};
const priceLevelMap = {};
const merchTypeMap = {};
const ageGroupMap = {}
const productMap = {}
const orderNrMap = {}

async function extractAndLoad() {
    try {
        const dirPath = 'csv';
        const files = fs.readdirSync(dirPath);

        const data = [];
        for (const file of files) {
            if (file.startsWith('sales-') && file.endsWith('.csv')) {
                const filePath = path.join(dirPath, file);
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


async function transform(data) {
    return data
}

async function getNodeFromMap(map, value, label, property, tx) {
    let node = map[value];
    if (!node) {
        const result = await tx.run(`MERGE (n:${label} {${property}: $value}) RETURN n`, { value: value });
        const records = result.records;
        if (records.length > 0) {
            node = records[0].get('n');
            map[value] = node;
        }
    }
    return node;
}


async function createProduct(productProps, tx) {
    const result = await tx.run(`
    CREATE (p:Product $props)
    RETURN p
  `, { props: productProps });
    const records = result.records;
    if (records.length > 0) {
        node = records[0].get('p');
    }
    return node;
}

async function createRelationShips(prop, tx) {

}


async function load(products) {
    try {
        const tx = session.beginTransaction();

        for (const product of products) {

            const {
                // bug here
                customer_gender: gender,
                category,
                color,
                brand,
                fk_customer: customer,
                subcategory,
                season,
                department,
                occasion,
                price_level: priceLevel,
                merch_type: merchType,
                age_group: ageGroup,
                sku_config,
                order_nr: orderNr,
                ...productProps
            } = product;

            if (!productMap[sku_config]) {
                const productNode = await createProduct({ sku_config, ...productProps }, tx)
                productMap[sku_config] = true;
            }

            const [
                genderNode,
                categoryNode,
                colorNode,
                brandNode,
                customerNode,
                subcategoryNode,
                seasonNode,
                departmentNode,
                occasionNode,
                priceLevelNode,
                merchTypeNode,
                ageGroupNode,
            ] = await Promise.all([
                getNodeFromMap(genderMap, gender, 'Gender', 'name', tx),
                getNodeFromMap(categoryMap, category, 'Category', 'name', tx),
                getNodeFromMap(colorMap, color, 'Color', 'name', tx),
                getNodeFromMap(brandMap, brand, 'Brand', 'name', tx),
                getNodeFromMap(customerMap, customer, 'Customer', 'id', tx),
                getNodeFromMap(subcategoryMap, subcategory, 'Subcategory', 'name', tx),
                getNodeFromMap(seasonMap, season, 'Season', 'name', tx),
                getNodeFromMap(departmentMap, department, 'Department', 'name', tx),
                getNodeFromMap(occasionMap, occasion, 'Occasion', 'name', tx),
                getNodeFromMap(priceLevelMap, priceLevel, 'PriceLevel', 'name', tx),
                getNodeFromMap(merchTypeMap, merchType, 'MerchType', 'name', tx),
                getNodeFromMap(ageGroupMap, ageGroup, 'AgeGroup', 'name', tx),
                getNodeFromMap(orderNrMap, orderNr, 'Order', 'id', tx),
            ]);

            await tx.run(`
          MATCH (p:Product {sku_config: $sku_config})
          MATCH (c:Category {name: $category})
          MATCH (s:Subcategory {name: $subcategory})
          MATCH (g:Gender {name: $gender})
          MATCH (cl:Color {name: $color})
          MATCH (b:Brand {name: $brand})
          MATCH (d:Department {name: $department})
          MATCH (o:Occasion {name: $occasion})
          MATCH (pl:PriceLevel {name: $priceLevel})
          MATCH (mt:MerchType {name: $merchType})
          MATCH (ag:AgeGroup {name: $ageGroup})
          MATCH (cust:Customer {id: $customerId})
          MATCH (order:Order {id: $orderNr})
          MERGE (cust)-[:BOUGHT]->(p)
          MERGE (cust)-[:BELONGS_TO]->(g)
          MERGE (p)-[:IN_CATEGORY]->(c)
          MERGE (p)-[:IN_SUBCATEGORY]->(s)
          MERGE (c)-[:HAS_CHILD]->(s)
          MERGE (p)-[:BELONGS_TO]->(s)
          MERGE (p)-[:IN_COLOR]->(cl)
          MERGE (p)-[:BY_BRAND]->(b)
          MERGE (p)-[:BELONGS_TO]->(d)
          MERGE (p)-[:FOR_OCCASION]->(o)
          MERGE (p)-[:HAS_PRICE_LEVEL]->(pl)
          MERGE (p)-[:HAS_MERCH_TYPE]->(mt)
          MERGE (p)-[:FOR_AGE_GROUP]->(ag)
          MERGE (p)-[:PART_OF]->(order)
        `, {
                sku_config: product.sku_config,
                customerId: product.fk_customer,
                gender: product.customer_gender,
                category: product.category,
                subcategory: product.subcategory,
                color: product.color,
                brand: product.brand,
                department: product.department,
                occasion: product.occasion,
                priceLevel: product.price_level,
                merchType: product.merch_type,
                ageGroup: product.age_group,
                orderNr: product.order_nr
            });

        }
        console.log('Product nodes and relationships created successfully.');
        await tx.commit();
    } catch (error) {
        console.error('Error creating product nodes and relationships:', error);
    } 
}


(async () => {
    await extractAndLoad();
    session.close();
    process.exit(1)
})();

