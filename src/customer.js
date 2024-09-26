const { driver } = require('./db');
const {formatCustomerDetails, readAndLoadCSV} = require('./utils');

const session = driver.session();

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

let totalRecords = 0;
let filesuffix = "";

async function extractAndLoad() {
    try {

        const dirPath = '../csv/customer-data';
        const filePrefix = 'customer_d'

        const data = await readAndLoadCSV(dirPath, filePrefix, load)

        return data;
    } catch (error) {
        console.error(error);
    }
}

const createCustomerNodes = async (tx, customerDetails) => {

    customerDetails = formatCustomerDetails(customerDetails)
    const {id_customer,country_name,
        customer_gender,customer_generation_name,customer_generation_year,
        RPC_gross_per_month,RPC_net_per_month,TPC_per_month,
        m0_flag, m0006_flag, m0612_flag, m1218_flag,
        last_purchased,last_session_date} = customerDetails

    const result = await tx.run(`
        MERGE (c:Customer {id: $customerId})
            ON CREATE
             SET c.gross_rpc_per_month= $RPCGross , c.net_rpc_per_month= $RPCNet, c.tpc_per_month = $TPC,
             c.last_purchased = $lastPurchased, c.last_session_date = $lastSessionDate, 
             c.m0_purchase = $m0purchase, c.m0006_purchase = $m0006Purchase, 
             c.m0612_purchase = $m0612Purchase, c.m1218_purchase = $m1218Purchase
            ON MATCH
             SET c.gross_rpc_per_month= $RPCGross , c.net_rpc_per_month= $RPCNet, c.tpc_per_month = $TPC,
             c.last_purchased = $lastPurchased, c.last_session_date = $lastSessionDate, 
             c.m0_purchase = $m0purchase, c.m0006_purchase = $m0006Purchase, 
             c.m0612_purchase = $m0612Purchase, c.m1218_purchase = $m1218Purchase
    `,{
        customerId: id_customer,
        RPCGross: RPC_gross_per_month ? RPC_gross_per_month:0.00,
        RPCNet: RPC_net_per_month ? RPC_net_per_month:0.00,
        TPC: TPC_per_month ? TPC_per_month:0.00,
        lastPurchased: last_purchased ? last_purchased : null,
        lastSessionDate: last_session_date ? last_session_date : null,
        m0purchase: m0_flag ? m0_flag : 0,
        m0006Purchase: m0006_flag ? m0006_flag : 0,
        m0612Purchase: m0612_flag ? m0612_flag : 0,
        m1218Purchase: m1218_flag ? m1218_flag : 0,

    })

    if(customer_gender) {
        await tx.run(`
        MATCH (c:Customer {id: $customerId})
        MERGE (g:Gender {name: $gender})
        MERGE (c)-[:BELONGS_TO]->(g)
        `,{
            customerId: id_customer,
            gender: customer_gender
        })
    }


    if(customer_generation_name) {
        await tx.run(`
        MATCH (c:Customer {id: $customerId})
        MERGE (gen:Generation {name: $generationName, year: $generationYear})
        MERGE (c)-[:IN_GENERATION]->(gen)
        `, {
            customerId: id_customer,
            generationName: customer_generation_name,
            generationYear: customer_generation_year,
        })
    }
    
    if(country_name) {
        await tx.run(`
        MATCH (c:Customer {id: $customerId})
        MERGE (co:Country {name: $countryName})
        MERGE (c)-[:IN_COUNTRY]->(co)
        `, {
            customerId: id_customer,
            countryName: country_name
        })
    }
    return true;

}

async function load(customers, totalRecords = 0, filesuffix = "") {
    try {
        const tx = session.beginTransaction();
        for (const customer of customers) {
            
            await createCustomerNodes(tx,customer)
            
            totalRecords = totalRecords + 1;
            console.log(customer.id_customer,totalRecords+' - '+filesuffix);

        }
        await tx.commit();
        console.log("Commited transaction", totalRecords);
    } catch (error) {
        console.error('Error creating customer nodes and relationships:', error);
    } 
}


(async () => {
    console.log('Starting process')
    await extractAndLoad();
    await session.close();
    console.log('Stopping process ' + totalRecords)
    process.exit(1);
})();

