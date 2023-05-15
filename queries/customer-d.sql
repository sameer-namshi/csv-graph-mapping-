SELECT 
    c.id_customer,
    c.country_name,
    c.gender,
    c.generation,
    c.RPC_gross_per_month,
    c.RPC_net_per_month,
    c.TPC_per_month,
    c.m0_flag,						
    c.m0006_flag,						
    c.m0612_flag,						
    c.m1218_flag,
    c.last_purchased,
    c.last_session_date
FROM
    `namshi-analytics.DatalabsSelfServe.customer_d` c 