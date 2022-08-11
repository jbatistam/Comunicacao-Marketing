import json

import pandas as pd
import numpy as np
from mainFunction import *

def execute(event, context):
    try:
        # create dataframe with redshift query
        query = """WITH first_last_order AS (
		              
		    SELECT o.customer_id,
            min(CONVERT_TIMEZONE('America/Sao_Paulo', o.created)::DATE) AS "first_order",
		    max(CONVERT_TIMEZONE('America/Sao_Paulo', o.created)::DATE) AS "last_order"
		
		
		    FROM prodpostgres.dispatch_order o                
            WHERE o.status IN ('finished','cancelledWithCharge')
			AND o.company_id is null
            GROUP BY 1),

reforge AS
            (SELECT  c.customer_id, 
            DATE_TRUNC('month', CONVERT_TIMEZONE('America/Sao_Paulo', c.finished))::DATE time_bin,
            ROW_NUMBER () OVER (PARTITION BY c.customer_id ORDER BY time_bin ASC) first_itinerary_rank,
                    
            CASE 
              WHEN time_bin = DATE_TRUNC('month', current_date)::DATE THEN 1 
              WHEN time_bin = DATE_TRUNC('month', DATEADD(month, -1, current_date))::DATE THEN 2 
              ELSE 0 --other months
            END AS status_number
                    
            FROM prodpostgres.dispatch_order c
            WHERE c.status in ('finished','cancelledWithCharge')
            AND c.customer_id is not null
            AND c.product != 1
            GROUP BY 1,2),

reforge_by_id AS (
                  
            SELECT reforge.customer_id, 
                
            CASE
                WHEN (SUM(reforge.status_number) = 1 AND SUM(reforge.first_itinerary_rank) = 1) THEN 'New'
                WHEN (SUM(reforge.status_number) = 1 AND SUM(reforge.first_itinerary_rank) != 1) THEN 'Resurrected'
                WHEN (SUM(reforge.status_number) = 2) THEN 'Dormant'
                WHEN (SUM(reforge.status_number) = 3) THEN 'Retained'
                ELSE 'Churned'
            END as account_status
        
        FROM reforge
        
        GROUP BY 1)
        
SELECT  pl.id, 
		    CONVERT_TIMEZONE('America/Sao_Paulo', pl.created)::DATE AS created,
		    DATE_TRUNC('week', current_date)::DATE analysis_week,
		    current_date as analysis_day,
		    pl.email, 
		    pl.first_name, 
		    concat('55',pl.mobile_1) AS phone,
        'BR' as locale, 
		    pl.user_type, 
		    f.first_order,
		    f.last_order,
		    a.state,
		    a.city,
		    r.account_status
		
		FROM prodpostgres.players_loggiuser pl
		
		LEFT JOIN first_last_order  f 
		ON pl.id = f.customer_id
		
		LEFT JOIN prodpostgres.players_loggiuseraddress a 
		ON pl.id = a.user_id
		
		LEFT JOIN reforge_by_id r 
		ON pl.id = r.customer_id
	
		WHERE pl.user_type = 'PersonalCustomer'
  	AND pl.is_superuser = False
  	AND CONVERT_TIMEZONE('America/Sao_Paulo', pl.created)::DATE = CAST(dateadd(hour, -3, getdate()) as date)"""

        df = get_from_redshift(query, 'JornadaSelfServicePF')

        # Data Adjustment
        df = clean_dataset(df)
        

        # to mkt cloud
        send_to_mkt_cloud(df, 'JornadaSelfServicePF.csv')

        return {
            'status': 200,
            'success': {
                'description': 'success',
            },
        }
    # debug exception
    except Exception as e:

        text = '<!here> JornadaSelfServicePF deu erro! Exception: ' + str(e)
        send_slack_message(text, 'Growth Bot', ':robot_face:')

        return {
            'status': 500,
            'error': {
                'type': type(e).__name__,
                'description': str(e),
            },
        }
