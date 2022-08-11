import json

import pandas as pd
import numpy as np
from mainFunction import *

def execute(event, context):
    try:
        # create dataframe with redshift query
        query = """
	WITH first_last_order AS (
		              
		    SELECT o.company_id,
            min(CONVERT_TIMEZONE('America/Sao_Paulo', o.created)::DATE) AS "first_order",
		    max(CONVERT_TIMEZONE('America/Sao_Paulo', o.created)::DATE) AS "last_order"
		    FROM prodpostgres.dispatch_order o                
        WHERE o.status IN ('finished','cancelledWithCharge')
			    AND o.company_id is null
        GROUP BY 1)

SELECT  
        pl.id, 
		    '' as is_smb,
		    '' as is_self_service,
		    '' as is_enterprise,
		    CONVERT_TIMEZONE('America/Sao_Paulo', comp.created)::DATE AS created,  
		    pl.email, 
		    pl.first_name, 
		    concat('55',pl.mobile_1) AS phone,
		    pl.user_type, 
		    f.first_order,
		    current_date as analysis_time, 
		    'BR' as locale, 
		    '' as motivo_de_descarte__c, 
		    current_date as last_table_update, 
		    'Jornada Self Service v2' as "experiment_name", 
		    f_get_mod_md5('JornadaSelfServicePJ' || pl.id) as hash,
		    CASE
			    WHEN hash <= 9 THEN 'controle'
			    ELSE 'amostra 1'
			  END AS "groups"		
		
		FROM prodpostgres.players_loggiuser pl
		
		JOIN prodpostgres.players_corporateuser corp
    ON pl.id = corp.loggiuser_ptr_id
           
    JOIN prodpostgres.players_company comp
    ON corp.company_id = comp.id
		
		LEFT JOIN first_last_order  f 
    ON corp.company_id = f.company_id
		
		WHERE pl.user_type = 'CorporateUser'
  	AND CONVERT_TIMEZONE('America/Sao_Paulo', comp.created)::DATE = CAST(dateadd(hour, -3, getdate()) as date)
    AND hash > 9 
	"""

        df = get_from_redshift(query, 'JornadaSelfServicePJ')

        # Data Adjustment
        df = clean_dataset(df)
        

        # to mkt cloud
        send_to_mkt_cloud(df, 'JornadaSelfServicePJ.csv')

        return {
            'status': 200,
            'success': {
                'description': 'success',
            },
        }
    # debug exception
    except Exception as e:

        text = '<!here> JornadaSelfServicePJ deu erro! Exception: ' + str(e)
        send_slack_message(text, 'Growth Bot', ':robot_face:')

        return {
            'status': 500,
            'error': {
                'type': type(e).__name__,
                'description': str(e),
            },
        }
