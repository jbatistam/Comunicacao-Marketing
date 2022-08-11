import json
import pandas as pd
import numpy as np
from mainFunction import *

##derick
def execute(event, context):
    try:
        # create dataframe with redshift query
        query = """WITH leads_table AS

          (SELECT   current_date                                           as analysis_day,
                    CONVERT_TIMEZONE('America/Sao_Paulo', l.created)::DATE as created,
                    lower(l.email)                                         as email,
                    'Cadastro Incompleto PF'                               as experiment_name,
                    l.first_name                                           as first_name,
                    f_get_mod_md5('CadastroIncompletoPF' || lower(l.email))       as hash, 

                    CASE
                        WHEN hash <= 9 THEN 'controle'
                        ELSE 'amostra 1'
                    END                                                    as groups,
                  
                    l.lead_type                                            as lead_type_leads,
                    
                    concat('55',l.phone)                                   as phone,
                    'BR'                                                   as locale,
                    lu.id                                                  as loggi_user_id,
                    lu.user_type                                           as lead_type_loggiuser,
                    
                    ROW_NUMBER () OVER (PARTITION BY lower(l.email) ORDER BY CONVERT_TIMEZONE('America/Sao_Paulo', l.created)::DATE ASC) lead_number

            FROM prodpostgres.leads_lead l

            LEFT JOIN prodpostgres.players_loggiuser  lu  
            ON lower(l.email)  = lower(lu.email)

            WHERE l.lead_type = 'personal'
            AND (lu.user_type IS NULL OR lu.user_type = 'PersonalCustomer')
            AND CONVERT_TIMEZONE('America/Sao_Paulo', l.created)::DATE >= '2020-09-03')
                        
                        
            SELECT *

            FROM leads_table l

            WHERE l.lead_number = 1 and groups = 'amostra 1'  AND  lead_type_loggiuser is null"""

        df = get_from_redshift(query, 'CadastroIncompletoPF')

        # Data Adjustment
        df = clean_dataset(df)
        df = hash_dataframe_by_day(df)
        df["loggi_user_id"] = df["loggi_user_id"].fillna(0)
        df["loggi_user_id"] = df["loggi_user_id"].astype(int)

        # to mkt cloud
        send_to_mkt_cloud(df, 'CadastroIncompletoPF.csv')

        # to redshift
        send_to_redshift(df, 'cadastroincompletopf', 'email', 'append', 'stg-marketing')

        return {
            'status': 200,
            'success': {
                'description': 'success',
            },
        }
    # debug exception
    except Exception as e:

        text = '<!here> CadastroIncompletoPF deu erro! Exception: ' + str(e)
        send_slack_message(text, 'Growth Bot', ':robot_face:')

        return {
            'status': 500,
            'error': {
                'type': type(e).__name__,
                'description': str(e),
            },
        }
