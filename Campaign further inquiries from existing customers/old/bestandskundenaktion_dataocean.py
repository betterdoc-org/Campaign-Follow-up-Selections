
'''BESTANDSKUNDENAKTION'''

import psycopg2
import pandas as pd
import borg_crypto 
import os
import datetime
from datetime import date

#==========================================================================================
## USER VARIABLE
filter_user = os.environ.get('USER')

if filter_user == 'npoensgen':
    filter_user_credentials = '/home/npoensgen/Documents/Persönlich/credentials_dataocean_NP.txt'
elif filter_user == 'nadine.poensgen':
    filter_user_credentials = '/Users/nadine.poensgen/Documents/Credentials/credentials_poensgen_dataocean.txt'

#==========================================================================================
## DATABASE CONNECTION
dataocean_connection = psycopg2.connect(open(filter_user_credentials).read())
dataocean_cursor = dataocean_connection.cursor()

with open('/Users/'+filter_user+'/Documents/GitHub/dataocean_campaigns/03. Bestandskunden/bestandskundenaktion_dataocean.sql') as query:
    globals()['df_bestandskunden'] = pd.read_sql(query.read(), dataocean_connection)

dataocean_connection.close()

#df_bestandskunden = df_bestandskunden.sort_values(by = 'case_id')
#==========================================================================================
## DECRYPT 
## title
for i in range(len(df_bestandskunden['title'])):
    if df_bestandskunden['title'][i] is None or df_bestandskunden['title'][i] == '' or df_bestandskunden['system'][i] == 'parc':
        df_bestandskunden['title'][i] = df_bestandskunden['title'][i]
    else:
        df_bestandskunden['title'][i] = borg_crypto.decrypt_string(df_bestandskunden['title'][i])

## first_name
for i in range(len(df_bestandskunden['first_name'])):
    if df_bestandskunden['first_name'][i] is None or df_bestandskunden['first_name'][i] == '' or df_bestandskunden['system'][i] == 'parc':
        df_bestandskunden['first_name'][i] = df_bestandskunden['first_name'][i]
    else:
        df_bestandskunden['first_name'][i] = borg_crypto.decrypt_string(df_bestandskunden['first_name'][i])

## last_name
for i in range(len(df_bestandskunden['last_name'])):
    if df_bestandskunden['last_name'][i] is None or df_bestandskunden['last_name'][i] == '' or df_bestandskunden['system'][i] == 'parc':
        df_bestandskunden['last_name'][i] = df_bestandskunden['last_name'][i]
    else:
        df_bestandskunden['last_name'][i] = borg_crypto.decrypt_string(df_bestandskunden['last_name'][i])

## birth_date
for i in range(len(df_bestandskunden['birth_date'])):
    if df_bestandskunden['birth_date'][i] is None or df_bestandskunden['birth_date'][i] == '' or df_bestandskunden['system'][i] == 'parc':
        df_bestandskunden['birth_date'][i] = df_bestandskunden['birth_date'][i]
    else:
        df_bestandskunden['birth_date'][i] = borg_crypto.decrypt_date(df_bestandskunden['birth_date'][i])
  
## street
for i in range(len(df_bestandskunden['street'])):
    if df_bestandskunden['street'][i] is None or df_bestandskunden['street'][i] == '' or df_bestandskunden['system'][i] == 'parc':
        df_bestandskunden['street'][i] = df_bestandskunden['street'][i]
    else:
        df_bestandskunden['street'][i] = borg_crypto.decrypt_string(df_bestandskunden['street'][i])

## street_number
for i in range(len(df_bestandskunden['street_number'])):
    if df_bestandskunden['street_number'][i] is None or df_bestandskunden['street_number'][i] == '' or df_bestandskunden['system'][i] == 'parc':
        df_bestandskunden['street_number'][i] = df_bestandskunden['street_number'][i]
    else:
        df_bestandskunden['street_number'][i] = borg_crypto.decrypt_string(df_bestandskunden['street_number'][i])

## zip
for i in range(len(df_bestandskunden['zip'])):
    if df_bestandskunden['zip'][i] is None or df_bestandskunden['zip'][i] == '' or df_bestandskunden['system'][i] == 'parc':
        df_bestandskunden['zip'][i] = df_bestandskunden['zip'][i]
    else:
        df_bestandskunden['zip'][i] = borg_crypto.decrypt_string(df_bestandskunden['zip'][i])

## zip
for i in range(len(df_bestandskunden['city'])):
    if df_bestandskunden['city'][i] is None or df_bestandskunden['city'][i] == '' or df_bestandskunden['system'][i] == 'parc':
        df_bestandskunden['city'][i] = df_bestandskunden['city'][i]
    else:
        df_bestandskunden['city'][i] = borg_crypto.decrypt_string(df_bestandskunden['city'][i])

## email 
for i in range(len(df_bestandskunden['email'])):
    if df_bestandskunden['email'][i] is None or df_bestandskunden['email'][i] == '' or '@' in df_bestandskunden['email'][i] or df_bestandskunden['system'][i] == 'parc':
        df_bestandskunden['email'][i] = df_bestandskunden['email'][i]
    else:
        df_bestandskunden['email'][i] = borg_crypto.decrypt_string(df_bestandskunden['email'][i])

## primary_phone
for i in range(len(df_bestandskunden['primary_phone'])):
    if df_bestandskunden['primary_phone'][i] is None or df_bestandskunden['primary_phone'][i] == '' or df_bestandskunden['system'][i] == 'parc':
        df_bestandskunden['primary_phone'][i] = df_bestandskunden['primary_phone'][i]
    else:
        df_bestandskunden['primary_phone'][i] = borg_crypto.decrypt_string(df_bestandskunden['primary_phone'][i])

## secondary_phone
for i in range(len(df_bestandskunden['secondary_phone'])):
    if df_bestandskunden['secondary_phone'][i] is None or df_bestandskunden['secondary_phone'][i] == '' or df_bestandskunden['system'][i] == 'parc':
        df_bestandskunden['secondary_phone'][i] = df_bestandskunden['secondary_phone'][i]
    else:
        df_bestandskunden['secondary_phone'][i] = borg_crypto.decrypt_string(df_bestandskunden['secondary_phone'][i])

#==========================================================================================

df_bestandskunden_google = df_bestandskunden[(df_bestandskunden['email'].str.contains('gmail') == True) | (df_bestandskunden['email'].str.contains('googlemail') == True)]

#==========================================================================================
## export
df_bestandskunden.to_excel('/Users/'+filter_user+'/Documents/selektion_bestandskunden_'+str(date.today())+'.xlsx', index=False)
df_bestandskunden_google.to_excel('/home/'+filter_user+'/Documents/GitHub/dataocean_campaigns/03. Bestandskunden/selektion_bestandskunden_google_'+str(date.today())+'.xlsx', index=False)



# THE END