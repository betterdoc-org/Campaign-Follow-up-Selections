#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 19 14:43:37 2022

@author: npoensgen
"""

#==========================================================================================
## IMPORT PACKAGES
import psycopg2
import pandas as pd
import borg_crypto 
import os
import datetime
from datetime import date

#==========================================================================================
## VARIABLEN

filter_user_credentials = '/Users/nadine.poensgen/Documents/Credentials/credentials_poensgen_dataocean.txt'

filter_start = datetime.datetime.strptime('2015-01-01','%Y-%m-%d').date()
#filter_end = datetime.datetime.strptime('2022-03-31','%Y-%m-%d').date()
filter_end = date.today()

#==========================================================================================
## DATABASE CONNECTION
dataocean_connection = psycopg2.connect(open(filter_user_credentials).read())
dataocean_cursor = dataocean_connection.cursor()

with open('/Users/nadine.poensgen/Documents/GitHub/dataocean_campaigns/01. google_rezension/cam_google_rezension.sql') as query:
    globals()['df_cam_google'] = pd.read_sql(query.read(), dataocean_connection)

dataocean_connection.close()

df_cam_google = df_cam_google
#==========================================================================================
## DECRYPT Email 
#Titel
for i in range(len(df_cam_google['title'])):
    try: df_cam_google['title'][i] = borg_crypto.decrypt_string(df_cam_google['title'][i])
    except: pass

#Vorname
for i in range(len(df_cam_google['first_name'])):
    try: df_cam_google['first_name'][i] = borg_crypto.decrypt_string(df_cam_google['first_name'][i])
    except: pass

#Nachname
for i in range(len(df_cam_google['last_name'])):
    try: df_cam_google['last_name'][i] = borg_crypto.decrypt_string(df_cam_google['last_name'][i])
    except: pass 

for i in range(len(df_cam_google['email'])):
    try: df_cam_google['email'][i] = borg_crypto.decrypt_string(df_cam_google['email'][i])
    except: pass

#==========================================================================================

df_all = df_cam_google

## FILTER FOR GOOGLE MAIL
df_cam_google = df_cam_google[(df_cam_google['email'].str.contains("@gmail.com")) | (df_cam_google['email'].str.contains("@google"))]

#==========================================================================================

filter_user_credentials_datalake = '/Users/nadine.poensgen/Documents/Credentials/credentials_poensgen_datalake.txt'
datalake_connection = psycopg2.connect(open(filter_user_credentials_datalake).read())
datalake_cursor = datalake_connection.cursor()

cam_previous = pd.read_sql('SELECT * FROM datalake_analytics.cam_select_29042022', datalake_connection)
cam_previous = cam_previous[cam_previous['cam_id'] == 3]

datalake_connection.close()

cam_previous = cam_previous.set_index('case_id')
df_cam_google = df_cam_google.set_index('masked_id')


df_cam_google = df_cam_google[~df_cam_google.index.isin(cam_previous.index)]
df_cam_google = df_cam_google.drop_duplicates(subset=['email'])


df_cam_google = df_cam_google.reset_index()
#THE END


test = df_all[df_all['email'] == 'hulyadiyen62@gmail.com']


df_cam_google.to_excel('/Users/nadine.poensgen/Documents/google_rezension.xlsx')




