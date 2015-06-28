# -*- coding: utf-8 -*-
"""
Created on Mon Feb 02 17:26:23 2015

@author: ben
"""

import pandas as pd
import os
from copy import deepcopy

#Settings------------------------------------
output_filename='output_unsampled_investors_WEB.csv'
path_name='.\\'
output_path = '.\\UNSAMPLED_OUTPUT\\'

report_name='INVESTORS'
country_device_cc=7
date_range_cc = 25
column_names=['ga:yearMonth','ga:deviceCategory','ga:sessions','ga:users','ga:pageviews',
    'ga:entrances','ga:bounces','ga:sessionDuration']
skip_rows=7
#------------------------------------

report_name_cc=len(report_name)
segment_start=country_device_cc+report_name_cc+1

#Read in all CSV files in the MAIN folder only--------------------------------------------
csv_filenames=[]
for directory, dirnames, filenames in os.walk(path_name):
    if directory==path_name:
        for filenames_itr in filenames:
            if (filenames_itr.find('.csv')!=-1) & (filenames_itr[3:6] == 'WEB'):
                csv_filenames.append(filenames_itr)

year_month = csv_filenames[0][-14:-10]+csv_filenames[0][-9:-7]

#Loop through to clean file
countries=['VN','ID','SG','PH','TH','MY']
index=0
for csv_filenames_itr in csv_filenames:
    # Create a segment based on the filename
    segment=csv_filenames_itr[segment_start:len(csv_filenames_itr)-(date_range_cc+4)]
    segment = segment.replace('-',' ')
    for countries_itr in countries:
        # Find out the country by finding out if country string is there (CASE SENSITIVE)
        if csv_filenames_itr[0:2]==countries_itr:
            country=deepcopy(countries_itr)
            break

    #Read in the file
    df_temp=pd.read_csv(csv_filenames_itr,delimiter=',',skiprows=skip_rows,header=None, names=column_names)
    
    if df_temp.iloc[:,1].str.contains('ga:segment').sum() ==1: # If the file has a ga:segment column in it
        df_temp=pd.read_csv(csv_filenames_itr,delimiter=',',skiprows=skip_rows,header=0) #Read the csv again with different settings
        df_temp=df_temp.drop('ga:segment',axis=1) #Read the 
    else:
        df_temp=pd.read_csv(csv_filenames_itr,delimiter=',',skiprows=skip_rows,header=0)
        
    if df_temp.shape[0]<3: # For the ones that incomplete data
        device_list = ['desktop', 'mobile', 'tablet']
        if df_temp.shape[0] == 0:
            df_temp = pd.DataFrame(columns=column_names, index = range(3))
            for i,v in enumerate(device_list):
                row_data = [year_month,v]
                for i in range(df_temp.shape[1]-2):
                    row_data.append(0)
                df_temp.ix[i] = row_data
        else:
            missings = []
            for d in device_list:
                if d not in list(df_temp.iloc[:,1]):
                    missings.append(d)
                    
            for i,v in enumerate(missings):
                row_data = [year_month,v]
                for i in range(df_temp.shape[1]-2):
                    row_data.append(0)
                df_temp.ix[i+(len(device_list)-len(missings))] = row_data
                
    if str(df_temp.iloc[0,0]).find('#')>=0: # For ones with the meta data headers starting with #
        if df_temp.shape[0]==7: #for the ones that have the headers but no data
            continue
        else:
            df_temp=df_temp.drop(range(7)) #drop the meta data headers
        
    #Add in the new columns
    df_temp['Country']=country
    df_temp['Segment']=segment
    
    #Merge the files into single DF
    if index==0:
        df_new=deepcopy(df_temp)
    else:
        df_new=df_new.append(df_temp)
    index+=1

# File output
df_new.to_csv(output_path+output_filename,index=False)

       
        

