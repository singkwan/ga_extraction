#!/bin/python
#
# GA extractor class

import httplib2
import sys
from datetime import date
from datetime import timedelta
from datetime import datetime

import time
import re
import itertools

from apiclient.discovery import build
from oauth2client.file import Storage
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.tools import run
from apiclient.errors import HttpError
import pandas as pd

import logging
from copy import deepcopy
import os

class ga_extractor(object):

    
    def __init__(self,config_file="C:\Users\Lazada\Google Drive\Analytics team\SK playground\credentials\\config.txt",id_mapping_file="C:\Users\Lazada\Google Drive\Analytics team\SK playground\credentials\\id_mapping.csv"):
        
        """ initialize the class. requires:
        1. Config file that has the API key to access the GA accounts
        2. id_mapping file that maps all the accounts to its respective table ids and country
        3. Source path """
        
        self.df_id_mapping=pd.read_csv(id_mapping_file)
        
        # Read in config data file and pass values        
        self.config_file=config_file        
        self.config_data = {}
        text=open(config_file,"r")
        a=[]
        for line in text:
            a= line.split(';') 
            self.config_data[a[0]] = a[1].strip()
        text.close()
        
        self.path_name=self.config_data['path_name']
        self.client_id=self.config_data['client_id']
        self.client_secret=self.config_data['client_secret']
       
        # Setup the logger to log the issues
        self.logger = logging.getLogger(self.config_file.split('.')[0]+"_logger")
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(self.path_name+"\\"+"ga_extractor_logger"+'.log')
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        # add the handlers to logger
        self.logger.addHandler(fh)

        # Authenticate and get a service object
        attempt_1=0
        while attempt_1 < 4:
            try:
            
                # Send in client secret and client ID to the authetication server. Need to set this up in google developer console to get the client secrets and ID
                # Then need to also activate google analytics in the allowed applications
                flow = OAuth2WebServerFlow(
                    self.client_id,
                    self.client_secret,
                    'https://www.googleapis.com/auth/analytics')
                
                # Stores the credentials in credentials.dat (i think)
                storage = Storage('credentials.dat')
                credentials = storage.get()
                if credentials is None or credentials.invalid:
                    credentials = run(flow, storage)
                
                # Use the credentials to get authentication?
                # Finally if this is the first time, your browser should pop to ask for login and permission allowing app
                http = httplib2.Http()
                http = credentials.authorize(http)
                self.service = build('analytics', 'v3', http=http)
                attempt_1=100
    
            except Exception as e_connection:
                attempt_1+=1            
                self.logger.info('Exception is: '+str(e_connection))
                self.logger.info('Attempt number '+str(attempt_1))     
                time.sleep(7)
                pass
        
        #Log success in logging in and put start and end dates in
        self.logger.info('Authentication successful')   

        
    def insert_unsampled(self,title,dimensions,metrics,table_id,start_date_iso,end_date_iso,filters='ga:source=~.*',segment='gaid::-1'):
        
        self.dimensions_list=dimensions.split(',')
        self.metrics_list=metrics.split(',')
        self.title=title
        self.start_date_iso=start_date_iso
        self.dimensions=dimensions
        self.metrics=metrics
        self.filters=filters
        self.table_id=table_id[3:]
        self.end_date_iso=end_date_iso     
        self.segment=segment
        self.reports=0
        
        self.columns_list=[]        
        for itr_1 in itertools.chain(self.dimensions_list,self.metrics_list):
            self.columns_list.append(itr_1)
        
        self.logger.info('The start date of report is set to '+self.start_date_iso)        
        self.logger.info('The end date of report is set to '+self.end_date_iso)

        
        id_row=self.df_id_mapping[self.df_id_mapping['table_id']=='ga:'+self.table_id]
        self.account_id=id_row['account_id'].values[0]
        self.property_id=id_row['property_id'].values[0]
        
        try:

            body_dict={
                  'title': self.title,
                  'start-date': self.start_date_iso,
                  'end-date': self.end_date_iso,
                  'metrics': self.metrics,
                  'dimensions': self.dimensions,
                  'filters': self.filters,
                  'segment': self.segment}
        
            self.reports = self.service.management().unsampledReports().insert(
                accountId=self.account_id,
                webPropertyId=self.property_id,
                profileId=self.table_id,
                body=body_dict
                ).execute()
            
            
        except TypeError, error:
                # Handle errors in constructing a query.
            print 'There was an error in constructing your query : %s' % error
        
        except HttpError, error:
                # Handle API errors.
            print ('There was an API error : %s : %s' %
                (error.resp.status, error.resp.reason)) 
        
        #return self.reports
    
   
        try:
            #Save report ids for retrieval
            self.report_dict={'ids':self.reports['id'],'name':self.reports['title'],
              'property_id':self.reports['webPropertyId'],'account_id':self.reports['accountId'],'table_id':self.reports['profileId']}
            self.df_report_id=pd.DataFrame(data=self.report_dict,index=[0])
            return self.df_report_id
        except:
            return "Error in API call"


    def gdrive_login(self):

        # Authenticate and get a service object
        attempt_1=0
        while attempt_1 < 4:
            try:
            
                # Send in client secret and client ID to the authetication server. Need to set this up in google developer console to get the client secrets and ID
                # Then need to also activate google analytics in the allowed applications
                flow = OAuth2WebServerFlow(
                    self.client_id,
                    self.client_secret,
                    'https://www.googleapis.com/auth/drive')
                
                # Stores the credentials in credentials.dat (i think)
                storage = Storage('credentials_gdrive.dat')
                credentials = storage.get()
                if credentials is None or credentials.invalid:
                    credentials = run(flow, storage)
                
                # Use the credentials to get authentication?
                # Finally if this is the first time, your browser should pop to ask for login and permission allowing app
                http = httplib2.Http()
                http = credentials.authorize(http)
                self.service_gd = build('drive', 'v2', http=http)
                attempt_1=100
    
            except Exception as e_connection:
                attempt_1+=1            
                self.logger.info('Exception is: '+str(e_connection))
                self.logger.info('Attempt number '+str(attempt_1))     
                time.sleep(7)
                pass
        
        #Log success in logging in and put start and end dates in



    def get_unsampled(self,sr_gd_dl):

        try:
            self.unsampled_report = self.service.management().unsampledReports().get(
              accountId=sr_gd_dl['account_id'],
              webPropertyId=sr_gd_dl['property_id'],
              profileId=sr_gd_dl['table_id'],
              unsampledReportId=sr_gd_dl['ids']
              ).execute()
        
        except TypeError, error:
            # Handle errors in constructing a query.
            print 'There was an error in constructing your query : %s' % error
        
        except HttpError, error:
            # Handle API errors.
            print ('There was an API error : %s : %s' %
                 (error.resp.status, error.resp.reason))
                 
        return self.unsampled_report['status']


        #self.logger.info('Gdrive authentication successful')


    def get_details(self):
        self.file_id=self.unsampled_report['driveDownloadDetails']['documentId']
        try:
            self.details_gd = self.service_gd.files().get(fileId=self.file_id).execute()
            return self.details_gd
            
        except HttpError, error:
            print 'An error occurred: %s' % error
        
        
    def download_file(self,file_name):

        download_url = self.details_gd.get('downloadUrl')
        if download_url:
            self.resp, self.content = self.service_gd._http.request(download_url)
            if self.resp.status == 200:
                try:
                    print('done for '+download_url)
                    rr=open(os.path.join(self.path_name,file_name),'wb')
                    rr.write(self.content)
                    rr.close()                
                    return "File written"
                except:
                    return "Fail in writing file"
            else:
                print 'An error occurred: %s' % self.resp
                return None
        else:
          # The file doesn't have any content stored on Drive.
          return None
          
    def download_gdocs_xlsx(self,output_filename='output.xlsx'):
        download_url = self.details_gd['exportLinks']['application/ vnd.openxmlformats-officedocument.spreadsheetml.sheet']

        if download_url:
            self.resp, self.content = self.service_gd._http.request(download_url)
            if self.resp.status == 200:
                print('done for '+download_url)
                a=open(output_filename,'wb')
                a.write(self.content)
                a.close()
                return 1
            else:
                print 'An error occurred: %s' % self.resp
                return 0
        else:
          # The file doesn't have any content stored on Drive.
          return None

    def get_data(self,table_id):
        
        self.table_id=table_id
         
        # Maps the table_id to the country OR device
        country_mapping={'ga:57882851':'MY','ga:57748423':'PH','ga:57130184':'ID','ga:83574663':'SG','ga:57661230':'VN','ga:57754019':'TH',
                           'ga:76788987':'MY','ga:76793115':'PH','ga:76792911':'ID','ga:87173936':'SG','ga:76793015':'VN','ga:76792218':'TH',
                         'ga:71134748':'MY','ga:71133279':'PH','ga:71140910':'ID','ga:87175539':'SG','ga:71132679':'VN','ga:71137422':'TH'  }
        device_mapping={'ga:76788987':'iOS','ga:76793115':'iOS','ga:76792911':'iOS','ga:87173936':'iOS','ga:76793015':'iOS','ga:76792218':'iOS',
                             'ga:71134748':'Android','ga:71133279':'Android','ga:71140910':'Android','ga:87175539':'Android','ga:71132679':'Android','ga:71137422':'Android'  }    
        
        # Set up the API query with the right data
        api_query = self.service.data().ga().get(
            ids=self.table_id,
            start_date=self.start_date_iso,
            end_date=self.end_date_iso,
            metrics=self.metrics,
            dimensions=self.dimensions,
            start_index=1,
            max_results='10000',
            filters=self.filters,
            segment=self.segment,
            samplingLevel='HIGHER_PRECISION'

        )
        
        # Execute query
        self.result = api_query.execute()
        sampled_check=self.result.get('containsSampledData')

        
        #Put data into a data frame          
        self.df_1=pd.DataFrame(self.result.get('rows'),columns=self.columns_list)
        
        country=self.df_id_mapping[self.df_id_mapping['table_id']==self.table_id].loc[:,'Country'].iloc[0]
        
        device=self.df_id_mapping[self.df_id_mapping['table_id']==self.table_id].loc[:,'Platform'].iloc[0]
        self.df_1['Country']=country
        self.df_1['Sampling level']=sampled_check
        
        if device_mapping.get(table_id) == None:
            self.df_1['Platform']='web'
        else:
            self.df_1['Platform']=device
        
        if self.segment <> 'gaid::-1':
            self.df_1['Segment']=self.segment
        else:
            self.df_1['Segment']='All'
            
        if self.filters <> 'ga:source=~.*':
            self.df_1['Filters']=self.filters
        else:
            self.df_1['Filters']='No filter' 
        #output the number of entries to cross check if something is up with the data
        no_of_entries_pulled=self.df_1.iloc[:,2].count()
        return self.df_1,country,no_of_entries_pulled,sampled_check


    def get_data_df(self,dimensions,metrics,table_id_list,start_date_iso,end_date_iso,filters='ga:source=~.*',segment='gaid::-1'):
        
        self.dimensions_list=dimensions.split(',')
        self.metrics_list=metrics.split(',')
        
        self.start_date_iso=start_date_iso
        self.dimensions=dimensions
        self.metrics=metrics
        self.filters=filters
        self.table_id_list=table_id_list
        self.end_date_iso=end_date_iso     
        self.segment=segment
        
        
        self.columns_list=[]        
        for itr_1 in itertools.chain(self.dimensions_list,self.metrics_list):
            self.columns_list.append(itr_1)
        
        self.logger.info('The start date of data pulled is '+self.start_date_iso)        
        self.logger.info('The end date of data pulled is '+self.end_date_iso)
       
    
        header_count=0
        for table_id in self.table_id_list:
            attempt_2=0
            #Add in a loop to try again if data source unavailable            
            while attempt_2 < 4:
                try:
                    df_2,country,no_of_entries,self.sampled_check=self.get_data(table_id)
                    #if device==None:
                    #    device="DT"
                    if header_count==0:
                        #df_2.to_csv(path_name+"\\"+file_name,index=False,mode='a',header=True)
                        self.df_out=deepcopy(df_2)
                    else:
                        #df_2.to_csv(path_name+"\\"+file_name,index=False,mode='a',header=False)
                        self.df_out=self.df_out.append(df_2)
                    self.logger.info('Done for '+country+' with number of entries: '+str(no_of_entries))
                    if self.sampled_check==1:
                        self.logger.info('Data is sampled!! WARNING')
                    else:
                        self.logger.info('Data is unsampled')
                    #if no_of_entries > 1.15*usual_num_entries[country+"_"+device] or no_of_entries < 0.85*usual_num_entries[country+"_"+device]:
                    #    logger.error('Number of values pulled '+str(no_of_entries)+' deviates more than 15% from the usual rows pulled which is set as '+str(usual_num_entries[country+"_"+device]))
                    attempt_2=100
                                    
                except Exception, GA_extraction_error:
                    self.logger.info(GA_extraction_error)
                    attempt_2+=1
                    self.logger.error('24hr Attempt number '+str(attempt_2))
                    time.sleep(7)
                    pass
                 
            header_count+=1       
        
        return self.df_out
    
#Run if main
if __name__=='__main__':
    
    # Set variables
    file_name="avg_basket.csv"
    start_date_iso='2015-01-01'
    end_date_iso='2015-01-03'
    
    # Set GA dimensions and metrics for the report to pull
    dimensions = 'ga:day,ga:channelGrouping,ga:sourceMedium'
    metrics = 'ga:revenuePerTransaction'    
    #filters='ga:channelGrouping==CPC Media'


    table_id_list=['ga:57882851','ga:71134748']
                   
    #main(file_name,dimensions,metrics,filters,table_id_list,start_date_iso,end_date_iso)
    