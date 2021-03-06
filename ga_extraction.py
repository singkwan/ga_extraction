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

    
    def __init__(self,config_file="C:\Users\Lazada\Google Drive\Analytics team\SK playground\credentials\\config.txt",id_mapping_file="C:\Users\Lazada\Google Drive\Analytics team\SK playground\credentials\\id_mapping.csv",map_master_file='map_master.xlsx',file_tracker='file_tracker.csv'):       
        """ initialize the class. requires:
        1. Config txt file that has the API key to access the GA accounts [config.txt]
        2. id_mapping csv file that maps all the accounts to its respective table ids and country [id_mapping.csv]
        3. Map master excel file that has the settings you want to extract [map_master.xlsx]
        4. File tracker csv that it will write the output to check for gdrive download statuses - default is set to file_tracker.csv"""

        self.file_tracker=file_tracker
                
        
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


        self.df_id_mapping=pd.read_csv(id_mapping_file)     
        
        self.df_map_master=pd.read_excel(map_master_file, sheetname='Main') 
               
        self.df_map_ga=pd.read_excel(os.path.join(self.path_name,map_master_file),sheetname='Main')

        self.df_map_ga=self.df_map_ga.reset_index(drop=True)
        self.df_map_ga['map_fk']=self.df_map_ga.loc[:,'country']+'_'+self.df_map_ga.loc[:,'platform']
        
        self.df_map_ga=self.df_map_ga.merge(self.df_id_mapping,how='left',left_on='map_fk',right_on='name')
        
        self.df_map_ga=self.df_map_ga.drop(['Platform'],axis=1)
#        self.df_map_ga=self.df_map_ga.rename(columns={'Platform_x':'Platform'})
        
        self.df_mm=self.df_map_ga.drop(['Country'],axis=1)
#            self.df_mm=self.df_map_ga.rename(columns={'Country':'Country'})
        

       
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
                print ('Exception is: '+str(e_connection)+'\n'+'Attempt number '+str(attempt_1))
                time.sleep(7)
                pass
        
        #Log success in logging in and put start and end dates in
        self.logger.info('Authentication successful')   
    
        
    def insert_unsampled(self,df_rows,start_date_iso,end_date_iso):
        """ Inserting the unsampled file into GA
        
        Parameters
        ---------------------------------------
        df_rows: This should be ideally rows from the map_master file. Pandas Series object with the following columns
                dimensions = e.g. ga:channelGrouping
                metrics = e.g. ga:sessions
                country = Country of account [this can be replaced with another variable]. Takes a string
                filter = filter to use. For default use ga:source=~.* which is no filter
                segment = Either the segment ID, or segment using REGEX. For no segment use gaid::-1
                platform = options are and,ios,web [specific to our use]
                report_name = Name of report to use - string
        start_date_iso: Format should be - '2015-07-04'
        end_date_iso: Format should be '2015-07-04'
        """

        self.df_rows=df_rows
        # Data from the series
        self.dimensions_list=df_rows['dimensions'].split(',')
        self.dimensions=df_rows['dimensions']
        self.metrics_list=df_rows['metrics'].split(',')
        self.metrics=df_rows['metrics']
        self.table_id=self.df_mm[(self.df_mm['country']==df_rows['country']) & (self.df_mm['platform']==df_rows['platform'])]['table_id'].iloc[0][3:]
        self.filters=df_rows['filter']
        self.segment=df_rows['segment']

        self.title=df_rows['country']+'_'+df_rows['platform']+'_'+df_rows['report_name']+'_'+start_date_iso+'__to__'+end_date_iso
        self.start_date_iso=start_date_iso        
        self.end_date_iso=end_date_iso     

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
            try:            
                a=pd.read_csv(os.path.join(self.path_name,self.file_tracker))
                a=a.append(self.df_report_id)
                a.to_csv(os.path.join(self.path_name,self.file_tracker),mode='w',index=0)
            except:    
                self.df_report_id.to_csv(os.path.join(self.path_name,self.file_tracker),mode='w',index=0)
                print('Error saving ids report')
            return self.df_report_id


        except:
            return "Error in API call"
            print('Error in API call')


    def gdrive_login(self):
        """ Logins to the gdrive using the your credentials - no input needed"""
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
                print ('Exception is: '+str(e_connection)+'\n'+'Attempt number '+str(attempt_1))
        
        #Log success in logging in and put start and end dates in



    def get_unsampled(self):
        """ Gets the status and file_ids of unsampled reports- DOES NOT actually get the file yet,
        its checking the status and returning the information required to download in from gdrive later

        How it works
        ---------------
        It writes the status of each file into the file_tracker.csv(unless you changed the name of the file)
        It writes the column status (done to show its done), as well as file_ids to use in the download_file function
        """
        self.df_unsampled=pd.read_csv(os.path.join(self.path_name,self.file_tracker))
        
        self.df_check=pd.DataFrame()
        for index_2, sr_gd_dl in self.df_unsampled.iterrows():
                   
            try:
                self.unsampled_report = self.service.management().unsampledReports().get(
                  accountId=sr_gd_dl['account_id'],
                  webPropertyId=sr_gd_dl['property_id'],
                  profileId=sr_gd_dl['table_id'],
                  unsampledReportId=sr_gd_dl['ids']
                  ).execute()
                  
                if self.unsampled_report['status']=='COMPLETED':
                     sr_gd_dl['status']='completed'
                     sr_gd_dl['file_id']=self.unsampled_report['driveDownloadDetails']['documentId']
                elif self.unsampled_report['status']=='PENDING':
                     sr_gd_dl['status']='pending'
                     sr_gd_dl['file_id']='pending'
                
                self.df_check=pd.concat([self.df_check,sr_gd_dl],axis=1)
                                     
                  
            except TypeError, error:
                # Handle errors in constructing a query.
                print 'There was an error in constructing your query : %s' % error
            
            except HttpError, error:
                # Handle API errors.
                print ('There was an API error : %s : %s' %
                     (error.resp.status, error.resp.reason))

        self.df_check=self.df_check.transpose()
        self.df_check.to_csv(os.path.join(self.path_name,self.file_tracker),index=0,mode='w')
        
        #self.logger.info('Gdrive authentication successful')
        
    def download_files(self,output_path=0):
        """Uses the file_tracker.csv information and downloads the files
        Do not run this until all files in the set have status - done!"""
        
        if output_path==0:
            self.output_path=self.path_name
        else:
            self.output_path=output_path
        
        self.df_unsampled=pd.read_csv(os.path.join(self.path_name,self.file_tracker))
        
        for index, rows in self.df_unsampled.iterrows():
            self.file_id=rows['file_id']
            print('running at index '+str(index))
          
            try:
                self.details_gd = self.service_gd.files().get(fileId=self.file_id).execute()
                
            except HttpError, error:
                print 'An error occurred: %s' % error
    
    
            self.download_url = self.details_gd['downloadUrl']
            if self.download_url:
                
                self.resp, self.content = self.service_gd._http.request(self.download_url)
                if self.resp.status == 200:
                    try:
                        print('done for '+self.download_url)
                        rr=open(os.path.join(self.output_path,rows['name']+'.csv'),'wb')
                        rr.write(self.content)
                        rr.close()                
                        
                    except:
                        print('something wrong --- 1')                        
                        return "Fail in writing file"
                else:
                    print 'An error occurred: %s' % self.resp
                    return None
            else:
              # The file doesn't have any content stored on Drive.
                print('something wrong')                
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
            print('No content in Drive')
            return None

    def get_data(self,start_index=1):
        
        # Set up the API query with the right data
        api_query = self.service.data().ga().get(
            ids=self.table_id_normal,
            start_date=self.start_date_iso,
            end_date=self.end_date_iso,
            metrics=self.metrics,
            dimensions=self.dimensions,
            start_index=start_index,
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
        
        country=self.df_id_mapping[self.df_id_mapping['table_id']==self.table_id_normal].loc[:,'Country'].iloc[0]
        
        device=self.df_id_mapping[self.df_id_mapping['table_id']==self.table_id_normal].loc[:,'Platform'].iloc[0]
        self.df_1['Country']=country
        self.df_1['Sampling level']=sampled_check
        
        if device == None:
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


    def get_data_df(self,df_rows,start_date_iso,end_date_iso):
        """ Get data using the core reporting API - this is the easiest way and ideal way to get this if dataset is not large/dimensions are not many so data is still unsampled
        
        Parameters
        ---------------------------------------
        df_rows: This should be ideally rows from the map_master file. Pandas Series object with the following columns
                dimensions = e.g. ga:channelGrouping
                metrics = e.g. ga:sessions
                country = Country of account [this can be replaced with another variable]. Takes a string
                filter = filter to use. For default use ga:source=~.* which is no filter
                segment = Either the segment ID, or segment using REGEX. For no segment use gaid::-1
                platform = options are and,ios,web [specific to our use]

        start_date_iso: Format should be - '2015-07-04'
        end_date_iso: Format should be '2015-07-04'
        """
        
        # Data from the series
        self.dimensions_list=df_rows['dimensions'].split(',')
        self.dimensions=df_rows['dimensions']
        self.metrics_list=df_rows['metrics'].split(',')
        self.metrics=df_rows['metrics']
        self.table_id_normal=self.df_mm[(self.df_mm['country']==df_rows['country']) & (self.df_mm['platform']==df_rows['platform'])]['table_id'].iloc[0]
        self.filters=df_rows['filter']
        self.segment=df_rows['segment']
        
        self.start_date_iso=start_date_iso
        self.end_date_iso=end_date_iso     
        
        self.columns_list=[]        
        for itr_1 in itertools.chain(self.dimensions_list,self.metrics_list):
            self.columns_list.append(itr_1)
        
        self.logger.info('The start date of data pulled is '+self.start_date_iso)        
        self.logger.info('The end date of data pulled is '+self.end_date_iso)
       
        self.df_out=pd.DataFrame()
        run_condition=1
        attempt_2=0
        index_counter=1
        #Add in a loop to try again if data source unavailable
        while run_condition==1:            
            while attempt_2 < 3:
                try:
                    df_2,country,no_of_entries,self.sampled_check=self.get_data(start_index=index_counter)
    
                    self.df_out=self.df_out.append(df_2)
                    self.logger.info('Done for '+country+' with number of entries: '+str(no_of_entries))
                    if self.sampled_check==1:
                        self.logger.info('Data is sampled!! WARNING')
                    else:
                        self.logger.info('Data is unsampled')
                    #if no_of_entries > 1.15*usual_num_entries[country+"_"+device] or no_of_entries < 0.85*usual_num_entries[country+"_"+device]:
                    #    logger.error('Number of values pulled '+str(no_of_entries)+' deviates more than 15% from the usual rows pulled which is set as '+str(usual_num_entries[country+"_"+device]))
                    
                    if no_of_entries==10000:
                        index_counter+=10000
                    else:
                        run_condition=0
                        attempt_2=100
                        
                except Exception, GA_extraction_error:
                    self.logger.info(GA_extraction_error)
                    attempt_2+=1
                    self.logger.error('24hr Attempt number '+str(attempt_2))
                    time.sleep(7)
                    pass
                    print ('24hr Attempt number '+str(attempt_2))
        
        return self.df_out
    
    def merge_output(self, start_date_iso):
        path = "./data"
        dirs = os.listdir(path)
        files = []
        df = pd.DataFrame()
        
        for i in dirs:
            files.append("data/" + i)
            
        for fname in files:
            print fname
            data = pd.read_csv(fname, skiprows=7)
            data["country"]=fname[5:7]
            iindex = fname[12:].index("_") + 12
            data["category"]=fname[12:iindex]           
            df = df.append(data)

        df["Currency"] = ""
        df["Revenue (USD)"] = ""            
        df["ga:date"] = pd.to_datetime(df["ga:date"], format="%Y%m%d")
        df = df.rename(columns = {"ga:date":"Date Range", "ga:transactionId":"Transaction ID", "ga:transactions":"Transactions", "ga:channelGrouping":"Channel Grouping", "category":"Category Page", "country":"Country"})

        df = df[["Date Range", "Channel Grouping", "Transaction ID", "Transactions", "Country", "Category Page", "Currency", "Revenue (USD)"]]

        df.to_csv("GA daily extracts by transaction id/Daily_report_"+start_date_iso +".csv", index = False, date_format = "%d/%m/%Y")
        
        return df
