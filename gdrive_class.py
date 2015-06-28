# -*- coding: utf-8 -*-
"""
Created on Mon Feb 02 13:32:25 2015

@author: singkwan
"""

import httplib2
import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')
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
import shutil
import logging
from copy import deepcopy

class gdrive(object):
    
    def __init__(self,config_file):
        #self.file_name=file_name
        self.config_file=config_file               
        config_data = {}
        text=open(config_file,"r")
        a=[]
        for line in text:
            a= line.split(';') 
            config_data[a[0]] = a[1].strip()
        text.close()
        self.config_data=config_data
        
        #Get data from config file, which includes path name, client ID and secret
        self.path_name=self.config_data['path_name']
        self.client_id=self.config_data['client_id']
        self.client_secret=self.config_data['client_secret']
        
        #Setup the logger to log the issues
        self.logger = logging.getLogger(self.config_file.split('.')[0]+"_logger")
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(self.path_name+"\\"+self.config_file.split('.')[0]+"_logger"+'.log')
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
                self.service = build('drive', 'v2', http=http)
                attempt_1=100
    
            except Exception as e_connection:
                attempt_1+=1            
                self.logger.info('Exception is: '+str(e_connection))
                self.logger.info('Attempt number '+str(attempt_1))     
                time.sleep(7)
                pass
        
        #Log success in logging in and put start and end dates in

        self.logger.info('Authentication successful')
        
    def get_details(self,file_id):
        self.file_id=file_id
        try:
            self.details = self.service.files().get(fileId=self.file_id).execute()
            return self.details
            
        except HttpError, error:
            print 'An error occurred: %s' % error
        
        
        
    def download_file(self):

        download_url = self.details.get('downloadUrl')
        if download_url:
            self.resp, self.content = self.service._http.request(download_url)
            if self.resp.status == 200:
                print('done for '+download_url)
                return self.content
            else:
                print 'An error occurred: %s' % self.resp
                return None
        else:
          # The file doesn't have any content stored on Drive.
          return None
          
    def download_gdocs_xlsx(self,output_filename='output.xlsx'):
        download_url = self.details['exportLinks']['application/ vnd.openxmlformats-officedocument.spreadsheetml.sheet']

        if download_url:
            self.resp, self.content = self.service._http.request(download_url)
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