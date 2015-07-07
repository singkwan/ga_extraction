# Key features!
- Allows for unsampled report extraction workflow that utlizes both GA and gdrive APIs
- Also allows for normal data extraction with API and lets you know if its sampled
- Centralized config file that stores the credentials information (client secret and Key) in a central file
- Accesses a mapping table that stores account, property and table id in a single file, so that all you need is to give a table ID and data can be extracted
- Heavy use of pandas especially as outputs - data output is dataframes which makes it easy to export into CSV or process

#Example usage

```python

import pandas as pd
import sys
sys.path.append('C:\Users\Lazada\Google Drive\Analytics team\SK playground\modules\ga_extraction')
import ga_extraction
import time

# Set start and end dates to run the extraction
start_date_iso='2015-06-01'
end_date_iso='2015-06-01'

# set which type of extraction is needed - 1: unsampled report 0: Core reporting API standard that could be sampled 
unsampled=0

# Initiating the class and authentication for GA
x2=ga_extraction.ga_extractor()  


if unsampled==1:  # For unsampled
    for index,rows in x2.df_mm.iterrows():
        x2.insert_unsampled(rows,start_date_iso,end_date_iso) # Insert unsampled report for each row of the dataframe
    
    # Login to gdrive
    x2.gdrive_login()
    
    # Get details of unsampled report status - it overwrites the file_tracker.csv file with the status and file_id
    x2.get_unsampled()
    while (x2.df_check.status=='pending').sum() >0:
        time.sleep(60*5) # Sleep to pause and check every x minutes
        x2.get_unsampled()
    
    # download the files into the specific folder     
    x2.download_files(".\\data")

elif unsampled==0:  # For normal core reporting API extraction
    
    df_out=pd.DataFrame()
    for index,rows in x2.df_mm.iterrows():
        df_out=df_out.append(x2.get_data_df(rows,start_date_iso,end_date_iso))

```

# How it works in words
###ga_extraction
- Extract data from google analytics both with normal API or by firing unsampled data request.
- Heavily uses pandas for processing and output for easy data processing

###Normal data extraction (ga_data_df)
- It takes table id's as a list and concantenates it into a single data frame
- It also adds a country column in the data frame based on an input table that is specific to my use (we access GA accounts from 6 countries)
  $ This is potentially useful as you could rename the country column in the id_mapping table and replace it with another variable that defines different acccounts
- Output is a pandas data frame

###Insert unsampled report (insert_unsampled)
- Inserts an unsampled data request into GA and then it saves the names of the reports
- Additional function checks to see if the file processing is ready [get_unsampled()] 
- Once it is ready you can just extract the data by running get_details() and then download_file() [set the filename] that uses the gdrive API!
