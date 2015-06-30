# ga_extraction
- Extract data from google analytics both with normal API or by firing unsampled data request.
- Heavily uses pandas for processing and output for easy data processing

# Normal data extraction (ga_data_df)
- It takes table id's as a list and concantenates it into a single data frame
- It also adds a country column in the data frame based on an input table that is specific to my use (we access GA accounts from 6 countries)
  $ This is potentially useful as you could rename the country column in the id_mapping table and replace it with another variable that defines different acccounts
- Output is a pandas data frame

# Insert unsampled report (insert_unsampled)
- Inserts an unsampled data request into GA and then it saves the names of the reports
- Additional function checks to see if the file processing is ready [get_unsampled()] 
- Once it is ready you can just extract the data by running get_details() and then download_file() [set the filename] that uses the gdrive API!

# Key features!
-  Allows for unsampled report extraction workflow that utlizes both GA and gdrive APIs
- Also allows for normal data extraction with API and lets you know if its sampled
- Centralized config file that stores the credentials information (client secret and Key) in a central file
- Accesses a mapping table that stores account, property and table id in a single file, so that all you need is to give a table ID and data can be extracted
- Heavy use of pandas especially as outputs - data output is dataframes which makes it easy to export into CSV or process
