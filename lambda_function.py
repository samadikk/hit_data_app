from __future__ import print_function
from datetime import datetime
import numpy as np
import pandas as pd
import csv as csv
import urllib.parse as urlparse
import urllib as urllib
import boto3 as boto3
import io as io

"""
Pre compiled Pandas and Numpy python dependencies are added using Lambda Layers functionality.
"""

s3 = boto3.client('s3')    #low level s3 api
s3r = boto3.resource('s3') # high level s3 api

"""
class PrepData is used to clean and format hit level data using python packages.
"""

class PrepData:
    
    def __init__(self):
        now = datetime.now()
        self.filename_stamp = now.strftime("%Y-%m-%d")

    def get_medium(self,mlist): 
        """
        Function get_medium() accepts 'referrer' column from the original dataset 
            and extracts the search domain using urllib.urlparse method.
            If the referrer does not contain either of Google, MSN or Yahoo
            the default return value is None 
        """

        med = np.nan
        if not pd.isnull(mlist): 
            medium = urlparse.urlsplit(mlist).netloc 
            if 'google' in medium: 
                med = 'google.com'
            elif 'bing' in medium: 
                med = 'msn.com' 
            elif 'yahoo' in medium: 
                med = 'yahoo.com'
        return med

    def get_keyword(self,mlist):
        """
        Function get_keyword() accepts 'referrer' column from the original dataset 
            and extracts the search keywords using urllib.urlparse.parse_qs method.
            The return value of parse_qs method is a dictionary (K,V) of query part of
            the URL. Follwing are the scenarios to extract query keywords as per Domain:
                a) For Google/Bing , search keyword has key = q
                b) For Yahoo, search keyword has key = p
            If the referrer does not contain either of Google, MSN or Yahoo
            the default return value is None. 
        """

        keyword = np.nan
        if not pd.isnull(mlist): 
            arr = urlparse.urlsplit(mlist).netloc 
            query_dict = urlparse.parse_qs(urlparse.urlparse(mlist).query) 
            if 'google' in arr: 
                keyword = query_dict.get('q') 
                keyword = ''.join(keyword)
            elif 'bing' in arr: 
                keyword = query_dict.get('q')
                keyword = ''.join(keyword)
            elif 'yahoo' in arr: 
                keyword = query_dict.get('p')
                keyword = ''.join(keyword)
        return keyword
    
    def get_revenue(self,event,products): 
        """
            Function get_revenue() accepts 'product_list (products)' and 'event_list(event)' 
                row value from the original dataset and extracts the revenue 
                based on purchase event = 1. It uses inbuilt python split function.
                Extracted Revenue part is then casted to Float. 
                Function also checks for multiple products in a given row and aggregates revenue based on it.
            If the product_list is blank or event_list <> 1 then revenue is 0.
            the default revenue return value is 0 
        """        

        rev = 0.0     
        if event == 1.0 and products != '':
            nproduct = products.split(',')
            if len(nproduct) == 1:
                arr = products.split(';')
                if arr[3] == '' or arr[3] is None: 
                    arr[3] = '0.0'
                rev = float(arr[3]) 
            else:
                for i in nproduct:
                    arr = ''
                    arr = i.split(';')
                    if arr[3] == '' or arr[3] is None: 
                        arr[3] = '0.0'
                    rev = rev + float(arr[3])
                
        return rev

    def writeDatatoS3(self, data_result,bucket):
        """
            Function writeDatatoS3() accepts result of pandas group by which is an aggregated  
                dataset and s3 bucket name. 
                File is first written to local lambda server directory (/tmp/) then it is uploaded to s3.
            If the incoming data_result argument is None, then an error message is printed.
        """        
        
        if data_result is not None:
            file_name = '/tmp/'+ self.filename_stamp + '_SearchKeywordPerformance' + '.tab'
            s3_name = 'result/' + self.filename_stamp + '_SearchKeywordPerformance' + '.tab'
            data_result.to_csv(file_name,
                               sep='\t',
                               encoding='utf-8',
                               index=False)
                               
            wrbucket = s3r.Bucket(bucket)
            wrbucket.upload_file(file_name, s3_name)
            print("Result File uploaded to s3")
        else:
            print("Unable write and upload results to S3, aggregated data is blank")

            
    def process_data(self,data,bucket):
        """
            Function process_data() accepts raw data and s3 bucket as arguments.   
            Follwing are the sequence of steps:
                a) Using pandas series, Search Engine Domain/Keyword is extracted from 'referrer' column.
                b) fillna() method forward and back fills all the NaN grouped by IP, user_agent, city, region and country.
                   This step is to ensure medium column always get the value of first referrer.
                   Keywords are not case sensitive, so all keywords are converted to lower case.
                c) Revenue attribute is then extracted from 'product_list' from original dataset based on event_list value.
                d) Finally results are aggregated by new columns 'Medium' and 'Keyword'
                e) The header row of the aggregated dataset (data_result) is renamed as per guidelines 
                    and data is written to s3 bucket which generated the event.
            If the incoming data argument is None, then an error message is printed.
            Return values:
                a) 0 if file is uploaded to s3.
                b) -1 if file is not upload to s3.
        """ 
        
        if data is not None:
            data['medium'] = data['referrer'].apply(self.get_medium)
            data['medium'] = data.groupby(['ip', 'user_agent','geo_city','geo_region','geo_country'])['medium'].fillna(method='ffill')
            data['medium'] = data.groupby(['ip', 'user_agent','geo_city','geo_region','geo_country'])['medium'].fillna(method='bfill')
            
            data['keyword'] = data['referrer'].apply(self.get_keyword)
            data['keyword'] = data['keyword'].str.lower()
            data['keyword'] = data.groupby(['ip', 'user_agent','geo_city','geo_region','geo_country'])['keyword'].fillna(method='ffill')
            data['keyword'] = data.groupby(['ip', 'user_agent','geo_city','geo_region','geo_country'])['keyword'].fillna(method='bfill')
            
            data['revenue'] = data.apply(lambda x: self.get_revenue(x['event_list'],x['product_list']), axis=1)
            
            data_result = data.groupby(['medium','keyword'], as_index=False)['revenue'].sum()

            data_result.sort_values(by='revenue')

            print("Completed generating aggregated results for original dataset")
            
            #Renaming Columns in Dataframe
            data_result.rename(columns={'medium':'Search Engine Domain', 'keyword':'Search Keyword','revenue':'Revenue'},inplace=True)
            
            self.writeDatatoS3(data_result,bucket)
        else:
            print("Raw data is null, please check pandas dataframe results")
            return -1
            
        return 0
        
        
def lambda_handler(event, context):    
    """
        Function lambda_handler() is the entry point of the lambda function.
        It extracts bucket and key value through the s3 event. If bucket/key is incorrect it raises an exception.
        'prep' is the instance of Class PrepData. 
        Using process_data() method data is cleaned and transformed.
        Return values:
            a) 0 if process is completed successfully.
            b) -1 if process is not completed successfully.
    """
    retval = 0
    
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

        print ("Bucket: {}, Key: {}".format(bucket,key))
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = pd.read_csv(io.BytesIO(obj['Body'].read()), 
                            encoding='utf8',
                            sep='\t', 
                            lineterminator='\r')
        
        prep = PrepData()
        retval = prep.process_data(data,bucket)
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        retval = -1
        raise e
        
    return retval
