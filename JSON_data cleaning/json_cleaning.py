# import libraries
import pandas as pd
import json
from flatten_json import flatten
import logging
 
logging.basicConfig(filename="logfile.log", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
   
#extracting dataframes
class DataExtract:
       
    def json_cleaning(self,json_file):
        with open(json_file, 'r', encoding='utf-8') as file:
            json_data = json.load(file)
        data_headers = json_data.get('data', [])
        flattened_data = [flatten(data) for data in data_headers]
        flattened_df = pd.DataFrame(flattened_data)
        logging.info("Data cleaning is successfully completed")
        return self.extract(flattened_df)
   
   
    #extracting relevant columns from csv
    def extract(self,flattened_df):
        try:
            extracted_df = flattened_df.loc[:,["parameterValues_DE:Query Type",  "parameterValues_DE:Number of Rows", "entryDate"]].copy()
            extracted_df.rename(columns = {'parameterValues_DE:Query Type':'Query Type', 'parameterValues_DE:Number of Rows':'Rows', 'entryDate':'Entry'}, inplace = True)
            extracted_df["Entry"] = extracted_df["Entry"].str.replace("T", " ")
            extracted_df['Entry'] = extracted_df['Entry'].str.rsplit(":",n=1).str.join(".")
            extracted_df['Entry'] = extracted_df['Entry'].str.replace(r'\+0530$', '', regex=True)  
            logging.info("Relevant columns have been successfully extracted")
        except FileNotFoundError:
            logging.error("File not found error.")
            return None
        except IOError:
            logging.error("Incorrect file name.")
            return None
        finally:
            return self.convert(extracted_df)
   
    #converting json -> csv
    def convert(self,dataframe):
        logging.info("file converted from JSON -> CSV")
        return dataframe.to_csv('result.csv', index=False)
   
 
if __name__ == "main":
    a = DataExtract()
    a.json_cleaning('DA_DRC.json')