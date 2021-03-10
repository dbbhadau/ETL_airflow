import pandas as pd
import numpy as np
from boto3.s3.transfer import S3Transfer
import boto3

def process(input_file, output_file):
    """
    Perform data cleanup 
    :param input_file: path to CSV file
    :param output_folder: path to folder with data cleaned
    Upload cleaned demographic dataframe to s3 using boto3
    
    """
    
    df_demographics_data = pd.read_csv(input_file, header=0,delimiter=";")
  
    # Rename columns
    df_demographics_data.rename(columns={  "City" : "city_name","State":"state","Median Age": "median_age", "Male Population": "male_population", "Female Population": "female_population", "Total Population": "total_population", "Number of Veterans": "number_of_veterans", "Foreign-born": "foreign_born","Average Household Size":"average_household_size","State Code":"state_code",
"Race" : "race" , "Count":"count" }, inplace=True)
      
    # clearing missing values and drop duplicates in U.S. City Demographic Data
    df_demographics_data.dropna(subset=['city_name','state','median_age','male_population','female_population','total_population','number_of_veterans','foreign_born','average_household_size','state_code','race','count'], inplace=True)
    df_demographics_data.drop_duplicates(subset=['city_name','state','median_age','male_population','female_population','total_population','number_of_veterans','foreign_born','average_household_size','state_code','race','count'], inplace=True)

    print(f"size of demographic dataframe '{df_demographics_data.count()}'")

    df_demographics_data.to_csv(output_file + 'demo_clean.csv',index=False)

def main():
    """
    Main method
    """
   
    #input_file = 'C:/Users/user/Desktop/Data_Engineer/UPLOAD_UDACITY/us-cities-demographics.csv'
    #output_file = 'C:/Users/user/Desktop/Ganpati-Hanuman_upload/data_cleaned/demo/'
    input_file = '/home/workspace/airflow/data/us-cities-demographics.csv'
    output_file = '/home/workspace/airflow/data_cleaned/demo/'
    
    process(input_file, output_file)
    
    from boto3.s3.transfer import S3Transfer
    import boto3
    
    # Establish s3 connection using boto
    
    client = boto3.client('s3', aws_access_key_id='enter aws_access_key_id',aws_secret_access_key='enter aws_secret_access_key')
    transfer = S3Transfer(client)
    transfer.upload_file(output_file + 'demo_clean.csv', "ganpati-hanuman", "demo"+"/"+"demo_clean.csv")
    print("uploaded cleaned demographic data to s3")
  
   

if __name__ == "__main__":
    main()
