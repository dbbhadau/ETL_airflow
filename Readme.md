## Project Summary
General Payment Data 2013 Across America and U.S. City Demographic datasets are used to create an ETL pipeline which can be used for querying and analysing General Payment Data 2013 trends based on the U.S. city demography.

The project follows the follow steps:
* Step 1: Project Scope and Gathering of Data
* Step 2: Explore and Assess the Data
* Step 3: Data Model Defination
* Step 4: ETL Pipelines to Model the Data
* Step 5: Project Write Up


## Step 1: Project Scope and Gathering of Data

#### Scope 
The main dataset includes General Payment 3013 Data (approx 4 million records) and supplementary dataset includes data on U.S. city demographics.
The project includes 2 dimension tables and 1 fact table. 
General Payment Data 2013  and U.S. city demographic data would be the two dimension tables. Both these tables are joined on  State to form fact table.
A joined Table will be created to query on General Payment Data 2013 trends.

#### Data Description 

General Payment 2013 Data comes from the [openpaymentsdata](https://openpaymentsdata.cms.gov/dataset/General-Payment-Data-Detailed-Dataset-2013-Reporti/gtwa-6ahd). 
It is provided in tsv format which we convert to json files based on Recipient_State partition for easy storage.
Sometimes, Physicians have financial relationships with health care manufacturing companies. These relationships can be include money for research activities, gifts, speaking fees, meals, or travel. The Affordable Care Act requires CMS to collect information from applicable manufacturers and group purchasing organizations (GPOs) in order to report information about their financial relationships with physicians and hospitals.

The U.S. City Demographic data comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
It stores demographics data for US cities.Format is csv.

## Step 2: Explore and Assess the Data
### Exploring the Data 

<b>General Payment 2013 Data</b> - drop all data points where values are NaN,convert tsv to json format and then store the cleaned dataset to s3 using boto3. Path for storing General Payment 2013 Data ***s3://ganpati-hanuman/pay/***

<b>the U.S. City Demographic data</b> - rename columns,drop all data points where values are NaN, drop duplicates,and then store the cleaned dataset to s3 using boto3.Path for storing the U.S. City Demographic data ***s3://ganpati-hanuman/demo/***

## Step 3: Data Model Defination
### 3.1 Conceptual Data Model

#### Tables:
| Table Name | Columns | Description | Type |
| ------- | ---------- | ----------- | ---- |
| General Payment 2013 Data | Record_ID-Physician_Profile_ID-Physician_First_Name-Physician_Specialty-Total_Amount_of_Payment_USDollars-Nature_of_Payment_or_Transfer_of_Value-Recipient_State|stores General Payment 2013 Data of US | dimension table |
| US city demographics | City- State -Median_Age - Male_Population - Female_Population - Total_Population - Number_of_Veterans - Foreign_born - Average_Household_Size - State_Code - Race - Count  | stores demographics data for cities | dimension table |
| General Payment 2013 Data joined with the US city demographics data on state |Record_ID-Physician_Profile_ID-Physician_First_Name-Physician_Specialty-Total_Amount_of_Payment_USDollars-Nature_of_Payment_or_Transfer_of_Value-Recipient_State-City- State -Median_Age - Male_Population - Female_Population - Total_Population - Number_of_Veterans - Foreign_born - Average_Household_Size - State_Code - Race - Count  | stores all immigrations data based on demography| fact table |


#### 3.2 Mapping Out Data Pipelines

Pipeline Steps:
##### Create these python files in Airflow
1. Clean General Payment 2013 Data covert to json files partioned by Recipient_State  then upload cleaned dataframe to s3 using boto3.(data_cleaned/pay/pay_data.py)
2. Clean US dempgraphic data and then upload cleaned dataframe to s3 using boto3.(data_cleaned/demo/America_demo.py)
3. Create General Payment 2013 Data dimension table by selecting relevant columns from df_pay_data(GH_create_tables.sql and operators/create_tables_Operator.py)
4. Create US dempgraphic dimension table by selecting relevant columns from df_dempgraphics_data.(GH_create_tables.sql and operators/create_tables_Operator.py)
5. Create fact table by joining General Payment 2013 Data and US dempgraphic dimension tables on Recipient_State and state_code.(GH_create_tables.sql and operators/create_tables_Operator.py)

## Step 4: ETL Pipelines to Model the Data 

#### 4.1 Data model Creation
Building the data pipelines to create the data model using Airflow , S3 and Redshift.
 
1. After uploading cleaned datasets to S3, create python file Ganpati-Hanuman_capstone.py for dag ETL pipeline
2. Create connections to AWS and Redshift in Airflow UI.
3. Run GH_create_tables.sql queries in Redshift query editor to build schema.
4. Use S3 buckets to store and pull data
4. Write sql queries in Queries.py in helpers to insert data in redshift tables.
5. Write python files of all dependent operators.
6. Write the task order to establish task dependencies.
7. Create table schemas in Redshift
8. Access Airflow UI to trigger ETL dag file Ganpati-Hanuman_capstone.py 
9. Run analytical queries in Redshift query editor.

#### 4.2 Data Quality Checks
Performing data quality checks to ensure the pipeline ran as expected. 
 * We check the Source/Count to ensure completeness.
 
#### 4.3 Data dictionary 

**Fact Table** - General Payment 2013 Data joined with the city Demographic data on US city.
Columns:
   - Record_ID
   - Physician_Profile_ID-Physician_First_Name
   - Physician_Specialty-
   - Recipient_State
   - City = city name
   - state= State name
   - male_population = count of male population
   - female_population = count of female population
   - total_population = count of total population
   - state_code = 2 digit state code
   - count = count of perticular race population
   
    
**Dimension Table** - General Payment 2013 Data
Columns:
   - Record_ID = Record ID of payment transaction
   - Physician_Profile_ID =Profile IDof Physician
   - Physician_First_Name = First Name of Physician
   - Physician_Specialty= The Specialty of Physician
   - Total_Amount_of_Payment_USDollars = Payment amount 
   - Nature_of_Payment_or_Transfer_of_Value = Types of paymennt
   - Recipient_State = Province of Recipient

**Dimension Table** - Demographic data
Columns:
- City = city name,
- state= State name,
- median_age= meadian age of population,
- male_population = count of male population
- female_population = count of female population
- total_population = count of total population
- state_code = 2 digit state code
- race = race
- count = count of perticular race population

## Step 5: Project Write Up
*  Tools and technologies used for the project:
    - The project utilizes Apache Airflow with custom and builtin operators for tasks also airflow can easily build scheduled task order ETL pipeline that contain large amounts of data. 
    - S3 is used to store and pull data from buckets.
    - Finally,  Redshift is used to run analytical queries.
    
    
*   Data Update schedule
    - Since the format of the raw files are monthly, we should pull the data monthly.
    
## Example Queries for Analytics Team :

* Query 1 : List of physician_specialities based on city and total population.

```
-- List of physician_specialities based on city and  total population

select  T.physician_specialty ,  T.city  , max(T.city_count),T.total_population 
from(select p.physician_specialty as physician_specialty, d.city_name as city,count(d.city_name) as city_count,d.total_population as total_population
FROM  pay p JOIN demographic d ON (p.recipient_state=d.state_code)
group by 2,1,4   
order by 3 desc) T
group by 2,1,4

```
### Output files  :

![graph_view Output file](output_images/graph_view.jpg)

![tree_view Output file](output_images/tree_view.jpg)

![redshift_query Output file](output_images/redshift_query.jpg)
    
### What if Scenarios
    1. if the data was increased by 100x.
        - Load data in AWS S3 bucket and use AWS EC2 and EMR jupyter notebook for accessing spark.
        - Use AWS Redshift for its an analytical database that is optimized for aggregation and read-heavy workloads
    2. if data populates a dashboard that must be updated on a daily basis by 7am every day.
        - Airflow used for scheduling purposes should have DAG retries and emails can be sent on failures.
        - Daily quality checks should be scheduled, upon failing send emails and flash notification on dashboards.
    3. if the database needed to be accessed by 100+ people.
        - Spark could be used.
        - AWS Redshift could be used as it has auto-scaling capabilities.
          Also appropriate Security Groups and VPN can be added for safe access by many users.



