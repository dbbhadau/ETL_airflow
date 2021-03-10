from datetime import datetime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.DataQualityOperator import DataQualityOperator
from operators.LoadFactOperator import LoadFactOperator
from operators.LoadDimensionOperator import LoadDimensionOperator
from operators.StagedemoToRedshiftOperator import StagedemoToRedshiftOperator
from operators.StagepayToRedshiftOperator import StagepayToRedshiftOperator
from operators.drop_tables_Operator import drop_tables_Operator
from operators.create_tables_Operator import create_tables_Operator
from helpers import Queries

default_args = {
    'owner': 'Ganpati-Hanuman',
    'start_date': datetime.now(),
    #'start_date': datetime(2019, 1, 12),
    #'end_date': datetime(2019, 1, 31),
    'depends_on_past': False,
    'retries': 3,
    #'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('ETL_pipeline_dag',
          default_args=default_args,
          description='ETL using Redshift and S3 with Airflow',
          #schedule_interval='0 * * * *',
          schedule_interval=None,
          #schedule_interval='@hourly',
          max_active_runs=1,
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

drop_tables = drop_tables_Operator(
    task_id="drop_tables",
    dag=dag,
    redshift_conn_id="redshift",
    sql="GH_drop_tables.sql"
)

create_tables = create_tables_Operator(
    task_id = "create_tables",
    dag = dag,
    sql="GH_create_tables.sql" ,
    redshift_conn_id="redshift"
)

# Load events on staging
cities_staging = StagedemoToRedshiftOperator(
    task_id = 'cities_staging',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_us_city_data',
    s3_bucket="ganpati-hanuman",
    s3_key="demo",
    #delimiter=";"
    delimiter=","

)

pay_staging = StagepayToRedshiftOperator(
    task_id = 'pay_staging',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_pay_code_data',
    s3_bucket="ganpati-hanuman",
    s3_key="pay",
    format_as_json='auto ignorecase'
)


end_staging = DummyOperator(task_id='end_staging', dag=dag)


# Load Dimension table
load_pay_dimension_table = LoadDimensionOperator(
    task_id='load_pay_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="pay",
    query=Queries.pay_table_insert,
    truncate=False
)

# Load Dimension table
load_demographic_dimension_table = LoadDimensionOperator(
    task_id='load_demographic_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="demographic",
    query=Queries.demographic_table_insert,
    truncate=False
)

# Load Fact table
load_fact_table = LoadFactOperator(
    task_id='load_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="fact_table",
    query=Queries.fact_table_insert,
    truncate=False
)


end_loading = DummyOperator(task_id='end_loading',  dag=dag)


run_quality_checks_on_pay_table = DataQualityOperator(
    task_id='run_quality_checks_on_pay_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="pay"
)

run_quality_checks_on_demographic_table = DataQualityOperator(
    task_id='run_quality_checks_on_demographic_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="demographic"
)

run_quality_checks_on_fact_table = DataQualityOperator(
    task_id='run_quality_checks_on_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="fact_table"
)

end_operator = DummyOperator(task_id='end',  dag=dag)

#Task order

start_operator >> drop_tables
drop_tables >> create_tables

create_tables  >> pay_staging
create_tables >> cities_staging


pay_staging >> end_staging
cities_staging >> end_staging


end_staging >> load_fact_table

load_fact_table >> load_pay_dimension_table
load_fact_table >> load_demographic_dimension_table


load_pay_dimension_table >> end_loading
load_demographic_dimension_table >> end_loading


end_loading >> run_quality_checks_on_fact_table
end_loading >> run_quality_checks_on_pay_table
end_loading >> run_quality_checks_on_demographic_table


run_quality_checks_on_fact_table >> end_operator

run_quality_checks_on_demographic_table >> end_operator
run_quality_checks_on_pay_table >> end_operator
