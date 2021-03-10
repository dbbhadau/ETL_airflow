from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
#from airflow.contrib.hooks.aws_hook import AwsHook



class StagedemoToRedshiftOperator(BaseOperator):
    """
    Copies the data from S3 into the staging table
        redshift_conn_id: Redshift connection ID
        aws_credentials_id: AWS connection ID
        table: Name of the staging table
        s3_bucket: Name of the bucket with the JSON data
        s3_key: Path of the csv file within the bucket
        
    """
   # template_fields = ()
    #template_ext = ()
    ui_color = '#358140'
     
    copy_query = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    DELIMITER'{}'
                    IGNOREHEADER 1  
                    EMPTYASNULL
                    BLANKSASNULL;
             """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_credentials_id,
                 table,
                 s3_bucket,
                 s3_key,
                 #format_as_json=",",
                 #file_type="csv",
                 delimiter="",
                 #copy_options=tuple(),
                 autocommit=False,
                 parameters=None,
                 *args, **kwargs):
        
        # Initialise the operator
       

        super(StagedemoToRedshiftOperator, self).__init__(*args, **kwargs)
     
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id  
        #self.format_as_json = format_as_json
        #self.file_type = file_type
        self.delimiter = delimiter
        
        #self.copy_options = copy_options
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        """
        Executes the operator logic
        :param context:
        """

        self.log.info('StagedemoToRedshiftOperator execute')
        redshift= PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(self.aws_credentials_id)
        #self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=False)
        credentials = self.s3.get_credentials()

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info('StagedemoToRedshiftOperator s3_path: ' + s3_path)
        #formatted_sql = StageToRedshiftOperator.copy_sql.format(
        formatted_sql = StagedemoToRedshiftOperator.copy_query.format(
                    self.table,
                    s3_path,
                    credentials.access_key,
                    credentials.secret_key,
                    #self.file_type
                    #self.format_as_json
                    self.delimiter
                    #copy_options=copy_options
                        )
        #redshift.run(copy_query, self.autocommit)
       # redshift.run(formatted_sql)
        redshift.run(formatted_sql,self.autocommit)