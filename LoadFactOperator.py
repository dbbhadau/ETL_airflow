  
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    load_fact_sql = """
        INSERT INTO {}{}
        
        ;
        
     
    """
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id,
                 table,
                 #columns,
                 query,
                 append=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        #self.columns = columns
        self.query = query
        self.append = append

    def execute(self, context):
        self.log.info('LoadFactOperator has started')
        # obtain the redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #columns = "({})".format(self.columns)
        if self.append == False:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))
        load_sql = LoadFactOperator.load_fact_sql.format(
            self.table,
            #columns,
            self.query
        )
        self.log.info(f"Executing  Load SQL Read sql log info here{load_sql} ...")
        redshift.run(load_sql)
    
      