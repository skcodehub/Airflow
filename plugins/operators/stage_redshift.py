from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        TRUNCATE TABLE {};
        COPY {}
        FROM '{}'
        format as json '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}';
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 #st_date="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path
        #self.st_date = st_date
    def execute(self, context):
        self.log.info('Copying {} from S3 to Redshift'.format(self.table)) 
        #st_date = self.st_date
        st_date=context.get("execution_date")
        s3_path=""
        if self.s3_key == "log_data":
            s3_path="s3://{}/{}/{}/{}/{}-events.json".format(self.s3_bucket, self.s3_key,st_date.year, st_date.month, st_date.strftime("%Y-%m-%d"))
        else:
            s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info('Loading {}'.format(s3_path))
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        #s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
       
        #if self.s3_key == "log_data":
         #   s3_path="s3://{}/{}/{}/{}/{}-events.json".format(self.s3_bucket, self.s3_key,st_date.year, st_date.month, st_date.strftime("%Y-%m-%d"))
        #else:
        #    s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.table,
            s3_path,
            #self.s3_key,
            self.json_path,
            credentials.access_key,
            credentials.secret_key
        )
        redshift.run(formatted_sql)