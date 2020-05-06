from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#f8a5c2'
    ui_fgcolor = '#000000'
    template_fields = ("s3_src_pattern",)

    @apply_defaults
    def __init__(
        self,
        # conexiones
        redshift_conn='',
        aws_credentials='',
        table='',
        s3_src_bucket='',
        s3_src_pattern='',
        data_format='json',   
        delimiter=',',
        jsonpaths='auto',
        ignore_header=0,
        *args, **kwargs):
        
        super(StageToRedshiftOperator,self).__init__(*args, **kwargs)

        self.redshift_conn = redshift_conn
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_src_bucket = s3_src_bucket
        self.s3_src_pattern = s3_src_pattern
        self.data_format = data_format
        self.delimiter = delimiter
        self.jsonpaths = jsonpaths
        self.ignore_header = ignore_header

        self.json_copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            JSON '{}'
            COMPUPDATE OFF
        """

        self.csv_copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            IGNOREHEADER {}
            DELIMITER '{}'
        """

    def execute(self,context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn)

        self.log.info("****************************************")
        self.log.info("Copying data from S3 to Redshift...")
            
        rendered_key = self.s3_src_pattern.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_src_bucket, rendered_key)
           
        if self.data_format == "json":
            formatted_sql = self.json_copy_sql.format(
                self.table,  # COPY
                s3_path,     # FROM
                credentials.access_key, # ACCESS_KEY_ID
                credentials.secret_key, # SECRET_ACCESS_KEY
                self.jsonpaths # JSON
            )
            redshift.run(formatted_sql)

        if self.data_format == "csv":
            formatted_sql = self.csv_copy_sql.format(
                self.table,  # COPY
                s3_path,  # FROM
                credentials.access_key,  # ACCESS_KEY_ID
                credentials.secret_key,  # SECRET_ACCESS_KEY
                self.ignore_headers, # IGNOREHEADER
                self.delimiter # DELIMITER
            )
            redshift.run(formatted_sql)
       
        self.log.info("Finished copying data from S3 to Redshift")
        self.log.info("****************************************")

        
