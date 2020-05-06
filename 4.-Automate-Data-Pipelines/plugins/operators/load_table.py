from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadTableOperator(BaseOperator):
    ui_color = '#a29bfe'
    ui_fgcolor = '#000000'
    template_fields = ("table",)

    def __init__(
        self,
        redshift_conn='',
        table="",
        sql_select_stmt="",
        append_data=True,
        *args, **kwargs
    ):
        super(LoadTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn = redshift_conn
        self.table = table
        self.sql_select_stmt = sql_select_stmt
        self.append_data=append_data

        self._sql = 'INSERT INTO "{table:}" ({sql_select_stmt:})'

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn)

        self.log.info("****************************************")
        self.log.info(f'Loading fact table in Redshift {self.table}')

        query = ''

        if self.append_data:
            query  = self._sql.format(**dict(
            table  = self.table,
            sql_select_stmt = self.sql_select_stmt
            ))
        else:
            query += f'TRUNCATE TABLE "{self.table}";\n'

        self.log.info(f'Loading fact table {self.table} ...')
        self.log.debug(f"Formatted query: {query}")
        redshift.run(query)
        self.log.info(f'Finished loading fact table {self.table}')
        self.log.info("****************************************")




