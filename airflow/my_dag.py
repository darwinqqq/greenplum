from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

DB_CONN = "adb"
DB_SCHEMA = "std11_3"
DB_PROC_LOAD = "f_full"
DB_LOAD_MART="f_mart"
PART_DATE='202104'
P_PXF_TABLE="gp"
SIMPLE_PARTITION_TABLE=['plan', 'sales']  
FULL_LOAD_TABLES = ['region','price','product','channel']
FULL_LOAD_FILES = {'region': 'region','price':'price','product':'product','channel':'channel'}
LOAD_PART_TABLE = f"select std11_3.f_load_simple_partition('{DB_SCHEMA}.{SIMPLE_PARTITION_TABLE}', 'date', '2021-05-01', '2021-06-01', '{P_PXF_TABLE}.{SIMPLE_PARTITION_TABLE}', 'admin', 'admin')"
MD_TABLE_LOAD_QUERY = f"select {DB_SCHEMA}.{DB_PROC_LOAD}(%(tab_name)s, %(file_name)s);"
LOAD_MART=f"select {DB_SCHEMA}.{DB_LOAD_MART}('PART_DATE');"

default_args = {
    'depends_on_past': False,
    'owner': 'std0',
    'start_date': datetime(2025, 4, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "task_dag",
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:

    task_start = DummyOperator(task_id="start") 

    with TaskGroup("load_part") as task_load_part:
        for table in SIMPLE_PARTITION_TABLE:
            task_part = PostgresOperator(task_id=f"start_insert_fact_{table}",
                                        postgres_conn_id="gp_conn",
                                        sql=LOAD_PART_TABLE.format(table)
                                )

    with TaskGroup("full_insert") as task_full_insert_tables:
        for table in FULL_LOAD_TABLES:
            task = PostgresOperator(task_id=f"load_table_{table}",
                                    postgres_conn_id=DB_CONN,
                                    sql=MD_TABLE_LOAD_QUERY,
                                    parameters={'tab_name': f'{DB_SCHEMA}.{table}', 'file_name': FULL_LOAD_FILES[table]}
                                    )
    task_load_mart = PostgresOperator(task_id="load_mart",
                                postgres_conn_id="gp_conn",
                                sql=LOAD_MART
                                )
    task_end = DummyOperator(task_id="end")

task_start >> task_part >> task_full_insert_tables >> task_load_mart >> task_end

