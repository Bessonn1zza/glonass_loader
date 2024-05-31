import os
import glob
from datetime import datetime
from pathlib import Path
from datetime import datetime, timedelta

from configs.dag_config import DEFAULT_DAG_CONFIG
from configs.glonass_config import (
    DEFAULT_TASK_CONFIG, 
    GLONASS_CONFIG
)
from utils.airflow_operators import GlonassOperator
from utils.generators import ConfigGenerator

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable


dag_file = Path(__file__)
dag_id = dag_file.stem

refresh_start_date_var = 'refresh_start_date'
refresh_end_date_var = 'refresh_end_date'
additional_keys = 'additional_keys'
default_offset_days_var = 'default_offset_days'

variables = {
    refresh_start_date_var: Variable.get(refresh_start_date_var),
    refresh_end_date_var: Variable.get(refresh_end_date_var),
    additional_keys: Variable.get(additional_keys, deserialize_json=True),
    default_offset_days_var: int(Variable.get(default_offset_days_var))
}

def set_date_variables():
    today_date = datetime.today().date()
    refresh_start_date = (today_date - timedelta(days=variables[default_offset_days_var])).isoformat()
    refresh_end_date = today_date.isoformat()

    Variable.set(refresh_start_date_var, refresh_start_date)
    Variable.set(refresh_end_date_var, refresh_end_date)

def process_configs(configs):
    config_processor = ConfigGenerator(timezone_offset=4)

    return [
        {
            **config,
            'request_kwargs': config_processor.render_kwargs(
                request_kwargs=config['request_kwargs'],
                offset_days=variables[default_offset_days_var],
                refresh_start_date=variables[refresh_start_date_var],
                refresh_end_date=variables[refresh_end_date_var],
                additional_keys=variables[additional_keys]
            )
        }
        for config in configs
    ]

SQL_TEMPLATE_CACHE = {}
SQL_SCRIPTS_CACHE = {}

def get_sql_template(template_name, template_dir, root_dir=None):
    root_dir = root_dir or os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'sql'))
    template_path = os.path.join(root_dir, template_dir, template_name + ".sql")
    if template_path not in SQL_TEMPLATE_CACHE:
        with open(template_path, "r") as f:
            SQL_TEMPLATE_CACHE[template_path] = f.read()
    return SQL_TEMPLATE_CACHE[template_path]

def get_sql_scripts_in_dir(directory):
    if directory not in SQL_SCRIPTS_CACHE:
        SQL_SCRIPTS_CACHE[directory] = [os.path.splitext(os.path.basename(f))[0] for f in glob.glob(os.path.join(directory, "*.sql"))]
    return SQL_SCRIPTS_CACHE[directory]

def create_extraction_task(config):
    group_id = config['extract_kwargs']['table_name']
    with TaskGroup(group_id=group_id) as extraction_group:
        extraction_params = {**DEFAULT_TASK_CONFIG, **config, 'task_id': 'extract_to_src'}
        extraction_task = GlonassOperator(**extraction_params)
        staging_task, ods_tasks = create_processing_tasks(extraction_params)
        extraction_task >> staging_task >> ods_tasks
    return extraction_group

def create_processing_tasks(config):
    table_name = f"{config['export_to']['prefix']}_{config['extract_kwargs']['table_name']}"
    conn_id = config['export_to']['conn_id']
    src_schema = config['export_to']['src_schema']
    stg_schema = config['export_to']['stg_schema']
    ods_schema = config['export_to']['ods_schema']
    tasks = []

    staging_task = create_postgres_operator(
        task_id=f'extract_to_stg',
        conn_id=conn_id,
        template_dir=stg_schema,
        template_name=table_name,
        source_table_path=f"{src_schema}.{table_name}",
        target_table_path=f"{stg_schema}.{table_name}"
    )

    template_names = get_sql_scripts_in_dir(f"sql/{ods_schema}/{table_name}")
    for template_name in template_names:
        ods_task = create_postgres_operator(
            task_id=f'extract_to_ods_{template_name}',
            conn_id=conn_id,
            template_dir=f"{ods_schema}/{table_name}",
            template_name=template_name,
            source_table_path=f"{stg_schema}.{table_name}",
            target_table_path=f"{ods_schema}.{template_name}"
        )
        ods_task.set_upstream(staging_task)
        tasks.append(ods_task)

    return staging_task, tasks

def create_postgres_operator(task_id, conn_id, template_dir, template_name, source_table_path, target_table_path):
    return PostgresOperator(
        task_id=task_id,
        postgres_conn_id=conn_id,
        sql=get_sql_template(template_dir=template_dir, template_name=template_name),
        params={
            "source_table_path": source_table_path,
            "target_table_path": target_table_path
        }
    )

with DAG(
    **DEFAULT_DAG_CONFIG,
    dag_id=dag_id,
    schedule_interval='15 5,17 * * *',
    start_date=datetime(year=2024, month=3, day=20),
    tags=['extraction', 'glonass']
) as dag:
    
    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')

    set_date_task = PythonOperator(
        task_id='set_date',
        python_callable=set_date_variables
    )

    extraction_task_group = [create_extraction_task(config) for config in process_configs(GLONASS_CONFIG)]

    start_task >> extraction_task_group >> set_date_task >> end_task