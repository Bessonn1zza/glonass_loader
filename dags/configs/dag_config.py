DEFAULT_DAG_CONFIG = {
    'default_args': {
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    },
    'concurrency': 4,
    'max_active_runs': 1,
    'catchup': False
}
