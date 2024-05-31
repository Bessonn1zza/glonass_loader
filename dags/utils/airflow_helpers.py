import logging
from typing import Any, Dict, Optional

from airflow import settings
from airflow.models import DagRun, XCom, TaskInstance
from airflow.utils.state import DagRunState, State

from utils.loggers import Logger

class AirflowHelper:
    def __init__(self, context: Dict[str, Any], logger: logging.Logger = None) -> None:
        """
        :param context: Context of operator execution in Apache Airflow.
        """
        self.log = logger if logger else Logger().logger
        self.context = context

    def extract_dag_context(self) -> Dict[str, Any]:
        """Extracts context information from the current DAG run."""
        dag_id = self.context['dag'].dag_id
        dag_task_id = self.context['task_instance'].task_id
        dag_start_dttm = self.context['dag_run'].start_date 
        dag_run_id = self.context['dag_run'].run_id
        job_id = self.context['task_instance'].job_id
        ts_nodash = self.context['ts_nodash']
        dataflow_id = f'{ts_nodash}___run_id-{dag_run_id}___job_id-{job_id}'
        context = {
            "dag_id": dag_id,
            "dag_task_id": dag_task_id.split('.')[0],
            "dag_start_dttm": dag_start_dttm.strftime("%Y-%m-%d %H:%M:%S"),
            "dataflow_id": dataflow_id
        }
        return context

    def _get_session(self):
        return settings.Session()
    
    def restart_dag_execution(self) -> None:
        """
        Restarts the execution of DAGs launched after the specified date.

        This method sets all DAGs launched after the execution date of the task in the context to the QUEUED state.
        This allows restarting the execution of these DAGs from the beginning, ignoring previous executions.
        """
        dag_id = self.context['dag'].dag_id
        execution_date = self.context['execution_date']

        with self._get_session() as session:
            dagruns_to_clear = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == dag_id,
                    DagRun.execution_date > execution_date,
                    DagRun.run_type == 'scheduled'
                )
                .order_by(DagRun.execution_date.asc())
                .all()
            )

            for dagrun in dagruns_to_clear:
                task_query = session.query(TaskInstance).filter(
                    TaskInstance.dag_id == dagrun.dag_id,
                    TaskInstance.run_id == dagrun.run_id
                )

                dagrun_query = session.query(DagRun).filter(
                    DagRun.dag_id == dagrun.dag_id,
                    DagRun.run_id == dagrun.run_id
                )

                task_query.update({TaskInstance.state: State.NONE}, synchronize_session=False)
                dagrun_query.update({DagRun.state: DagRunState.QUEUED}, synchronize_session=False)

            session.commit()

    def get_last_dag_run(self, dag_id: str) -> Optional[DagRun]:
        """Gets the last dag_run for the given dag_id."""
        with self._get_session() as session:
            return (
                session.query(DagRun)
                .filter_by(dag_id=dag_id)
                .order_by(DagRun.execution_date.desc())
                .first()
            )

    def get_last_xcom_value(self, dag_id: str, key: str) -> Optional[XCom]:
        """Gets the last XCom entries for the given dag_id and key."""
        with self._get_session() as session:
            return (
                session.query(XCom)
                .filter_by(dag_id=dag_id, key=key)
                .order_by(XCom.timestamp.desc())
                .first()
            )

    def push_to_xcom(self, key: str, value: Any) -> None:
        """Saves a value to XCom variable."""
        try:
            ti: TaskInstance = self.context.get('ti')
            if ti:
                ti.xcom_push(key=key, value=value)
            else:
                error_message = "TaskInstance not found in context."
                self.log.info(error_message)
                raise ValueError(error_message)
        except Exception as e:
            error_message = f"Failed to save value to XCom with key '{key}': {str(e)}"
            self.log.info(error_message)
            raise ValueError(error_message)

    def pull_from_xcom(self, key: str, dag_id: Optional[str] = None) -> Any:
        """Retrieves a value from the last XCom entry for the given key and dag_id."""
        dag_id = dag_id or self.context.get('dag').dag_id

        xcom_value = self.get_last_xcom_value(dag_id, key)

        if xcom_value:
            return xcom_value.value
        
        error_message = f"No XCom value found for DAG '{dag_id}' and key '{key}'"
        self.log.info(error_message)
        raise ValueError(error_message)