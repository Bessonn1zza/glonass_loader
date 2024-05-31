from typing import Any, Dict, List
from utils.airflow_hooks import (
    GoogleSheetHook,
    GlonassHook,
    DatabaseHook
)
from utils.airflow_helpers import AirflowHelper
from utils.processors import DataProcessor
from utils.client_database import DatabaseClient

from airflow.models.baseoperator import BaseOperator


class GoogleSheetOperator(BaseOperator):
    def __init__(
            self,
            conn_id: str,
            task_id: str,
            request_kwargs: Dict[str, Any],
            extract_kwargs: Dict[str, Any],
            export_to: Dict[str, Any],
            **kwargs: Any
    ) -> None:
        
        super().__init__(
            task_id=task_id,
            **kwargs
        )
        self.conn_id = conn_id
        self.request_kwargs = request_kwargs
        self.extract_kwargs = extract_kwargs
        self.export_to = export_to

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = GoogleSheetHook(conn_id=self.conn_id)
        helper = AirflowHelper(context=context)
        dag_context = helper.extract_dag_context()
        primary_keys = self.extract_kwargs['primary_keys']
        
        processed_data = []
        for task_kwargs in self.request_kwargs:
            execution_date = task_kwargs["execution_dt"]
            dag_context["execution_dt"] = execution_date
            
            json_response = hook.get_sheet_data(task_kwargs)
            processor = DataProcessor(metadata=dag_context, content=json_response, primary_keys=primary_keys, remove_duplicates=False)
            processed_data.extend(processor.process_items())

        database_operator = DatabaseOperator(task_id=self.task_id, export_to=self.export_to, extract_kwargs=self.extract_kwargs)
        database_operator._load_data_to_database(processed_data)


class GlonassOperator(BaseOperator):
    def __init__(
            self,
            conn_id: str,
            task_id: str,
            task_kwargs: Dict[str, Any],
            request_kwargs: Dict[str, Any],
            extract_kwargs: Dict[str, Any],
            export_to: Dict[str, Any],
            **kwargs: Any
    ) -> None:
        
        super().__init__(
            task_id=task_id,
            **kwargs
        )
        self.hook = None
        self.conn_id = conn_id
        self.task_kwargs = task_kwargs
        self.request_kwargs = request_kwargs
        self.extract_kwargs = extract_kwargs
        self.export_to = export_to

    def execute(self, context: Dict[str, Any]) -> Any:
        hook = GlonassHook(conn_id=self.conn_id)
        helper = AirflowHelper(context=context)
        dag_context = helper.extract_dag_context()
        primary_keys = self.extract_kwargs["primary_keys"]
        data_path = self.task_kwargs.get("data_path")
        
        processed_data = []
        for task_kwargs in self.request_kwargs:
            module = task_kwargs["module"]
            args = task_kwargs["args"]
            execution_date = task_kwargs["execution_dt"]
            dag_context["execution_dt"] = execution_date

            json_response = hook.execute_request_with_args(self.task_kwargs["method"], module, args)
            records = self._process_json_response(json_response)
            processor = DataProcessor(dag_context, records, primary_keys, data_path, remove_duplicates=False)
            processed_data.extend(processor.process_items())

        database_operator = DatabaseOperator(task_id=self.task_id, export_to=self.export_to, extract_kwargs=self.extract_kwargs)
        database_operator._load_data_to_database(processed_data)

    def _process_json_response(self, json_response: Dict[str, Any]) -> List[Dict[str, Any]]:
        if "rows" in json_response and "columns" in json_response:
            records = []
            columns = json_response["columns"]

            for row in json_response["rows"]:
                if len(row) != len(columns):
                    continue

                record = {}
                for i, column in enumerate(columns):
                    record[column["name"]] = row[i]
                records.append(record)
        else:
            records = json_response

        return records


class DatabaseOperator(BaseOperator):
    def __init__(
            self,
            task_id: str,
            export_to: Dict[str, Any] = None,
            extract_kwargs: Dict[str, Any] = None,
            **kwargs: Any
    ) -> None:
        
        super().__init__(task_id=task_id, **kwargs)
        self.export_to = export_to
        self.extract_kwargs = extract_kwargs
        self.connection = DatabaseHook(conn_id=self.export_to["conn_id"]).create_connection()
    
    def _load_data_to_database(self, data: List[Dict[str, Any]]):
        with self.connection, self.connection.cursor as cursor:
            client = DatabaseClient(cursor=cursor, logger=self.log)
            table_name_prefix = self.export_to.get('prefix', '')
            if table_name_prefix:
                table_name_prefix += '_'
            table_name = self.extract_kwargs.get("table_name", "")
            table_path = f"{self.export_to.get('src_schema')}.{table_name_prefix}{table_name}"
            client._perform_insert_operation(table_path, data)