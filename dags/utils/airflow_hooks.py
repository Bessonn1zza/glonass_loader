import logging
import threading
import json
from typing import Dict, Any, Callable

from airflow.hooks.base_hook import BaseHook

from utils.client_google import (
    GoogleClientFactory, 
    GoogleSheetManager, 
    GoogleSheetReader
)
from utils.client_glonass import (
    GlonassSessionManager,
    GlonassRequestExecutor
)

from utils.client_database import DatabaseConnection


class GoogleSheetHook(BaseHook):
    def __init__(self, conn_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self._client = None

    def get_client(self):
        if self._client is not None:
            return self._client
        
        conn = self.get_connection(self.conn_id)
        credentials = json.loads(conn.password)

        self._client = self._initialize_google_client(credentials)
        return self._client

    def setup_client_wrapper(self, client_class: Callable, logger: logging.Logger, *client_args, **client_kwargs) -> Callable:
        client = self.get_client()
        
        def wrapper(method_name: str, *method_args, **method_kwargs):
            client_instance = client_class(client, logger, *client_args, **client_kwargs)
            return getattr(client_instance, method_name)(*method_args, **method_kwargs)
        
        return wrapper
    
    def _initialize_google_client(self, credentials: Dict[str, Any]):
        self.log.info("Google client successfully initialized.")
        return GoogleClientFactory.init_google_client(credentials, self.log)

    def _initialize_google_sheet_manager(self):
        return self.setup_client_wrapper(GoogleSheetManager, self.log)

    def _initialize_google_sheet_reader(self, config: Dict[str, Any]):
        return self.setup_client_wrapper(GoogleSheetReader, config, self.log)

    def get_spreadsheet(self, sheet_id: str) -> Dict[str, Any]:
        return self._initialize_google_sheet_manager()('get_spreadsheet', sheet_id)

    def get_values(self, sheet_id: str, sheet_range: str = '!A1:Z') -> Dict[str, Any]:
        return self._initialize_google_sheet_manager()('get_values', sheet_id, sheet_range)

    def get_batch_values(self, sheet_id: str, sheet_range: str = '!A1:Z') -> Dict[str, Any]:
        return self._initialize_google_sheet_manager()('get_batch_values', sheet_id, sheet_range)

    def get_metadata(self, sheet_id: str) -> Dict[str, Any]:
        return self._initialize_google_sheet_manager()('get_metadata', sheet_id)

    def get_sheet_data(self, config: Dict[str, Any]) -> Dict[str, Any]:
        return self._initialize_google_sheet_reader(config)('get_sheet_data')
    

class GlonassHook(BaseHook):
    def __init__(self, conn_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self._session_manager_instance = None
        self._session_manager_lock = threading.Lock()

    @property
    def _session_manager(self):
        with self._session_manager_lock:
            if not self._session_manager_instance:
                self._session_manager_instance = self._create_session_manager()
            return self._session_manager_instance

    def _create_session_manager(self):
        conn = self.get_connection(self.conn_id)
        credentials = {
            'host': conn.host,
            'port': conn.port,
            'login': conn.login,
            'password': conn.password
        }
        self.log.info("Session manager successfully initialized.")
        return GlonassSessionManager(credentials, self.log)

    def execute_request(self, operation: str, module: str, args: Dict[str, Any]):
        executor = GlonassRequestExecutor(self._session_manager, self.log)
        return executor.execute_request(operation, module, args)

    def execute_request_with_args(self, operation: str, module: str, args: Dict[str, Any]):
        query_string = "&".join(f"{key}={value}" for key, value in args.items())
        return self.execute_request(operation, module, query_string)


class DatabaseHook(BaseHook):
    def __init__(self, conn_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.connection = None
        self.connection_cache = {}

    def create_connection(self):
        if not self.connection:
            conn = self.get_connection(self.conn_id)
            conn_type = conn.conn_type.lower()
            conn_config = {
                'host': conn.host,
                'port': conn.port,
                'database': conn.schema,
                'user': conn.login,
                'password': conn.password
            }
            self.log.info("Database hook successfully initialized.")
            self.connection = self.connect_to_database(conn_type, conn_config)
        return self.connection

    def connect_to_database(self, conn_type: str, conn_config: Dict[str, Any]):
        if self.connection_cache.get(conn_type):
            return self.connection_cache[conn_type]

        connection_functions = {
            'postgres': self.connect_postgres,
            'mysql': self.connect_mysql
        }
        connect_func = connection_functions.get(conn_type)
        if not connect_func:
            error_message = f"Unsupported connection type: {conn_type}"
            self.log.error(error_message)
            raise ValueError(error_message)

        connection = DatabaseConnection(connector=connect_func, conn_config=conn_config, logger=self.log)
        self.connection_cache[conn_type] = connection
        return connection

    def connect_postgres(self, **conn_config):
        import psycopg2
        return psycopg2.connect(**conn_config)

    def connect_mysql(self, **conn_config):
        import mysql.connector
        return mysql.connector.connect(**conn_config)