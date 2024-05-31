import logging
import time
from typing import Any, Dict, List, Union, Tuple, Optional, Callable
from contextlib import AbstractContextManager
from utils.loggers import Logger


class DatabaseConnection(AbstractContextManager):
    def __init__(
            self, 
            connector: Callable[..., Any],
            max_retries: int = 5, 
            retry_delay: int = 30,
            conn_config: Dict[str, Any] = None,
            logger: logging.Logger = None
        ):

        self.connector = connector
        self.conn_config = conn_config or {}
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.connection = None
        self.cursor = None
        self.logger = logger or Logger().logger

    def __enter__(self):
        self.create_connection()
        self.create_cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback_transaction(str(exc_val))
        else:
            self.commit_transaction()
        self.close_cursor()
        self.close_connection()

    def create_connection(self) -> None:
        if not self.connection:
            host = self.conn_config.get('host', 'localhost')
            port = self.conn_config.get('port', 5432)
            self.logger.info(f"Connection open {host}:{port}")
            self.connection = self.retry_connection()

    def retry_connection(self):
        for attempt in range(1, self.max_retries + 1):
            try:
                if attempt > 1:
                    self.logger.info(f"Connection attempt ({attempt}/{self.max_retries})")
                return self.connector(**self.conn_config)
            except Exception as e:
                error_message = f"Error connecting: {e}"
                self.logger.error(error_message)
                if attempt < self.max_retries:
                    self.logger.info(f"Retrying connection in {self.retry_delay} seconds")
                    time.sleep(self.retry_delay)
        
        error_message = f"Exceeded maximum connection attempts. Last error: {error_message}"
        self.logger.error(error_message)
        raise ConnectionError(error_message)

    def close_connection(self):
        if self.connection:
            self.logger.info('Closing connection')
            self.connection.close()
            self.connection = None

    def commit_transaction(self):
        self.connection.commit()
        self.logger.info('Query successfully commit')
    
    def rollback_transaction(self, error_description: str):
        self.connection.rollback()
        error_message = f"Query execution failed. Error description: {error_description}"
        self.logger.error(error_message)
        raise Exception(error_message)

    def create_cursor(self):
        if not self.cursor:
            self.logger.info('Creating cursor')
            self.cursor= self.connection.cursor()

    def close_cursor(self):
        if self.cursor:
            self.logger.info('Closing cursor')
            self.cursor.close()
   

class DatabaseClient:
    FetchAll = List[Tuple]
    FetchOne = Tuple
    FetchFirstColumn = Any
    FetchColumnNames = List[str]

    def __init__(self, cursor, logger: logging.Logger = None):
        self.cursor = cursor
        self.logger = logger or Logger().logger
        self.table_checked = False
        self.index_checked = False
        self.constraint_checked = False

    def _fetch_all_rows(self) -> FetchAll:
        result = self.cursor.fetchall()
        return result if result else []

    def _fetch_one_row(self) -> FetchOne:
        result = self.cursor.fetchone()
        return result if result else ()

    def _fetch_column_names(self) -> FetchColumnNames:
        result = [column[0] for column in self.cursor.description]
        return result if result else []

    def _fetch_first_column(self) -> FetchFirstColumn:
        result = self.cursor.fetchone()
        return result[0] if result is not None else None

    def _fetch_data_from_cursor(
        self, operation: str
    ) -> Union[FetchAll, FetchOne, FetchFirstColumn, FetchColumnNames, None]:
        operation_mapping = {
            "fetch_all_rows": self._fetch_all_rows,
            "fetch_one_row": self._fetch_one_row,
            "fetch_first_column": self._fetch_first_column,
            "fetch_column_names": self._fetch_column_names,
        }
        fetch_method = operation_mapping.get(operation)
        if fetch_method:
            return fetch_method()
        else:
            error_message = f"Invalid operation: {operation}"
            self.logger.error(error_message)
            raise ValueError(error_message)

    def _extract_table_path(self, table_name: str) -> Tuple[str, str]:
        return table_name.split('.')

    def _execute_query(self, query: str, operation: str = "execute", query_params: Optional[Tuple] = None):
        max_query_length_to_display = 200
        display_query = (
            f"{query[:max_query_length_to_display]}..."
            if len(query) > max_query_length_to_display
            else query
        )
        self.logger.info(f"Executing query: {display_query}")
        self._execute_query_operation(query, operation, query_params)
        self.logger.info("Query execution successful")

    def _execute_query_operation(self, query: str, operation: str, query_params: Optional[Tuple] = None):
        operation_mapping = {
            "execute": self.cursor.execute,
            "executemany": self.cursor.executemany,
        }
        execute_method = operation_mapping.get(operation)
        if not execute_method:
            error_message = f"Invalid operation: {operation}"
            self.logger.error(error_message)
            raise ValueError(error_message)
        execute_method(query, query_params)

    def _execute_query_get_table_columns(self, table_path: str) -> List[str]:
        table_schema, table_name = self._extract_table_path(table_path)
        columns_query = (
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_schema = '{}' AND table_name = '{}';"
        ).format(table_schema, table_name)
        self._execute_query(query=columns_query)
        columns_result = self._fetch_data_from_cursor("fetch_all_rows")
        if not columns_result:
            error_message = f"Columns not found for table '{table_path}'"
            self.logger.error(error_message)
            raise Exception(error_message)
        table_columns = [column[0] for column in columns_result]
        self.logger.info(f"Columns to insert for table '{table_path}': {table_columns}")
        return table_columns

    def _execute_query_get_table_constraint(self, table_path: str) -> str:
        table_schema, table_name = self._extract_table_path(table_path)
        constraint_query = (
            "SELECT conname "
            "FROM pg_constraint "
            "INNER JOIN pg_class ON conrelid = pg_class.oid "
            "INNER JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid "
            "WHERE pg_namespace.nspname = '{}' AND pg_class.relname = '{}';"
        ).format(table_schema, table_name)
        self._execute_query(query=constraint_query)
        constraint_name = self._fetch_data_from_cursor("fetch_first_column")
        if not constraint_name:
            error_message = f"Constraint not found for table '{table_path}'"
            self.logger.error(error_message)
            raise Exception(error_message)
        self.logger.info(f"Constraint to upsert for table '{table_path}': '{constraint_name}'")
        return constraint_name

    def _execute_query_check_table_exists(self, table_path: str) -> bool:
        table_schema, table_name = self._extract_table_path(table_path)
        exists_query = (
            "SELECT EXISTS ("
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = '{}' AND table_name = '{}');"
        ).format(table_schema, table_name)
        self._execute_query(query=exists_query)
        table_exists = self._fetch_data_from_cursor("fetch_first_column")
        self.logger.info("Table '{}' {}exists".format(table_path, "" if table_exists else "does not "))
        return bool(table_exists)
    
    def _execute_query_check_constraint_exists(self, table_path: str, constraint_name: str) -> bool:
        table_schema, table_name = self._extract_table_path(table_path)
        exists_query = (
            "SELECT EXISTS ("
            "SELECT 1 FROM pg_constraint "
            "INNER JOIN pg_class ON conrelid = pg_class.oid "
            "INNER JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid "
            "WHERE pg_namespace.nspname = '{}' AND pg_class.relname = '{}' "
            "AND conname = '{}');"
        ).format(table_schema, table_name, constraint_name)
        self._execute_query(query=exists_query)
        constraint_exists = self._fetch_data_from_cursor("fetch_first_column")
        self.logger.info("Constraint '{}' {}exists for table '{}'".format(constraint_name, "" if constraint_exists else "does not ", table_path))
        return bool(constraint_exists)

    def _execute_query_check_index_exists(self, table_path: str, index_name: str) -> bool:
        schema_name, table_name = table_path.split('.')
        exists_query = (
            "SELECT EXISTS ("
            "SELECT 1 FROM pg_indexes "
            "WHERE schemaname = '{}' AND tablename = '{}' AND indexname = '{}'"
            ");"
        ).format(schema_name, table_name, index_name)
        self._execute_query(query=exists_query)
        index_exists = self._fetch_data_from_cursor("fetch_first_column")
        self.logger.info("Index '{}' {}exists for table '{}'".format(index_name, "" if index_exists else "does not ", table_path))
        return bool(index_exists)
        
    def _execute_query_create_table(self, table_path: str, columns_with_types: List[Dict[str, Any]]) -> bool:
        column_definitions = ', '.join(f"{column['name']} {column['type']}" for column in columns_with_types)
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_path} ({column_definitions});"

        try:
            self._execute_query(query=create_table_query)
            self.logger.info(f"Table '{table_path}' created successfully.")
            return True
        except Exception as error_description:
            error_message = f"Failed to create table '{table_path}'. Error: {error_description}"
            self.logger.error(error_message)
            return Exception(error_message)

    def _execute_query_create_constraint(self, table_path: str, constraint_name: str, column_names: List[str]) -> bool:
        try:
            columns = ", ".join(column_names)
            alter_table_query = f"ALTER TABLE {table_path} ADD CONSTRAINT {constraint_name} UNIQUE ({columns});"
            self._execute_query(query=alter_table_query)
            self.logger.info(f"Unique constraint '{constraint_name}' created successfully for table '{table_path}' on columns '{', '.join(column_names)}'.")
            return True
        except Exception as error_description:
            error_message = f"Failed to create unique constraint '{constraint_name}' for table '{table_path}' on columns '{', '.join(column_names)}'. Error: {error_description}"
            self.logger.error(error_message)
            return Exception(error_message)

    def _execute_query_create_index(self, table_path: str, index_name: str, column_names: List[str]) -> bool:
        try:
            columns = ", ".join(column_names)
            create_index_query = f"CREATE INDEX {index_name} ON {table_path} USING BTREE ({columns});"
            self._execute_query(query=create_index_query)
            self.logger.info(f"Index '{index_name}' created successfully for table '{table_path}' on columns '{', '.join(column_names)}'.")
            return True
        except Exception as error_description:
            error_message = f"Failed to create index '{index_name}' for table '{table_path}' on columns '{', '.join(column_names)}'. Error: {error_description}"
            self.logger.error(error_message)
            raise Exception(error_message)

    def _execute_query_truncate_table(self, table_path: str) -> bool:
        truncate_table_query = f"TRUNCATE TABLE {table_path};"
        try:
            self._execute_query(truncate_table_query)
            self.logger.info(f"Table '{table_path}' truncated successfully.")
            return True
        except Exception as e:
            error_message = f"Failed to truncate table '{table_path}'. Error description: {e}"
            self.logger.error(error_message)
            raise Exception(error_message)

    def _execute_query_drop_table(self, table_path: str) -> bool:
        drop_table_query = f"DROP TABLE IF EXISTS {table_path};"
        try:
            self._execute_query(drop_table_query)
            self.logger.info(f"Table '{table_path}' dropped successfully.")
            return True
        except Exception as e:
            error_message = f"Failed to drop table '{table_path}'. Error description: {e}"
            self.logger.error(error_message)
            raise Exception(error_message)

    def _perform_insert_operation(self, table_path: str, data: List[Union[dict, Tuple]], operation: str = "insert") -> bool:
        keys_for_insert = data[0].keys()
        values_to_insert = [tuple(item[column] for column in keys_for_insert) for item in data]
        
        insert_query = "INSERT INTO {} ({}) VALUES ({})".format(
            table_path,
            ', '.join(keys_for_insert),
            ', '.join(['%s'] * len(keys_for_insert))
        )

        try:
            self._execute_query(query=insert_query, operation="executemany", query_params=values_to_insert)
            self.logger.info(f"{len(data)} rows inserted into table '{table_path}'")
            return True
        except Exception as error_description:
            raise Exception(error_description)