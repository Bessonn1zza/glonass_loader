
import hashlib
import logging
from typing import Any, Dict, List, Callable

from google.oauth2 import service_account
from google.auth.transport.requests import Request
from googleapiclient.discovery import Resource, build

from utils.loggers import Logger


def setup_client_wrapper(client_class: Callable, *client_args, **client_kwargs):
    client_instance = None
    
    def wrapper(method_name: str, *method_args, **method_kwargs):
        nonlocal client_instance
        if client_instance is None:
            client_instance = client_class(*client_args, **client_kwargs)
        return getattr(client_instance, method_name)(*method_args, **method_kwargs)
    
    return wrapper

def initialize_google_sheet_manager(credentials: Dict[str, Any]):
    client = GoogleClientFactory.init_google_client(credentials)
    return setup_client_wrapper(GoogleSheetManager, client)

def initialize_google_sheet_reader(credentials: Dict[str, Any], config: Dict[str, Any]):
    client = GoogleClientFactory.init_google_client(credentials)
    return setup_client_wrapper(GoogleSheetReader, client, config)

def get_spreadsheet(credentials: Dict[str, Any], sheet_id: str) -> Dict[str, Any]:
    return initialize_google_sheet_manager(credentials)('get_spreadsheet', sheet_id)

def get_values(credentials: Dict[str, Any], sheet_id: str, sheet_range: str = '!A1:Z') -> Dict[str, Any]:
    return initialize_google_sheet_manager(credentials)('get_values', sheet_id, sheet_range)

def get_batch_values(credentials: Dict[str, Any], sheet_id: str, sheet_range: str = '!A1:Z') -> Dict[str, Any]:
    return initialize_google_sheet_manager(credentials)('get_batch_values', sheet_id, sheet_range)

def get_metadata(credentials: Dict[str, Any], sheet_id: str) -> Dict[str, Any]:
    return initialize_google_sheet_manager(credentials)('get_metadata', sheet_id)

def get_sheet_data(credentials: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    return initialize_google_sheet_reader(credentials, config)('get_sheet_data')


class GoogleClientFactory:
    """
    Фабрика для создания клиентов Google API.

    Args:
        credentials (Dict[str, Any]): Учетные данные для создания клиентов.
        logger (logging.Logger, опционально): Объект логгера для записи информационных сообщений и ошибок. Если не указан, будет использован экземпляр по умолчанию.

    Attributes:
        log (logging.Logger): Объект логгера для записи информационных сообщений и ошибок.
        credentials (Dict[str, Any]): Учетные данные для создания клиентов.
        client_instances (Dict[Tuple[str, str], Resource]): Словарь экземпляров клиентов Google API, индексированный по имени сервиса и его версии.

    Methods:
        get_instance(credentials: Dict[str, Any], logger: logging.Logger = None) -> 'GoogleClientFactory': Получает экземпляр фабрики клиентов Google API, создавая его, если он еще не был создан.
        get_client(service_name: str, api_version: str) -> Resource: Получает экземпляр клиента Google API для указанного сервиса и его версии, создавая его при необходимости.
        _create_client(service_name: str, api_version: str) -> Resource: Создает и возвращает экземпляр клиента Google API для указанного сервиса и его версии.
        init_google_client(credentials: Dict[str, Any], logger: logging.Logger = None) -> Dict[str, Any]: Инициализирует клиенты Google API для сервисов Google Drive и Google Sheets и возвращает словарь с их экземплярами.
    """
    _instance = None

    def __init__(self, credentials: Dict[str, Any], logger: logging.Logger = None):
        self.log = logger if logger else Logger().logger
        self.credentials = credentials
        self.client_instances = {}

    @classmethod
    def get_instance(cls, credentials: Dict[str, Any], logger: logging.Logger = None):
        if cls._instance is None:
            cls._instance = cls(credentials, logger)
        return cls._instance

    def get_client(self, service_name: str, api_version: str):
        key = (service_name, api_version)
        if key not in self.client_instances:
            self.log.info(f"Created new client for service: {service_name}, version: {api_version}.")
            self.client_instances[key] = self._create_client(service_name, api_version)
        return self.client_instances[key]

    def _create_client(self, service_name: str, api_version: str):
        credentials = service_account.Credentials.from_service_account_info(
            self.credentials,             
            scopes=[
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets'
            ]
        )
        credentials.refresh(Request())
        service = build(serviceName=service_name, version=api_version, credentials=credentials)
        return service
    
    @classmethod
    def init_google_client(cls, credentials: Dict[str, Any], logger: logging.Logger = None) -> Dict[str, Any]:
        factory = cls.get_instance(credentials, logger)
        services = {
            'drive_service': factory.get_client('drive', 'v3'),
            'sheets_service': factory.get_client('sheets', 'v4')
        }
        return services


class GoogleSheetManager:
    """
    Класс для управления Google Sheets, предоставляющий функциональность для получения данных из Google Sheets и их манипуляций.

    Args:
        client (Dict[str, Resource]): Словарь клиентов Google API, содержащий сервисы Google Drive и Google Sheets.
        logger (logging.Logger, опционально): Объект логгера для записи информационных сообщений и ошибок. Если не указан, будет использован экземпляр по умолчанию.

    Attributes:
        log (logging.Logger): Объект логгера для записи информационных сообщений и ошибок.
        drive_service (Resource): Сервис Google Drive.
        sheets_service (Resource): Сервис Google Sheets.
        spreadsheet_cache (dict): Кэш для хранения данных о таблицах Google Sheets.
        metadata_cache (dict): Кэш для хранения метаданных таблиц Google Sheets.

    Methods:
        get_spreadsheet(sheet_id: str) -> Dict[str, Any]: Получает данные о таблице Google Sheets по её идентификатору.
        get_values(sheet_id: str, sheet_range: str) -> Dict[str, Any]: Получает значения из указанного диапазона ячеек таблицы Google Sheets.
        get_batch_values(sheet_id: str, sheet_range: str) -> Dict[str, Any]: Получает значения из указанных диапазонов ячеек для всех листов таблицы Google Sheets.
        get_metadata(sheet_id: str) -> Dict[str, Any]: Получает метаданные файла таблицы Google Sheets, такие как идентификатор файла, время последнего изменения и версия.
    """
    def __init__(self, client: Dict[str, Resource], logger: logging.Logger = None):
        self.log = logger if logger else Logger().logger
        self.drive_service = client.get('drive_service')
        self.sheets_service = client.get('sheets_service')
        self.spreadsheet_cache = {}
        self.metadata_cache = {}

    def get_spreadsheet(self, sheet_id: str) -> Dict[str, Any]:
        if sheet_id in self.spreadsheet_cache:
            return self.spreadsheet_cache[sheet_id]

        spreadsheet = self.sheets_service.spreadsheets().get(spreadsheetId=sheet_id).execute()
        self.spreadsheet_cache[sheet_id] = spreadsheet
        return spreadsheet
    
    def get_values(self, sheet_id: str, sheet_range: str) -> Dict[str, Any]:
        return self.sheets_service.spreadsheets().values().get(spreadsheetId=sheet_id, range=sheet_range).execute()
        
    def get_batch_values(self, sheet_id: str, sheet_range: str) -> Dict[str, Any]:
        spreadsheet = self.get_spreadsheet(sheet_id)
        sheet_ranges = [f"{sheet['properties']['title']}{sheet_range}" for sheet in spreadsheet['sheets']]
        return self.sheets_service.spreadsheets().values().batchGet(spreadsheetId=sheet_id, ranges=sheet_ranges).execute()

    def get_metadata(self, sheet_id: str) -> Dict[str, Any]:
        if sheet_id in self.metadata_cache:
            return self.metadata_cache[sheet_id]
        
        try:
            sheet_metadata = self.drive_service.files().get(
                fileId=sheet_id,
                fields='id, modifiedTime, lastModifyingUser, version'
            ).execute()

            metadata = {
                'file_id': sheet_metadata['id'],
                'modified_time': sheet_metadata['modifiedTime'],
                'last_modifying_user': sheet_metadata['lastModifyingUser']['displayName'],
                'version': sheet_metadata['version']
            }
            self.metadata_cache[sheet_id] = metadata
            self.log.info(f"Get metadata information: {metadata}.")
            return metadata
        except Exception as e:
            self.log.error(f"Error retrieving metadata: {e}.")
            raise e
        

class GoogleSheetReader(GoogleSheetManager):
    """
    Класс для чтения данных из Google Sheets с учетом конфигурационных параметров.

    Args:
        client (Dict[str, Resource]): Словарь клиентов Google API, содержащий сервисы Google Drive и Google Sheets.
        config (Dict[str, Any]): Конфигурационные параметры для чтения данных из Google Sheets, включая идентификатор таблицы, идентификатор листа, диапазон ячеек и сопоставление полей.
        logger (logging.Logger, опционально): Объект логгера для записи информационных сообщений и ошибок. Если не указан, будет использован экземпляр по умолчанию.

    Attributes:
        log (logging.Logger): Объект логгера для записи информационных сообщений и ошибок.
        sheet_id (str): Идентификатор Google Sheets таблицы.
        sheet_gid (str): Идентификатор листа в Google Sheets таблице.
        sheet_range (str): Диапазон ячеек для чтения данных из таблицы.
        field_mapping (Dict[str, str]): Сопоставление полей данных между исходной таблицей и Python объектами.
        spreadsheet (Dict[str, Any]): Данные о Google Sheets таблице.
        metadata (Dict[str, Any]): Метаданные Google Sheets таблицы.

    Methods:
        _get_header_hashes() -> Dict[str, Any]: Получает хэши заголовков столбцов из всех листов таблицы.
        _compare_sheet_hashes() -> None: Сравнивает хэши заголовков столбцов на всех листах таблицы.
        _get_sheet_name_by_gid() -> str: Получает название листа по его идентификатору.
        _parse_values(values: List[List[Any]]) -> List[Dict[str, Any]]: Преобразует значения ячеек в словари согласно сопоставлению полей.
        _fetch_data_from_all_sheets() -> List[Dict[str, Any]]: Получает данные из всех листов таблицы.
        get_sheet_data() -> List[Dict[str, Any]]: Получает данные из указанного листа или из всех листов таблицы в зависимости от настройки конфигурации.
    """
    def __init__(self, client: Dict[str, Resource], config: Dict[str, Any],logger: logging.Logger = None):
        super().__init__(client, logger=logger)
        self.log = logger if logger else Logger().logger
        self.sheet_id = config.get('sheet_id')
        self.sheet_gid = config.get('sheet_gid')
        self.sheet_range = config.get('sheet_range', '!A1:Z')
        self.field_mapping = config.get('field_mapping')
        self.log.info(f"Loader configuration - sheet_id: {self.sheet_id}, sheet_gid: {self.sheet_gid}, sheet_range: {self.sheet_range}, field_mapping: {self.field_mapping}.")
        
        self.spreadsheet = self.get_spreadsheet(self.sheet_id)
        self.spreadsheet_cache = {}

        self.metadata = self.get_metadata(self.sheet_id)
        self.metadata_cache = {}

    def _get_header_hashes(self) -> Dict[str, Any]:
        hashes = {}
        for sheet in self.spreadsheet['sheets']:
            sheet_name = sheet['properties']['title']
            sheet_range = f"{sheet_name}{self.sheet_range}"
            values = self.get_values(self.sheet_id, sheet_range).get('values', [])
            if values:
                head_hash = hashlib.sha256(str(values[0]).encode('utf-8')).hexdigest()
                hashes[sheet_name] = head_hash
        self.log.info(f"Get header hashes: {hashes}.")
        return hashes
    
    def _compare_sheet_hashes(self) -> None:
        hashes = self._get_header_hashes()
        first_sheet_title, *other_sheet_titles = hashes.keys()
        first_sheet_hashes = hashes[first_sheet_title]

        for sheet_title in other_sheet_titles:
            sheet_hashes = hashes[sheet_title]
            if sheet_hashes != first_sheet_hashes:
                error_message = f"Header names on sheet '{sheet_title}' differ from those on the first sheet. Please check the names."
                self.log.error(error_message)
                raise Exception(error_message)
            
    def _get_sheet_name_by_gid(self) -> str:
        for sheet in self.spreadsheet['sheets']:
            if sheet['properties']['sheetId'] == self.sheet_gid:
                return sheet['properties']['title']
            
    def _parse_values(self, values: List[List[Any]]) -> List[Dict[str, Any]]:
        if values:
            return [dict(zip(self.field_mapping.keys(), row)) for row in values[1:]]
        else:
            return []
        
    def _fetch_data_from_all_sheets(self) -> List[Dict[str, Any]]:
        all_sheet_data = []
        self.log.info(f"Fetching data from all sheets...")
        all_sheet_values = self.get_batch_values(self.sheet_id, self.sheet_range)['valueRanges']
        for values in all_sheet_values:
            data = self._parse_values(values.get('values', []))
            all_sheet_data.extend(data)
        
        return all_sheet_data

    def get_sheet_data(self) -> List[Dict[str, Any]]:
        all_sheet_data = []
        if self.sheet_gid is not None:
            sheet_name = self._get_sheet_name_by_gid()
            self.log.info(f"Loading from sheet '{sheet_name}' started.")

            sheet_range = f"{sheet_name}{self.sheet_range}"
            values = self.get_values(self.sheet_id, sheet_range).get('values', [])
            all_sheet_data = self._parse_values(values)
        else:
            self._compare_sheet_hashes()
            all_sheet_data = self._fetch_data_from_all_sheets()

        self.log.info(f"Loading successfully completed, loaded {len(all_sheet_data)} records.")
        return all_sheet_data