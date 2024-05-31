import json
import os
from dotenv import load_dotenv
from typing import Any, Dict, List

from loggers import Logger

file_manager = None

def initialize_file_manager(project_root: str = None):
    global file_manager
    if not file_manager:
        file_manager = FileManager(project_root)
    return file_manager

def file_manager_method_wrapper(method_name, *args, **kwargs):
    file_manager_instance = initialize_file_manager() 
    method = getattr(file_manager_instance, method_name)
    return method(*args, **kwargs)

def create_folders(folders):
    return file_manager_method_wrapper("create_folders", folders)

def get_file_path(file_name):
    return file_manager_method_wrapper("get_file_path", file_name)

def get_folder_path(folder_name, parent_folder=None):
    return file_manager_method_wrapper("get_folder_path", folder_name, parent_folder)

def get_env_variable(variable_name):
    return file_manager_method_wrapper("get_env_variable", variable_name)

def save_to_file(file_path, data):
    return file_manager_method_wrapper("save_to_file", file_path, data)

def load_from_file(file_path):
    return file_manager_method_wrapper("load_from_file", file_path)


class FileManager(Logger):
    '''
    Класс для управления файлами в проекте.

    Args:
        project_root (str, optional): Корневая директория проекта. По умолчанию None.

    Attributes:
        project_root (str): Путь к корневой директории проекта.
        env_variables (Dict[str, str]): Словарь с переменными окружения, загруженными из файла .env.
    
    Methods:
        create_folders(folders: List[str]) -> None: Создает директории, если они не существуют.
        get_folder_path(folder_name: str, parent_folder: str = None) -> str: Возвращает путь к указанной папке.
        get_file_path(file_name: str) -> str: Находит файл в структуре проекта.
        get_env_variable(variable_name: str) -> str: Возвращает значение переменной окружения.
        save_to_file(file_path: str, data: Any) -> None: Сохраняет данные в файл.
        load_from_file(file_path: str) -> Any: Загружает данные из файла.

        _find_project_folder() -> str: Находит корневую директорию проекта.
        _get_project_folder(project_root: str) -> str: Возвращает корневую директорию проекта, если она указана, иначе находит её.
        _check_folder_existence(folder_path: str) -> bool: Проверяет существование директории по пути.
        _load_env_file() -> Dict[str, str]: Загружает переменные окружения из файла .env.
    
    '''
    def __init__(self, project_root: str = None):

        super().__init__()
        self.project_root = self._get_project_folder(project_root)
        self.logger.info(f"Project folder path '{self.project_root}'")
        self.env_variables = self._load_env_file()

    def _find_project_folder(self) -> str:
        project_folder_name = 'dags'
        current_file_path = os.path.abspath(__file__)

        while True:
            current_folder_name = os.path.basename(current_file_path)
            if current_file_path == os.path.dirname(current_file_path):
                error_message = f"Project folder '{project_folder_name}' not found"
                self.logger.info(error_message)
                raise FileNotFoundError(error_message)
            
            if current_folder_name == project_folder_name:
                return current_file_path
            
            current_file_path = os.path.dirname(current_file_path)

    def _get_project_folder(self, project_root: str) -> str:
        if project_root:
            project_root = os.path.abspath(project_root)
            if self._check_folder_existence(project_root):
                return project_root
        return self._find_project_folder()

    def _check_folder_existence(self, folder_path: str) -> bool:
        if os.path.isdir(folder_path):
            return True
        else:
            error_message = f"Directory '{folder_path}' does not exist"
            self.logger.error(error_message)
            raise FileNotFoundError(error_message)
        
    def _load_env_file(self) -> Dict[str, str]:
        try:
            env_file_path = self.get_file_path('.env')
            load_dotenv(env_file_path)
            return dict(os.environ)
        except Exception as e:
            error_message = f"Error reading environment variables from '{env_file_path}': {e}"
            self.logger.info(error_message)
            raise ValueError(error_message) from e

    def create_folders(self, folders: List[str]) -> None:
        for folder in folders:
            if not os.path.exists(folder):
                os.makedirs(folder)
                self.logger.info(f"Created folder: {folder}")
        
    def get_file_path(self, file_name: str) -> str:
        file_path = os.path.join(self.project_root, file_name)
        if os.path.exists(file_path):
            return file_path
        else:
            error_message = f"{file_name} not found in project directory structure"
            self.logger.info(error_message)
            raise FileNotFoundError(error_message)
        
    def get_folder_path(self, folder_name: str, parent_folder: str = None) -> str:
        return os.path.join(self.project_root, parent_folder, folder_name) if parent_folder else os.path.join(self.project_root, folder_name)

    def get_env_variable(self, variable_name: str) -> str:
        variable_value = self.env_variables.get(variable_name)
        if not variable_value:
            error_message = f"Environment variable '{variable_name}' is not set in the .env file"
            self.logger.error(error_message)
            raise Exception(error_message)
        return variable_value
    
    def save_to_file(self, file_path: str, data: Any) -> None:
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f)
            self.logger.info(f"Saved to file: {file_path}")
        except (json.JSONDecodeError, FileNotFoundError) as e:
            error_message = f"Error {'decoding JSON from' if isinstance(e, json.JSONDecodeError) else 'loading from'} file '{file_path}': {e}"
            self.logger.error(error_message)
            raise ValueError(error_message) from e
        except Exception as e:
            error_message = f"Error saving to file '{file_path}': {e}"
            self.logger.error(error_message)
            raise Exception(error_message) from e

    def load_from_file(self, file_path: str) -> Any:
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError) as e:
            error_message = f"Error {'decoding JSON from' if isinstance(e, json.JSONDecodeError) else 'loading from'} file '{file_path}': {e}"
            self.logger.error(error_message)
            raise ValueError(error_message) from e
        except Exception as e:
            error_message = f"Error loading from file '{file_path}': {e}"
            self.logger.error(error_message)
            raise ValueError(error_message) from e