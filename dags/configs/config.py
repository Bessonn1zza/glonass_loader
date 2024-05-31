from typing import Any, Dict

from utils.managers import (
    create_folders,
    get_folder_path,
    get_env_variable, 
    get_file_path, 
    load_from_file
)

class ConfigLoader:
    def __init__(self, project_root=None) -> None:
        self.project_root = project_root

        self.metadata_path = self._get_variable_path('SETTINGS_DATA_FOLDER')
        self.secrets_path = self._get_variable_path('SETTINGS_SECRETS_FOLDER')
        create_folders([self.metadata_path, self.secrets_path])

        self.google_credentials_path = self._get_credentials_path('GOOGLE_APPLICATION_CREDENTIALS')
        self.postgres_credentials_path = self._get_credentials_path('POSTGRES_DEV_CREDENTIALS')

        self.google_credentials_data = self.load_credentials_data(self.google_credentials_path)
        self.postgres_credentials_data = self.load_credentials_data(self.postgres_credentials_path)

    def _get_variable_path(self, env_var: str) -> str:
        folder_name = get_env_variable(env_var)
        return get_file_path(folder_name)
    
    def _get_credentials_path(self, variable_name) -> str:
        credentials_file = get_env_variable(variable_name)
        credentials_path = get_folder_path(credentials_file, self.secrets_path)
        return get_file_path(credentials_path)
    
    def load_credentials_data(self, credentials_path) -> Dict[str, Any]:
        return load_from_file(credentials_path)