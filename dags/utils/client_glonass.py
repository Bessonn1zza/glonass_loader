import logging
import time
from typing import Any, Dict

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from urllib3.util.retry import Retry
from ratelimiter import RateLimiter

from utils.loggers import Logger
from utils.exceptions import AuthenticationError

session_manager = None

def initialize_session_manager(credentials: Dict[str, Any]):
    global session_manager
    if not session_manager:
        session_manager = GlonassSessionManager(credentials)
    return session_manager

def execute_request(credentials, operation, module, args):
    session_manager = initialize_session_manager(credentials)
    executor = GlonassRequestExecutor(session_manager)
    return executor.execute_request(operation, module, args)


class BaseClient:
    FORCELIST = (500, 502, 503, 504, 429)
    MAX_RETRIES = 10
    BACKOFF_FACTOR = 0.5
    MAX_CALLS = 160
    PERIOD = 60

    def __init__(self):
        self.adapter = self.create_http_adapter()
        self.session = self.create_session()

    @classmethod
    def create_http_adapter(cls):
        retry_strategy = Retry(
            total=cls.MAX_RETRIES,
            read=cls.MAX_RETRIES,
            connect=cls.MAX_RETRIES,
            status=cls.MAX_RETRIES,
            backoff_factor=cls.BACKOFF_FACTOR,
            status_forcelist=cls.FORCELIST,
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        return adapter

    @RateLimiter(max_calls=MAX_CALLS, period=PERIOD)
    def create_session(self):
        session = requests.Session()
        session.mount('http://', self.adapter)
        session.mount('https://', self.adapter)
        return session
        

class GlonassSessionManager(BaseClient):

    def __init__(self, credentials: Dict[str, Any], logger: logging.Logger=None):
        super().__init__()
        self.log = logger or Logger().logger
        self.credentials = credentials
        self.session_id = None
        self.authenticate()
    
    def _build_url(self, module, args):
        return "http://{}:{}/{}?{}&out=json&nogzip&noxmldecl".format(
            self.credentials['host'], self.credentials['port'], module, args
        )
    
    def _perform_request(self, method, url, params=None, data=None):
        try:
            response = self.session.request(method, url, params=params, data=data)
            if response.status_code == 401:
                self.log.warning("Authentication required. Re-authenticating...")
                self.authenticate()
                response = self.session.request(method, url, params=params, data=data)
            response.raise_for_status()
            print(response.json())
            return response.json()
        except (HTTPError, requests.RequestException, ValueError) as e:
            error_message = f"An error occurred while executing request to {url}: {e}"
            self.log.error(error_message, exc_info=True)
            raise

    def authenticate(self):
        module = "main"
        login = self.credentials['login']
        password = self.credentials['password']
        args = f"func=auth&login={login}&password={password}"
        url = self._build_url(module, args)
        
        try:
            data = self._perform_request('GET', url)
            session_id = data.get('data')
            if session_id:
                self.log.info(f"Authentication successful. Session ID: {session_id}")
                self.session_id = session_id
            else:
                self.log.error("Authentication failed. Invalid response format.")
                raise AuthenticationError
        except AuthenticationError:
            self.log.error("Authentication failed.")
            raise


class GlonassRequestExecutor:
    WAIT_TIME_INITIAL = 10
    WAIT_TIME_MULTIPLIER = 1.5
    MAX_ATTEMPTS = 20

    def __init__(self, session_manager: GlonassSessionManager, logger: logging.Logger=None):
        self.log = logger or Logger().logger
        self.session_manager = session_manager

    def execute_request(self, operation, module, args):
        self.log.info(f"Loading from module '{module}', args '{args}' started.")
        operation_mapping = {
            "sync": self.execute_sync_request,
            "async": self.execute_async_request
        }
        execute_method = operation_mapping.get(operation)
        if execute_method is None:
            error_message = f"Invalid operation: {operation}."
            self.log.error(error_message)
            raise ValueError(error_message)
        return execute_method(module, args)

    def execute_sync_request(self, module, args):
        response_data = self._perform_request(module, args)
        self.log.info(f"Loading from module '{module}', args '{args}' successfully completed.")
        return response_data

    def execute_async_request(self, module, args):
        args += "&async"
        request_id = self._perform_request(module, args).get('data')
        self.log.info(f"Async request successful. Request ID: {request_id}")  

        attempt_count = 0
        while attempt_count < self.MAX_ATTEMPTS:
            attempt_count += 1
            time.sleep(self.WAIT_TIME_INITIAL * (self.WAIT_TIME_MULTIPLIER ** (attempt_count - 1)))
            response_data = self._perform_request("main", f"func=asyncresult&id={request_id}")

            request_info = f"Async request with ID {request_id}"
            result = response_data.get('result', '')

            if result == 'not complete':
                self.log.info(f"{request_info} is still in progress. Attempt {attempt_count}. Progress: {response_data.get('data', 'N/A')}")
            elif result == 'not found':
                error_message = f"{request_info} not found."
                self.log.error(error_message)
                raise Exception(error_message)
            elif result == 'ERROR':
                error_message = f"{request_info} failed. Error: {response_data.get('data')}"
                self.log.error(error_message)
                raise Exception(error_message)
            else:
                self.log.info(f"Loading from module '{module}', args '{args}' successfully completed.")
                return response_data

        self.log.warning(f"{request_info} reached maximum attempts ({self.MAX_ATTEMPTS}). Last result: {result}")
        return response_data

    def _perform_request(self, module, args):
        url = self.session_manager._build_url(module, f"{args}&sess={self.session_manager.session_id}")
        response_data = self.session_manager._perform_request('GET', url)
            
        if response_data.get('result', '') == 'ERROR':
            error_message = f"Request failed. Error: {response_data['data']}"
            self.log.error(error_message)
            raise Exception(error_message)
            
        return response_data
    