import logging

class Logger:
    """
    Класс для настройки логгера и его использования в других классах.

    Methods:
        configure_logger(): Настраивает логгер, если у него нет обработчиков.
    
    Attributes:
        logger (Logger): Логгер для записи сообщений.
    """
    logger = logging.getLogger(__name__)

    @staticmethod
    def configure_logger():
        if not Logger.logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            stdout_handler = logging.StreamHandler()
            stdout_handler.setLevel(logging.INFO)
            stdout_handler.setFormatter(formatter)
            Logger.logger.setLevel(logging.INFO)
            Logger.logger.addHandler(stdout_handler)
            Logger.logger.propagate = True

    def __init__(self):
        Logger.configure_logger()