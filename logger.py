"""
    Imports:
        os: used for interact with file system
        logging: basic lib for work with logging
        settings from base_class: stores used consts for class Logger
"""
import os
import logging
from typing import Union
from settings import LOG_FOLDER, LOG_LVL, SH_LVL, FH_LVL


class Logger:
    """Class Logger is used for logging events and exceptions

    Methods:
        create_logger(self, path='!loggers'):
            Used for creating logger.
            Takes the folder name or path to the folder to store log files.
        add_log(self, text, level='DEBUG'):
            Used for adding the message to the log-file by level.
        create_path(path):
            Used for creating folder (if it doesn't exist) to store log-files
            (folder '!loggers' as default)
    """

    def __init__(self, prefix: str):
        """Function of init for class

        Attributes:
            self._prefix: prefix to name the logger
            self._logger: name of logger (default value: None)
            self._filehandler: handler of log-file (default value: None)
            self._streamhandler: handler of stream (default value: None)
            self._path_to_handler: Path for file with logs (default value: None)

        Parameters:
            prefix: Name of parser
        """
        # prefix to name the loggers
        self.__prefix = prefix
        # logger
        self._logger = None
        # logger handlers
        self._filehandler = None
        self._streamhandler = None
        # path to log-handler
        self._path_to_handler = None
        # create_logger
        self.create_logger()

    @property
    def prefix(self):
        """Getter method for prefix"""
        return self.__prefix

    @prefix.setter
    def prefix(self, prefix: str):
        """Setter method for prefix

        Parameters:
            prefix: name of logger
        """
        self.__prefix = prefix

    def create_logger(self):
        """Creates logger for parser. Takes the folder name or path to the folder to store
        """

        # creating full path of log-file (path+file_name)
        os.makedirs(LOG_FOLDER, exist_ok=True)
        file = f'{self.prefix}.log'
        full_name = os.path.join(LOG_FOLDER, file)

        # creating new logger
        self._logger = logging.getLogger(f'{self.prefix}_logger')
        # set the logger level, messages which are less severe than level will be ignored
        self._logger.setLevel(LOG_LVL)
        #  file handler
        self._filehandler = logging.FileHandler(full_name, 'a')
        self._filehandler.setLevel(FH_LVL)
        #  stream handler
        self._streamhandler = logging.StreamHandler()
        self._streamhandler.setLevel(SH_LVL)
        # save path to handler in var
        self._path_to_handler = self._filehandler.baseFilename
        # set format of messages
        self._filehandler.setFormatter(
            logging.Formatter('%(asctime)s: %(levelname)s: %(name)s: %(message)s',
                              "%Y-%m-%d %H:%M:%S"))
        # add handlers to logger
        self._logger.addHandler(self._filehandler)
        self._logger.addHandler(self._streamhandler)

    def add_log(self, text: str, level: Union[str, int] = 'DEBUG'):
        """Adds the message to the log-file by level (DEBUG-EXCEPTION):
        int(1-Debug, 2-Info etc.) or str(Warning, Error, etc.) - case-insensitive

        Parameters:
            text: text of event
            level: importance level of event (default value: "Debug")
        Raises:
            Exception: if log-level doesn't exist
        """
        # TODO (uncomment / comment) to (print / don't print) ALL logs in console
        print(f"[{self.prefix}] {text}")
        # converting to uppercase
        if isinstance(level, str):
            level = level.upper()
        # for DEBUG level
        if level in ['DEBUG', 1, '1']:
            self._logger.debug(text)
        elif level in ['INFO', 2, '2']:
            self._logger.info(text)
        elif level in ['WARNING', 3, '3']:
            self._logger.warning(text)
        elif level in ['ERROR', 4, '4']:
            self._logger.error(text)
        elif level in ['CRITICAL', 5, '5']:
            self._logger.critical(text)
        elif level in ['EXCEPTION', 6, '6']:
            self._logger.exception(text)
        # for wrong level
        else:
            raise Exception(f"Level '{level}' for logger doesn't exist")
