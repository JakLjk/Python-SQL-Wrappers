from abc import ABC, abstractmethod

class DBConnector(ABC):
    def __init__(self):
        self._connector = None
        self._cursor = None

    @abstractmethod
    def connect_to_db(self,
                    username: str, 
                    password: str,
                    host_name: str, 
                    database_name: str):
        """Method used to initialise connection to database"""
        pass
    
    @property
    def get_cursor(self):
        assert self._cursor is not None, \
            "Connector was not initialised properly.."
        return self._cursor
    

