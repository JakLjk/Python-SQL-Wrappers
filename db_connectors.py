import mysql.connector
import mariadb

from _abstract_db_connector import DBConnector


class MariaDBConnector(DBConnector):
    mariadb_connector = mariadb

    def __init__(self):
        super().__init__()
        self._connector = None
        self._cursor = None

    def connect_to_db(self,
                    username: str, 
                    password: str,
                    host_name: str, 
                    database_name: str):
        
        self._connector = self.mariadb_connector.connect(
                host=host_name,
                user=username,
                passwd=password,
                database=database_name)
        self._connector.autocommit = False
        self._cursor = self.connection.cursor()


class MySqlConnector(DBConnector):
    mysql_connector =  mysql.connector

    def __init__(self):
        super().__init__()
        self._connector = None
        self._cursor = None

    def connect_to_db(self,
                    username: str, 
                    password: str,
                    host_name: str, 
                    database_name: str):
        
        self._connector = self.mysql_connector.connect(
                host=host_name,
                user=username,
                passwd=password,
                database=database_name)
        self._cursor = self._connector.cursor()
