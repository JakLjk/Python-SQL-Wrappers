#!/usr/bin/env python3

import mysql.connector
from mysql.connector import Error
import pandas as pd
import time


class Database:
    """Class providing general sql handling methods,
    based on mysql module, 
    gives partial support for pandas dataframe"""
    def __init__(self, host_name:str, user:str, user_passwd:str, db_name:str):
        try:
            self.connection = mysql.connector.connect(
                host=host_name,
                user=user,
                passwd=user_passwd,
                database=db_name)
            self.cursor = self.connection.cursor()
        except Error as err:
            raise Error(f"There was a problem with initialising database: \n{err}")
        
    def execute_query(self, query):
        """General methond for  """
        try:
            self.cursor.execute(query)
        except Error as err:
            raise Error(err)

    def insert_into_table(self, 
                          table_name, 
                          column_data:dict):
        """!Requires separate use of commit method,
        in order to save changes to db.

        Column data takes unlimited amount of key:val pairs,
        where key corresponds to column in which we want to 
        append and val is the value which will be inserted
        """

        col_names_list = list(column_data.keys())
        col_names_list = [f"`{name}`" for name in col_names_list]
        columns_parsed = ",".join(col_names_list)
        values_list = list(column_data.values())
        values_parsed = ",".join([f"\"{val}\"" for val in values_list])

        if type(values_parsed) == tuple:
            self.cursor.execute(f"""
            INSERT INTO {table_name} ({columns_parsed}) 
            VALUES {values_parsed}""")
        else:
            self.cursor.execute(f"""
            INSERT INTO {table_name} ({columns_parsed}) 
            VALUES ({values_parsed})""")
        
    def fetch_from_table(self, 
                         table_name, what_columns='*', 
                         where={}):
        """Allows for receiving one, or multiple rows from specified
        table_name. What_columns argument can be changed, in order to
        return only specified columns.

        Argument where allows for filtering values that are to be
        returned. It takes dictionary with any amount of key:va pairs, 
        in which key is the column containing argument, and val is the 
        value we want to assert is true.
        """
        where_statement = self._dict_to_sql_statement(where)
        self.cursor.execute(f'''
        SELECT {what_columns} 
        FROM {table_name} 
        WHERE {where_statement}
        ''')
        values = self.cursor.fetchall()
        return values
    
    def fetch_from_table_to_df(self, 
                         table_name, what_columns='*', 
                         where={},
                         df_index_col=None):
        """Product of this function is Pandas dataframe.
        
        Allows for receiving one, or multiple rows from specified
        table_name. What_columns argument can be changed, in order to
        return only specified columns.

        Argument where allows for filtering values that are to be
        returned. It takes dictionary with any amount of key:va pairs, 
        in which key is the column containing argument, and val is the 
        value we want to assert is true.
        """
        where_statement = self._dict_to_sql_statement(where)
        dataframe = pd.read_sql(f"""
        SELECT {what_columns} 
        FROM {table_name} 
        WHERE {where_statement}
        """,
        con=self.connection, 
        index_col=df_index_col)
        if dataframe.empty: return None
        else: return dataframe

    # TODO add lowercase or exact string check
    def check_if_value_exists(self, 
                              table_name, 
                              where={}, 
                              only_one_row_exists=True) -> bool:
        """Allows for checking if one occurence or multiple occurences
        of specified value exists in database."""
        where_statement = self._dict_to_sql_statement(where)
        self.cursor.execute(f"""
        SELECT COUNT(1)
        FROM {table_name}
        WHERE {where_statement}
        """)
        found_values = self.cursor.fetchone()[0]
        if not found_values: 
            return None
        elif found_values > 1 and only_one_row_exists: 
            raise Exception('''
            More than one row found, 
            WHERE statement may be not precise enough, 
            or db structure may be compromised\n
            ''')
        elif found_values == 1: 
            return True
        else: raise Exception("Unexpected return value")

    def update_field_new_val(self, 
                      table_name, 
                      where={}, 
                      values_to_update={}, 
                      only_one_row_exists=True):
        """!Requires separate use of commit method,
        in order to save changes to db.

        values which may be updated are strings, ints or floats.
        Other data types and objects may not work properly
        """
        # checks if only one row exists, 
        # to prevent changing multiple rows in db, 
        # if such action is unwanted
        self.check_if_value_exists(
            table_name=table_name,
            where=where,
            only_one_row_exists=only_one_row_exists)
        
        set_statement = self._dict_to_sql_statement(values_to_update)
        where_statement = self._dict_to_sql_statement(where)

        self.cursor.execute(f"""
        UPDATE {table_name}
        SET {set_statement}
        WHERE {where_statement}
        """)

    def update_field_current_val(self, 
                                 table_name, 
                                 where={},
                                 values_to_change={}, 
                                 only_one_row_exists=True):
        
        """!Requires separate use of commit method,
        in order to save changes to db.
        
        Allows for updating value in database, by 
        performing an arithmetic operation on current value.

        values_to_change argument takes any amount of key:val 
        pairs, where key is column and val is value in this 
        column to be altered.

        [current column value] = [current column value] + specified_value
        """
        # checks if only one row exists, 
        # to prevent changing multiple rows in db, 
        # if such action is unwanted
        self.check_if_value_exists(
            table_name=table_name,
            where=where,
            only_one_row_exists=only_one_row_exists)
        
        set_statement = self._set_dictionary_add_to_value(values_to_change)
        where_Statement = self._dict_to_sql_statement(where)

        self.cursor.execute(f"""
        UPDATE {table_name}
        SET {set_statement}
        WHERE {where_Statement}
        """)

    def commit(self):
        """used to commit all current operations,
        waiting for such command."""
        self.connection.commit()

    def multiple_transactions(self, queries=[]):
        """used to manually pass multiple sql queries as list,
        which are commited together.

        In case of failure, script uses rollback in order not to 
        pass incomplete information.
        """
        try:
            for query in queries:
                self.cursor.execute(query)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Could not complete transaction: {e}")
        
    def get_sql_date(self, datetime=False):
        "Returns current date or datetime in proper sql format"
        if datetime:
            return time.strftime(('%Y-%m-%d %H:%M:%S'))
        else:
            return time.strftime(('%Y-%m-%d'))
        
    def _dict_to_sql_statement(self, dictionary={}):
        """Used mainly with passed where statement in dict form.
        Changes passed dict into proprer sql format."""
        if dictionary == {}: statement = '1 = 1'
        else:
            statement = " AND ".join(
                [f"{(k)} = '{str(v)}'" for k,v in dictionary.items()])
        return statement
    
    def _set_dictionary_add_to_value(self, dictionary):
        """Used mainly with passed SET statement in dict form.
        Changes passed dict into proprer sql format."""
        assert dictionary, "improper dictionary"
        statement = " AND ".join(
                [f"{str(k)} = {str(k)} + '{str(v)}'" for k,v in dictionary.items()])
        return statement

d =1