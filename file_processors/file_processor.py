from os import getenv, path
from typing import List, Union
from unicodedata import normalize
from re import sub

import pandas as pd
from abc import ABC, abstractmethod

from rich.console import Console
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR


class FileProcessor(ABC):
    def __init__(self, file_path: str, **kwargs):
        """
        Initializes the FileProcessor object.

        - df: The data frame to be processed. Must be set by the child class.
        - skip_rows: Number of rows to skip.
        - sheet_name: The sheet name to read from an Excel file.
        - separator: The separator character for a CSV file.
        - encoding: The encoding of the file.

        Available Cleaning Steps:

        - parse_sinona: Replaces 'Sí', 'No', 'No aplica', 'Si' with 'SI', 'NO', 'NA', 'SI'.
        - remove_empty_rows: Removes rows that are all NA.
        - empty_asnull: Replaces empty strings with pd.NA.
        - replace_values: Replaces old values with new values in the specified columns.
            - old_values: The old values to replace.
            - new_values: The new values to replace with.
            - columns: The columns to replace values in.
        - trim_column_names: Trims the column names.
        - truncate_column_names: Truncates the column names to a specified length.
            - length: The length to truncate the column names to.
        - remove_specialchars_from_column_names: Removes special characters from the column names.
        - normalize_column_names: Normalizes the column names.
        - remove_duplicate_rows: Removes duplicate rows.

        :param file_path: Path to the file or folder to be processed.
        :param kwargs: Arbitrary keyword arguments.
        """
        self.file_path: str = file_path
        self.file: Union[str, None] = None
        self.df: Union[pd.DataFrame, None] = None
        self.kwargs = kwargs
        self.skip_rows = kwargs.get('skip_rows', 0)
        self.sheet_name = kwargs.get('sheet_name', 0)
        self.separator = kwargs.get('separator', ',')
        self.encoding = kwargs.get('encoding', 'utf8')
        self.cleaning_steps = kwargs.get('cleaning', [])
        self.console = Console()

    def read(self):
        """
        Reads the file(s) specified in the file_path and applies the cleaning steps.

        - Reads the file(s) into a DataFrame.
        - Applies the specified cleaning steps.
        - Concatenates the DataFrames if multiple files are read.
        """
        df_list = []
        if isinstance(self.file_path, str):
            self.file_path = [self.file_path]

        for file in self.file_path:
            self.file = file
            self.df = self._read()
            self.apply_cleaning_steps()
            df_list.append(self.df)

        self.df = pd.concat(df_list)

    def apply_cleaning_steps(self):
        """
        Applies the cleaning steps specified in the cleaning_steps attribute.

        - Iterates over the cleaning steps and executes each one.
        """
        for step in self.cleaning_steps:
            if isinstance(step, dict):
                for method_name, params in step.items():
                    self.execute_cleaning_step(method_name, params)
            else:
                self.execute_cleaning_step(step)

    def execute_cleaning_step(self, method_name, params=None):
        """
        Executes a single cleaning step.

        :param method_name: The name of the cleaning method to execute.
        :param params: The parameters to pass to the cleaning method.
        """
        method = getattr(self, method_name, None)
        if method:
            if params:
                method(**params)
            else:
                method()
        else:
            raise ValueError(f'Unsupported cleaning step: {method_name}')

    @abstractmethod
    def _read(self) -> pd.DataFrame:
        """
        Abstract method to read the file into a DataFrame.

        Must be implemented by child classes.
        """
        pass

    def upload_to_db(self, db_config: dict):
        """
        Uploads the data frame to a database.

        :param db_config: A dictionary containing database configuration.

        - 'database': The database name.
        - 'table': The table name.
        - 'if_exists': The action to take if the table already exists. Possible values are 'replace', 'append', 'fail'.
        - 'schema': The schema name.
        - 'index': Whether to write the DataFrame index as a column. Default is False.
        - 'varchar_size': The size of the VARCHAR columns. Default is 2500.
        - 'SQL_ENGINE_STRING': environment variable containing the engine string.
        """
        if self.df is not None:
            engine_string = getenv('SQL_ENGINE_STRING')
            if engine_string is None:
                raise ValueError('SQL_ENGINE_STRING must be provided')

            database: Union[str, None] = db_config.get('database', None)
            if database is None:
                raise ValueError('database must be provided')

            table: Union[str, None] = db_config.get('table', None)
            if table is None:
                raise ValueError('table_name must be provided')

            if_exists: str = db_config.get('if_exists', 'replace').lower()
            schema: str = db_config.get('schema', 'dbo')
            engine = create_engine(engine_string + database)
            index = db_config.get('index', False)
            varchar_size = db_config.get('varchar_size', 2500)

            # noinspection PyTypeChecker
            self.df.to_sql(name=table, con=engine, schema=schema, if_exists=if_exists, index=index,
                           dtype={col_name: VARCHAR(varchar_size) for col_name in self.df.columns})

    @staticmethod
    def execute_sql(db_config: dict):
        """
        Executes an SQL command.

        :param db_config: A dictionary containing database configuration.

        - 'database': The database name.
        - 'sql_file': The name of the SQL file to execute, stored in the folder querys.
        - 'SQL_ENGINE_STRING': environment variable containing the engine string.
        """
        engine_string = getenv('SQL_ENGINE_STRING')
        if engine_string is None:
            raise ValueError('SQL_ENGINE_STRING must be provided')

        database: Union[str, None] = db_config.get('database', None)
        if database is None:
            raise ValueError('database must be provided')

        sql_files: Union[str, List[str]] = db_config.get('sql_file', [])
        if isinstance(sql_files, str):
            sql_files = [sql_files]

        for sql in sql_files:
            # Ensure the SQL file exists
            sql = path.basename(sql)
            if not path.isfile(f'querys/{sql}'):
                raise ValueError(f'SQL file {sql} does not exist')

            # Read the SQL file
            with open(f'querys/{sql}', 'r') as file:
                sql = file.read()

            engine = create_engine(engine_string + database)

            with engine.begin() as connection:
                connection.execute(sql)

        sql_querys: Union[str, List[str]] = db_config.get('sql_query', [])
        if isinstance(sql_querys, str):
            sql_querys = [sql_querys]

        for sql in sql_querys:
            engine = create_engine(engine_string + database)
            with engine.begin() as connection:
                connection.execute(sql)

    def parse_sinona(self):
        """
        Replaces 'Sí', 'No', 'No aplica', 'Si', 'N.A.' with 'SI', 'NO', 'NA', 'SI', 'NA' in the DataFrame.
        """
        if self.df is not None:
            self.df.replace(['Sí', 'No', 'No aplica', 'Si', 'N.A.'], ['SI', 'NO', 'NA', 'SI', 'NA'], inplace=True)

    def remove_empty_rows(self):
        """
        Removes rows that are empty from the DataFrame.
        """
        if self.df is not None:
            self.df.dropna(how='all', inplace=True)
            self.df = self.df[self.df.any(axis=1)]

    def empty_asnull(self):
        """
        Replaces empty strings with pd.NA in the DataFrame.
        """
        if self.df is not None:
            self.df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)

    def replace_values(self, old_values: List, new_values: List, columns: Union[str, List[str]]):
        """
        Replaces old values with new values in the specified columns.

        :param old_values: The old values to replace.
        :param new_values: The new values to replace with.
        :param columns: The columns to replace values in.
        """
        if self.df is not None:
            if isinstance(columns, str):
                columns = [columns]

            for column in columns:
                if column in self.df.columns:
                    self.df[column].replace(old_values, new_values, inplace=True)

    def trim_column_names(self):
        """
        Trims the column names in the DataFrame.
        """
        if self.df is not None:
            self.df.columns = self.df.columns.map(
                lambda col: ' '.join(col.strip().replace('\r', '').replace('\n', '').split()))

    def trim_column_values(self, columns: Union[str, List[str]]):
        """
        Trims the values in the specified columns.

        :param columns: The columns to trim values in.
        """
        if self.df is not None:
            if isinstance(columns, str):
                columns = [columns]

            for column in columns:
                if column in self.df.columns:
                    self.df[column] = self.df[column].str.strip()

    def trim_all_values(self):
        """
        Trims all string values in the DataFrame.
        """
        if self.df is not None:
            self.df = self.df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    def truncate_column_names(self, length: int):
        """
        Truncates the column names to a specified length.

        :param length: The length to truncate the column names to.
        """
        if self.df is not None:
            self.df.columns = self.df.columns.map(lambda col: col[:length])

    def remove_specialchars_from_column_names(self):
        """
        Removes special characters from the column names in the DataFrame.
        """
        if self.df is not None:
            self.df.columns = self.df.columns.str.replace('[¿?#,@&“"”/()]', '', regex=True)

    def ignore_columns(self, columns: Union[List[str], str]):
        """
        Drops the specified columns from the DataFrame.

        :param columns: The columns to drop.
        """
        if self.df is not None:
            if isinstance(columns, str):
                columns = [columns]

            # if column doesnt exists in df ignore
            columns = [col for col in columns if col in self.df.columns]

            self.df.drop(columns=columns, inplace=True)

    def filter_columns(self, columns: Union[List[str], str]):
        """
        Filters the DataFrame to only include the specified columns.

        :param columns: The columns to include.
        """
        if self.df is not None:
            if isinstance(columns, str):
                columns = [columns]

            # if column doesnt exists in df ignore
            columns = [col for col in columns if col in self.df.columns]

            self.df = self.df[columns]

    def only_numbers_columns(self, columns: Union[List[str], str]):
        """
        Extracts only numeric values from the specified columns.

        :param columns: The columns to extract numeric values from.
        """
        if self.df is not None:
            if isinstance(columns, str):
                columns = [columns]

            for column in columns:
                if column in self.df.columns:
                    self.df[column] = self.df[column].str.extract(r'(\d+)')

    def normalize_column_names(self):
        """
        Normalizes the column names in the DataFrame.
        """
        if self.df is not None:
            self.df.columns = [self._normalize_name(col) for col in self.df.columns]

    @staticmethod
    def _normalize_name(txt: str) -> str:
        """
        Normalizes a string by removing special characters and converting to uppercase.

        :param txt: The string to normalize.
        :return: The normalized string.
        """
        txt = str(txt).strip()
        txt = normalize('NFD', txt).encode('ascii', 'ignore').decode()
        txt = sub('[^0-9a-zA-Z&_]+', ' ', txt)
        txt = sub(' +', ' ', txt)
        txt = txt.strip().upper().replace(" ", '_')
        return txt

    def remove_duplicate_rows(self):
        """
        Removes duplicate rows from the DataFrame.
        """
        if self.df is not None:
            self.df.drop_duplicates(inplace=True)
