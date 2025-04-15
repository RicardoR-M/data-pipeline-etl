from typing import Union

import pandas as pd
import numpy as np
import datetime

from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR

from file_processors.file_processor import FileProcessor
from dateutil.parser import parse
from os import path, getenv
from openpyxl.utils.datetime import from_excel


class CustomFormacionConsolidadoProcessor(FileProcessor):
    def __init__(self, file_path, **kwargs):
        super().__init__(file_path, **kwargs)
        self.df_rampas_bd = pd.DataFrame()
        self.df_total_asistencia = pd.DataFrame()
        self.df_total_rampas = pd.DataFrame()

    @staticmethod
    def parse_date_excel(x):
        if pd.isna(x) or x is None or x == 0:
            return None
        if str(x).strip() == '-' or str(x).strip() == '0':
            return None
        # custom case hasta que se corrija en el archivo
        if str(x).strip() == '15/-4':
            return parse('2024-04-15').date()
        try:
            return parse(str(x)).date()
        except ValueError:
            try:
                dt = from_excel(int(x))
                return dt.date() if isinstance(dt, datetime.datetime) else dt
            except (ValueError, TypeError, OverflowError):
                raise ValueError(f'No se pudo parsear fecha: {x}')

    @staticmethod
    def clean_value(x):
        if pd.notnull(x):
            if np.isreal(x):
                return str(int(x)).strip()
            else:
                return str(x).strip()
        else:
            return None

    @staticmethod
    def replace_empty_and_hyphen(df):
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        df.replace(r'^\s*-\s*$', np.nan, regex=True, inplace=True)
        return df

    def carga_hoja_db(self):
        # Carga de datos de la hoja BD
        self.df_rampas_bd = self.replace_empty_and_hyphen(self.df_rampas_bd)
        self.df_rampas_bd.dropna(subset=['ID PEOPLE', 'GERENCIA'], axis=0, inplace=True)
        self.df_rampas_bd[['ID PEOPLE', 'GERENCIA', 'CAMPAÑA']] = self.df_rampas_bd[
            ['ID PEOPLE', 'GERENCIA', 'CAMPAÑA']].applymap(
            lambda x: x.strip() if isinstance(x, str) else x)

        lista_fechas = ['INICIO DE CAPACITACION', 'INICIO DE OJT', 'FIN DE OJT', 'FECHA FIRMA (SIN EXTENSION)',
                        'FECHA FIRMA (CON EXTENSION)', 'FECHA - FIRMA CONTRATO', 'FECHA ENTREGA - OPERACIÓN',
                        'FECHA DE PAGO DE CAPA', 'ULT. ASISTENCIA']
        self.df_rampas_bd[lista_fechas] = self.df_rampas_bd[lista_fechas].applymap(self.parse_date_excel)
        self.df_rampas_bd['FILE'] = path.basename(self.file_path)

    def read_excel_sheet(self, rampa):
        try:
            return pd.read_excel(self.file_path, sheet_name=rampa, engine='pyxlsb')
        except ValueError as e:
            self.console.log(e)
            return None

    def process_dataframe(self, df, row_num, column_num):
        df = df.iloc[row_num:, column_num:]
        df.columns = df.iloc[0]
        df = df[1:]
        df = self.replace_empty_and_hyphen(df)
        df.dropna(subset=['DNI'], axis=0, inplace=True)
        return df if len(df) > 0 else None

    def process_asistencia(self, df, rampa):
        tipo_ing_col = df.columns.get_loc('TIPO INGRESO')
        df_asistencia_fechas: pd.DataFrame = df.iloc[:, tipo_ing_col + 1: tipo_ing_col + 42].copy()
        df = df.drop(df.columns[tipo_ing_col + 1: tipo_ing_col + 42], axis=1)
        df_asistencia_fechas = df_asistencia_fechas.dropna(axis=1, how='all')

        target_columns = ['DNI', 'TELÉFONO', 'TIPO INGRESO']
        # Find which of these columns ACTUALLY EXIST in the DataFrame
        cols_to_clean = [col for col in target_columns if col in df.columns]

        if cols_to_clean:
            df[cols_to_clean] = df[cols_to_clean].applymap(self.clean_value)

        columnas = ['NRO. RESUMEN', 'DNI', 'NOMBRES']

        df_fechas_identidad = df[columnas].copy()

        df_asistencia = pd.concat([df_fechas_identidad, df_asistencia_fechas], axis=1)
        df_asistencia['RAMPA'] = rampa.strip()
        columnas.append('RAMPA')

        df_asistencia = df_asistencia.melt(id_vars=columnas, var_name='FECHA', value_name='ASISTENCIA')
        df_asistencia['DIA_NRO'] = df_asistencia.groupby('FECHA').ngroup() + 1

        try:
            df_asistencia['FECHA'] = df_asistencia['FECHA'].apply(self.parse_date_excel)
        except ValueError as e:
            self.console.log(f'Asistencia - Error de fechas Rampa: {rampa}, e: {e}')
            raise e

        df_asistencia['ASISTENCIA'] = df_asistencia['ASISTENCIA'].apply(
            lambda x: str(x).strip() if pd.notnull(x) else x)

        df_asistencia['FILE'] = path.basename(self.file_path)
        self.df_total_asistencia = self.df_total_asistencia.append(df_asistencia)

        lista_fechas = ['Fecha OJT', 'Fecha Baja', 'Fecha Firma', 'Fecha Entregado']
        try:
            df[lista_fechas] = df[lista_fechas].applymap(self.parse_date_excel)
        except ValueError as e:
            self.console.log(f'Rampas BD - Error de fechas Rampa: {rampa}, e: {e}')
            raise e

        df['RAMPA'] = rampa
        df['FILE'] = path.basename(self.file_path)

        self.df_total_rampas = self.df_total_rampas.append(df)

    def carga_asistencia_detalle(self):
        for rampa in self.df_rampas_bd['ID PEOPLE']:
            df = self.read_excel_sheet(rampa)
            if df is None:
                continue

            row_num, column_num = np.nonzero(df.values == 'NRO. RESUMEN')

            if row_num.size == 0:
                self.console.log(f'No se encontró la columna "NRO. RESUMEN" - Rampa: {rampa}')
                continue

            df = df.iloc[row_num[0]:, column_num[0]:]
            df.columns = df.iloc[0]

            for column in df.columns:
                if str(column).strip() == '-' or column is np.NAN:
                    self.console.log(f'Se detectaron columnas no válidas "-" Rampa: {rampa}')

            df = df[1:]
            df = self.replace_empty_and_hyphen(df)
            df.dropna(subset=['DNI'], axis=0, inplace=True)

            if len(df) == 0:
                self.console.log(f'Rampa {rampa} no tiene datos.')
                continue

            if 'TIPO INGRESO' not in df.columns:
                self.console.log(f'No se encontró la columna "TIPO INGRESO" - Rampa: {rampa}')
                continue

            self.process_asistencia(df, rampa)

    def read(self):
        self._read()

    def _read(self) -> None:
        self.df_rampas_bd = pd.read_excel(self.file_path, sheet_name='BD', engine='pyxlsb', skiprows=8, usecols='B:EC',
                                          header=0,
                                          dtype=str)
        self.carga_hoja_db()
        self.carga_asistencia_detalle()

    def upload_to_db(self, db_config: dict):
        engine_string = getenv('SQL_ENGINE_STRING')
        if engine_string is None:
            raise ValueError('SQL_ENGINE_STRING must be provided')

        database: Union[str, None] = db_config.get('database', None)
        if database is None:
            raise ValueError('database must be provided')

        table_rampas_bd: Union[str, None] = db_config.get('table_rampas_bd', None)
        if table_rampas_bd is None:
            raise ValueError('table_rampas_bd must be provided')

        table_rampas_asistencia: Union[str, None] = db_config.get('table_rampas_asistencia', None)
        if table_rampas_asistencia is None:
            raise ValueError('table_rampas_asistencia must be provided')

        table_rampas_detalle: Union[str, None] = db_config.get('table_rampas_detalle', None)
        if table_rampas_detalle is None:
            raise ValueError('table_rampas_detalle must be provided')

        if_exists: str = db_config.get('if_exists', 'replace').lower()
        schema: str = db_config.get('schema', 'dbo')
        engine = create_engine(engine_string + database)
        index = db_config.get('index', False)
        varchar_size = db_config.get('varchar_size', 2500)

        self.df_rampas_bd.to_sql(name=table_rampas_bd, con=engine, schema=schema, if_exists=if_exists, index=index,
                                 dtype={col_name: VARCHAR(varchar_size) for col_name in self.df_rampas_bd.columns})

        self.df_total_asistencia.to_sql(name=table_rampas_asistencia, con=engine, schema=schema, if_exists=if_exists,
                                        index=index,
                                        dtype={col_name: VARCHAR(varchar_size) for col_name in
                                               self.df_total_asistencia.columns})

        self.df_total_rampas.to_sql(name=table_rampas_detalle, con=engine, schema=schema, if_exists=if_exists,
                                    index=index,
                                    dtype={col_name: VARCHAR(varchar_size) for col_name in
                                           self.df_total_rampas.columns})
