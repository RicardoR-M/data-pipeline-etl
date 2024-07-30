import pandas as pd
from file_processors.file_processor import FileProcessor


class HTMLTableProcessor(FileProcessor):

    def __init__(self, file_path, **kwargs):
        super().__init__(file_path, **kwargs)

    def _read(self) -> pd.DataFrame:
        try:
            df_t = pd.read_html(self.file, flavor='bs4', encoding=self.encoding)[0].columns
            return pd.read_html(self.file, flavor='bs4', encoding=self.encoding, keep_default_na=False,
                                skiprows=self.skip_rows,
                                converters={col_name: str for col_name in df_t})[0]
        except ValueError as e:
            if str(e) == 'No tables found':
                print('Alerta:', str(e))
            else:
                print(f'Error reading HTML file: {e}')
            self.df = None
