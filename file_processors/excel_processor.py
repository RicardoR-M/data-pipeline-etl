import pandas as pd
from .file_processor import FileProcessor


class ExcelProcessor(FileProcessor):

    def __init__(self, file_path, **kwargs):
        super().__init__(file_path, **kwargs)

    def _read(self) -> pd.DataFrame:
        return pd.read_excel(self.file, dtype=str, na_filter=False, sheet_name=self.sheet_name, skiprows=self.skip_rows)
