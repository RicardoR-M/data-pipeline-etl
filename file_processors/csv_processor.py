import pandas as pd
from .file_processor import FileProcessor


class CSVProcessor(FileProcessor):
    def __init__(self, file_path, **kwargs):
        super().__init__(file_path, **kwargs)

    def _read(self) -> pd.DataFrame:
        return pd.read_csv(self.file, dtype=str, na_filter=False, encoding=self.encoding, sep=self.separator, skiprows=self.skip_rows)
