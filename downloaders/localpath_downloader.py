from os import makedirs, path
from pathlib import Path
from shutil import copy2

from downloaders.downloader import Downloader


class LocalPathDownloader(Downloader):
    def __init__(self, local_fullpath: str, **kwargs):
        super().__init__(**kwargs)
        self.local_fullpath = local_fullpath

    def download(self) -> str:
        if not Path(self.local_fullpath).exists():
            raise FileNotFoundError(f'File not found: {self.local_fullpath}')

        # get local_fullpath file extension
        file_extension = Path(self.local_fullpath).suffix[1:]
        file_name = Path(self.local_fullpath).stem
        new_path = self.generate_fullpath_name(file_extension, file_name)

        # Ensure the directory exists
        makedirs(path.dirname(new_path), exist_ok=True)

        # copy from local_fullpath to new_path
        copy2(self.local_fullpath, new_path)
        return str(new_path)
