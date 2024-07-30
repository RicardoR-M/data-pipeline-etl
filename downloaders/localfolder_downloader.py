from os import makedirs, path
from pathlib import Path
from shutil import copy2
from typing import List

from downloaders.downloader import Downloader


class LocalFolderDownloader(Downloader):
    def __init__(self, local_fullpath: str, **kwargs):
        super().__init__(**kwargs)
        self.local_folderpath = local_fullpath

    def download(self) -> List[str]:
        if not Path(self.local_folderpath).is_dir():
            raise FileNotFoundError(f'Folder not found: {self.local_folderpath}')

        new_paths = []
        for file in Path(self.local_folderpath).iterdir():
            if file.is_file():
                # get file extension
                file_extension = file.suffix[1:]
                original_filename = file.stem
                new_path = self.generate_fullpath_name(file_extension, original_filename)

                # Ensure the directory exists
                makedirs(path.dirname(new_path), exist_ok=True)

                # copy from local_folderpath to new_path
                copy2(str(file), new_path)
                new_paths.append(str(new_path))

        return new_paths