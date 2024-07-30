import datetime
from abc import ABC, abstractmethod
from typing import Union
from os.path import join
from pathlib import Path

from babel.dates import format_datetime
from pendulum import now, timezone, today, parse, DateTime, Date, instance, date
from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext, Page


class Downloader(ABC):
    def __init__(self, **kwargs):
        """
        Initializes the Downloader object.

        kwargs: Arbitrary keyword arguments. Possible key-value pairs are:
            - 'root_download_dir': The root directory for downloads.
            - 'service': The service name.
            - 'sub_service': The sub-service name.
            - 'keep_original_name': A boolean indicating if the original name should be kept.
            - 'headless': A boolean indicating if the browser should be headless or not.
            - 'slow_mo': The slow motion value.
            - 'trace_pw': A boolean indicating if the Playwright tracing should be enabled.
            - 'fecha_ini': The initial date for the range of dates to download.
            - 'fecha_fin': The final date for the range of dates to download.
            - 'fecha_dias': The number of days to download.
            - 'fecha_threshold': The threshold day of the month to download.
        """
        self.kwargs = kwargs
        self.pw: Union[Playwright, None] = None
        self.browser: Union[Browser, None] = None
        self.context: Union[BrowserContext, None] = None
        self.page: Union[Page, None] = None
        self.root_download_dir = kwargs.get('root_download_dir', './data')
        self.service = kwargs.get('servicio', 'temp')
        self.sub_service = kwargs.get('sub_servicio', 'temp')
        self.downloader_name = kwargs.get('name', 'temp')
        self.headless = kwargs.get('headless', True)
        self.slow_mo = kwargs.get('slow_mo', 1500)
        self.trace_pw = kwargs.get('trace_pw', False)
        self.fecha_ini = kwargs.get('fecha_ini')
        self.fecha_fin = kwargs.get('fecha_fin')
        self.fecha_dias = kwargs.get('fecha_dias')
        self.fecha_threshold = kwargs.get('fecha_threshold')
        self.add_original_name = kwargs.get('add_original_name', False)
        self.add_downloader_name = kwargs.get('add_downloader_name', True)
        self.add_timestamp = kwargs.get('add_timestamp', True)
        self.add_full_timestamp = kwargs.get('add_full_timestamp', False)
        self.tz = timezone(kwargs.get('tz', 'America/Lima'))

    @abstractmethod
    def download(self) -> str:
        """
        Abstract method to download content.

        Must be implemented by child classes.
        """
        pass

    def start_pw(self):
        """
        Starts Playwright and initializes the browser, context, and page.

        - Launches the browser in headless mode if specified.
        - Enables tracing if specified.
        """
        self.pw = sync_playwright().start()
        self.browser = self.pw.chromium.launch(headless=self.headless, slow_mo=1000)
        self.context = self.browser.new_context()
        self.page = self.context.new_page()
        if self.trace_pw:
            self.context.tracing.start(screenshots=True, snapshots=True)

    def close_pw(self):
        """
        Closes Playwright and the browser context.

        - Stops tracing if enabled.
        - Closes the browser and context.
        """
        if self.browser:
            if self.trace_pw:
                self.context.tracing.stop(path=join(self.generate_folder_name(), 'trace/trace.zip'))
            self.context.close()
            self.browser.close()
            self.pw.stop()
            self.browser = None
            self.context = None
            self.page = None
            self.pw = None

    def generate_fullpath_name(self, extension: str, original_filename: str) -> Path:
        """
        Generates the full path name for the downloaded file.

        :param extension: The file extension.
        :param original_filename: The original file name.
        :return: The full path as a Path object.
        """
        file_name = ''
        date_format = ''
        if self.add_downloader_name:
            file_name = self.downloader_name + '_'

        if self.add_original_name:
            file_name = file_name + original_filename + '_'

        if self.add_timestamp:
            date_format = 'yMMdd_HHmm'
        if self.add_full_timestamp:
            date_format = 'yMMdd_HHmmss'

        if date_format:
            file_name = file_name + format_datetime(now(self.tz), format=date_format, locale='es_PE')

        if file_name == '':
            file_name = 'download'

        file_name = file_name + '.' + extension
        full_path = Path(self.generate_folder_name()) / file_name
        return full_path

    def generate_folder_name(self) -> Path:
        """
        Generates the folder name for the download path.

        :return: The folder path as a Path object.
        """
        full_path = Path(self.root_download_dir) / self.service / self.sub_service
        return full_path

    def generate_range_dates(self) -> tuple:
        """
        Generates a range of dates based on the provided parameters.

        :return: A tuple containing the start and end dates.
        """
        if self.fecha_ini and self.fecha_fin:
            return self.ensure_date(self.fecha_ini), self.ensure_date(self.fecha_fin)
        elif self.fecha_ini and not self.fecha_fin:
            return self.ensure_date(self.fecha_ini), today(self.tz)
        elif self.fecha_dias:
            return today(self.tz).subtract(days=self.fecha_dias), today(self.tz)
        elif self.fecha_threshold:
            if isinstance(self.fecha_threshold, int):
                if today(self.tz).day <= self.fecha_threshold:
                    return today(self.tz).subtract(months=1).start_of('month'), today(self.tz)
                else:
                    return today(self.tz).start_of('month'), today(self.tz)
            else:
                raise ValueError('fecha_threshold must be an integer')
        else:
            return today(self.tz).subtract(days=1), today(self.tz)

    @staticmethod
    def ensure_date(date_value: Union[str, datetime.date, DateTime, Date, None]) -> Union[date, None]:
        """
        Ensures the date value is converted to a Date object.

        :param date_value: The date value to convert.
        :return: The converted Date object or None.
        """
        if isinstance(date_value, str):
            try:
                return parse(date_value).date()
            except ValueError as e:
                print(f"Error parsing date string '{date_value}': {e}")
                raise
        elif isinstance(date_value, datetime.date):
            return Date(date_value.year, date_value.month, date_value.day)
        elif isinstance(date_value, DateTime):
            return instance(date_value).date()
        elif isinstance(date_value, Date):
            return date_value
        elif date_value is None:
            return None
        else:
            raise ValueError(f"Unexpected date type: {type(date_value)}")
