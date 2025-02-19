import glob
import os
import timeit
import re

import yaml

from downloaders.feedback_intranet_downloader import FeedbackIntranetDownloader
from downloaders.internaldash_downloader import InternalDashDownloader
from downloaders.localfolder_downloader import LocalFolderDownloader
from downloaders.localpath_downloader import LocalPathDownloader
from downloaders.qualtrics_downloader import QualtricsDownloader
from downloaders.qualtrics_survey_data_downloader import QualtricsSurveyDataDownloader
from file_processors.csv_processor import CSVProcessor
from file_processors.customFormacionConsolidado_processor import CustomFormacionConsolidadoProcessor
from file_processors.excel_processor import ExcelProcessor
from rich.console import Console
from os.path import basename


class Pipeline:
    """
    Initializes the Pipeline object.

    :param config_dir: The directory containing the configuration files.
    """

    def __init__(self, config_dir):
        self.config_dir = config_dir
        self.console = Console()

    def run(self):
        """
        Runs the pipeline process.

        - Loads configuration files.
        - Processes each service defined in the configuration files.
        - Logs the total time taken, number of reports processed, and number of errors.
        """
        root_dir = os.path.dirname(os.path.abspath(__file__))
        log_folder = os.path.join(root_dir, 'logs')
        os.makedirs(log_folder, exist_ok=True)

        self.console.log('[bold green]Importing config files...')
        config_files = self.load_config_files()

        total_time = timeit.default_timer()
        q_errors = 0
        q_reportes = 0
        reportes_error = []

        for config_file in config_files:
            with open(config_file, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)

            for service in config:
                if service['enabled']:
                    try:
                        start_time = timeit.default_timer()
                        self._process_report(service)
                        elapsed_time = timeit.default_timer() - start_time
                        self.console.log(
                            f"[bold green]{service['servicio']} - {service['sub_servicio']}: Processed in {elapsed_time:.2f} seconds",
                            style='green')
                        q_reportes += 1
                    except Exception as e:
                        q_errors += 1
                        reportes_error.append(f"{service['servicio']} - {service['sub_servicio']}")
                        self.console.log(
                            f"Error processing {service['servicio']} - {service['sub_servicio']}: {e}", style='red')
                        # save trace stack to file in the log folder
                        log_file = os.path.join(log_folder, f'traceback_{service["servicio"]}_{service["sub_servicio"]}.txt')
                        self.log_exception_to_file(log_file)

        # removes the priority tags from the config files after they have been processed
        self.remove_priority_tags(config_files)

        total_elapsed_time = timeit.default_timer() - total_time
        self.console.log(f"[bold green]Total time: {total_elapsed_time:.2f} seconds", style='green')
        self.console.log(f"[bold green]Total config files: {len(config_files)}", style='green')
        self.console.log(f"[bold green]Total reports: {q_reportes}", style='green')
        if q_errors > 0:
            self.console.log(f"[bold red]Total errors: {q_errors}", style='red')
            self.console.log(f"[bold red]Reports with errors: {reportes_error}", style='red')

    def _process_report(self, service):
        """
        Processes a single report for a given service.

        :param service: The service configuration dictionary.
        """
        downloader = self.get_downloader(service)
        self.console.log(f'{downloader.service} - {downloader.sub_service}: Downloading file')
        file_path = downloader.download()

        processor = self.get_processor(service, file_path)
        if processor:
            self.console.log(f'{downloader.service} - {downloader.sub_service}: Reading file')
            processor.read()

            self.handle_upload_and_sql(service, processor)

    @staticmethod
    def get_downloader(service):
        """
        Retrieves the appropriate downloader class based on the service configuration.

        :param service: The service configuration dictionary.
        :return: An instance of the appropriate downloader class.
        :raises ValueError: If the downloader configuration is missing or unsupported.
        """
        downloader_config = service.get('downloader')
        if downloader_config is None:
            raise ValueError('downloader must be provided')

        downloader_name = downloader_config.get('name')
        if downloader_name is None:
            raise ValueError('downloader name must be provided')

        servicio = service.get('servicio')
        if servicio is None:
            raise ValueError('servicio must be provided')

        sub_servicio = service.get('sub_servicio')
        if sub_servicio is None:
            raise ValueError('sub_servicio must be provided')

        downloader_classes = {
            'qualtrics': QualtricsDownloader,
            'internaldash': InternalDashDownloader,
            'localpath': LocalPathDownloader,
            'qualtricsSurveyData': QualtricsSurveyDataDownloader,
            'localfolder': LocalFolderDownloader,
            'feedbackIntranet': FeedbackIntranetDownloader
        }

        downloader_class = downloader_classes.get(downloader_name)
        if downloader_class is None:
            raise ValueError(f'Unsupported downloader: {downloader_name}')

        return downloader_class(servicio=servicio, sub_servicio=sub_servicio, **downloader_config)

    @staticmethod
    def get_processor(service, file_path):
        """
        Retrieves the appropriate processor class based on the service configuration.

        :param service: The service configuration dictionary.
        :param file_path: The path to the downloaded file.
        :return: An instance of the appropriate processor class or None if no processor is specified.
        :raises ValueError: If the processor configuration is unsupported.
        """
        processor_config = service.get('processor', {})
        processor_name = processor_config.get('name')

        processor_classes = {
            'csv': CSVProcessor,
            'excel': ExcelProcessor,
            'customFormacionConsolidado': CustomFormacionConsolidadoProcessor
        }

        if processor_name is None:
            return None

        processor_class = processor_classes.get(processor_name)
        if processor_class is None:
            raise ValueError(f'Unsupported processor: {processor_name}')

        return processor_class(file_path=file_path, **processor_config)

    def handle_upload_and_sql(self, service, processor):
        """
        Handles the upload and SQL execution for a given service and processor.

        :param service: The service configuration dictionary.
        :param processor: The processor instance.
        """
        upload_config = service.get('upload')
        servicio = service.get('servicio')
        sub_servicio = service.get('sub_servicio')
        if upload_config:
            self.console.log(f'{servicio} - {sub_servicio}: Uploading to DB')
            processor.upload_to_db(upload_config)

        sql_config = service.get('sql_exec')
        if sql_config:
            self.console.log(f'{servicio} - {sub_servicio}: Executing SQL')
            processor.execute_sql(sql_config)

    @staticmethod
    def log_exception_to_file(log_file):
        """
        Logs the exception traceback to a file.

        :param log_file: The path to the log file.
        """
        with open(log_file, 'w') as f:
            console_log = Console(file=f)
            console_log.print_exception(show_locals=True)

    def load_config_files(self):
        """
        Loads and sorts the configuration files based on priority.

        :return: A list of sorted configuration file paths.
        """
        config_files = glob.glob(f'{self.config_dir}/*.yaml')

        def get_priority(filename):
            """
            Determines the priority of a configuration file based on its name.
                - Priority levels:
                - 0: [PP] - Permanent priority
                - 1: [P]  - Priority (Primary Priority)
                - 2: [H]  - High priority
                - 3: Normal priority (no tag)
                - 4: [L]  - Low priority
            :param filename: The name of the configuration file.
            :return: The priority level as an integer.
            """

            base = basename(filename).upper()
            if base.startswith('[PP]'):
                return 0
            elif base.startswith('[P]'):
                return 1
            elif base.startswith('[H]'):
                return 2
            elif base.startswith('[L]'):
                return 4
            return 3  # Normal priority

        # Filter out disabled files and sort the rest
        active_files = [f for f in config_files if not basename(f).upper().startswith('[D]')]
        sorted_files = sorted(active_files, key=get_priority)

        # Get priority files
        priority_files = [f for f in sorted_files if get_priority(f) <= 1]

        return priority_files if priority_files else sorted_files

    @staticmethod
    def remove_priority_tags(config_files):
        """
        Removes priority tags from the configuration file names.

        :param config_files: A list of configuration file paths.
        """
        for config_file in config_files:
            base_name = basename(config_file).upper()
            if '[PP]' in base_name:
                continue  # Skip permanent priority files

            # Use regex to remove [P] while preserving other tags
            new_name = re.sub(r'\[P]', '', config_file, flags=re.IGNORECASE)
            new_name = re.sub(r'\[([^]]*)P([^]]*)]', r'[\1\2]', new_name, flags=re.IGNORECASE)
            new_name = new_name.replace('[]', '')  # Remove empty brackets if any
            if new_name != config_file:
                os.rename(config_file, new_name)
