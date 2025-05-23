from os import getenv
from pathlib import Path
from babel.dates import format_date
from downloaders.downloader import Downloader
from playwright.sync_api import TimeoutError, Locator
from pendulum import Date


class QualtricsDownloader(Downloader):
    def __init__(self, dash_id: str, dash_page: str, user: str, password: str, **kwargs):
        """
        Initializes the QualtricsDownloader object.
            - dash_id: The dashboard ID.
            - dash_page: The dashboard page.
            - user: The Qualtrics username.
            - password: The Qualtrics password.
            - 'date_custom_label': The label for the first date filter. Custom date range. It will be used as the main date filter, fecha_ini and fecha_fin will be used here.
            - 'date_alltime_label': The label for the second date filter. All time.
        If no 'date_custom_label' or 'date_alltime_label' is provided, the downloader will not set any date filter.
        """
        super().__init__(**kwargs)
        self.dash_id = dash_id
        self.dash_page = dash_page
        self.user = user
        self.password = password
        self.date_custom_label = kwargs.get('date_custom_label')
        self.date_alltime_label = kwargs.get('date_alltime_label')

    def download(self) -> str:
        qualtrics_url = getenv('QUALTRICS_URL')
        dashboard = f'{qualtrics_url}/reporting-dashboard/web/{self.dash_id}/pages/{self.dash_page}/view'
        self.start_pw()

        try:
            self.page.goto(f'{qualtrics_url}/login')
            self.page.get_by_role('textbox', name="Username").fill(self.user)
            self.page.get_by_placeholder('Password').fill(self.password)
            self.page.get_by_role('button', name='Sign In').click()
            self.page.wait_for_url(f'{qualtrics_url}/homepage/ui')
            self.page.wait_for_selector('#profile-data-intro', state='visible')

            title = self.page.locator('#profile-data-intro').text_content().strip().lower()
            if 'bienvenido' in title:
                date_alltime = 'Todo el tiempo'
                date_custom = 'Rango de fechas personalizado'
                locale = 'es_PE'
                download_label = 'Descargar'
                download_now_label = 'Descargar archivo automáticamente'
                export_label = 'Exportar'
            elif 'welcome' in title:
                date_alltime = 'All Time'
                date_custom = 'Custom Date Range'
                locale = 'en_US'
                download_label = 'Download dashboard'
                download_now_label = 'Automatically download file'
                export_label = 'Export'
            else:
                raise ValueError('No se identificó el idioma de la interfaz de usuario.')

            self.page.goto(dashboard)

            if self.date_custom_label:
                fecha_ini, fecha_fin = self.generate_range_dates()
                date_1 = self.page.locator('button', has_text=self.date_custom_label)
                date_1.wait_for(state='visible')
                self.set_date_filter_custom(date_1, date_custom, fecha_ini, fecha_fin, locale)

            if self.date_alltime_label:
                date_2 = self.page.locator('button', has_text=self.date_alltime_label)
                date_2.wait_for(state='visible')
                self.set_date_filter_alltime(date_2, date_alltime)

            try:
                # new qualtrics UI
                self.page.get_by_test_id("download-icon").click(timeout=6000)
            except TimeoutError:
                # old qualtrics UI
                self.page.locator("#export-options-button").click()

            self.page.get_by_text(download_label, exact=False).click()

            # Clic en File type
            self.page.locator("#export-file-type-menu").click()
            self.page.get_by_text("CSV").click()
            self.page.wait_for_load_state("load")

            # remove line breaks
            self.page.locator("i > i").first.click()
            # automatic download
            self.page.get_by_role("radio", name=download_now_label).click()

            # wait until the evas counter is a number
            self.page.wait_for_function("""
                                                           (selector) => {
                                                               const el = document.querySelector(selector);
                                                               if (!el) return false;
                                                               const content = el.value;
                                                               return !isNaN(content) && Number.isInteger(Number(content));
                                                           }
                                                       """, arg=["#export-fieldset-limit-results-menu"])

            with self.page.expect_download(timeout=600000) as download_info:
                self.page.get_by_role("button", name=export_label, exact=True).click()
            download = download_info.value
            file_name = Path(download.suggested_filename).stem
            extension = Path(download.suggested_filename).suffix[1:]
            full_path = self.generate_fullpath_name(extension, file_name)
            download.save_as(full_path)
            return str(full_path)

        finally:
            self.page.goto(f'{qualtrics_url}/authn/api/v1/logout')
            self.page.wait_for_url(f'{qualtrics_url}/login')
            self.close_pw()

    def set_date_filter_alltime(self, date_field: Locator, alltime_label: str):
        if alltime_label not in date_field.text_content():
            date_field.click()
            self.page.locator("button[ng-model='filter.rangeKey']").click()
            self.page.get_by_text(alltime_label).click()

    def set_date_filter_custom(self, date_field: Locator, custom_label: str, fecha_ini: Date, fecha_fin: Date, locale: str):
        if custom_label not in date_field.text_content():
            date_field.click()
            self.page.locator("button[ng-model='filter.rangeKey']").click()
            self.page.get_by_text(custom_label).click()
            self.page.locator("input[ng-model='daterange.start']").fill(format_date(fecha_ini, locale=locale))
            self.page.locator("input[ng-model='daterange.end']").fill(format_date(fecha_fin, locale=locale))
