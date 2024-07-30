from os import getenv
from pathlib import Path
from babel.dates import format_date
from downloaders.downloader import Downloader
from playwright.sync_api import TimeoutError


class QualtricsDownloader(Downloader):
    def __init__(self, dash_id: str, dash_page: str, user: str, password: str, **kwargs):
        super().__init__(**kwargs)
        self.dash_id = dash_id
        self.dash_page = dash_page
        self.user = user
        self.password = password

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
                mon_date_label = 'Fecha de monitoreo'
                call_date_label = 'Fecha de la llamada'
                download_now_label = 'Descargar archivo automáticamente'
                export_label = 'Exportar'
            elif 'welcome' in title:
                date_alltime = 'All Time'
                date_custom = 'Custom Date Range'
                locale = 'en_US'
                download_label = 'Download dashboard'
                mon_date_label = 'Fecha de monitoreo'
                call_date_label = 'Fecha de la llamada'
                download_now_label = 'Automatically download file'
                export_label = 'Export'
            else:
                raise ValueError('No se identificó el idioma de la interfaz de usuario.')

            self.page.goto(dashboard)

            # obtiene acceso a los filtros
            fecha_monitoreo = self.page.locator('button', has_text=mon_date_label)
            fecha_monitoreo.wait_for(state='visible')
            fecha_llamada = self.page.locator('button', has_text=call_date_label)
            fecha_llamada.wait_for(state='visible')

            fecha_ini, fecha_fin = self.generate_range_dates()

            if date_custom not in fecha_monitoreo.text_content():
                fecha_monitoreo.click()
                self.page.locator("button[ng-model='filter.rangeKey']").click()
                self.page.get_by_text(date_custom).click()
                self.page.locator("input[ng-model='daterange.start']").fill(format_date(fecha_ini, locale=locale))
                self.page.locator("input[ng-model='daterange.end']").fill(format_date(fecha_fin, locale=locale))

            if date_alltime not in fecha_llamada.text_content():
                fecha_llamada.click()
                self.page.locator("button[ng-model='filter.rangeKey']").click()
                self.page.get_by_text(date_alltime).click()

            try:
                # new qualtrics UI
                self.page.get_by_test_id("download-icon").click(timeout=3000)
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
