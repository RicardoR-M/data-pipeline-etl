from os import getenv
from pathlib import Path
from downloaders.downloader import Downloader


class QualtricsSurveyDataDownloader(Downloader):
    def __init__(self, dash_id: str, user: str, password: str, **kwargs):
        super().__init__(**kwargs)
        self.dash_id = dash_id
        self.user = user
        self.password = password

    def download(self) -> str:
        qualtrics_url = getenv('QUALTRICS_URL')
        dashboard = f'{qualtrics_url}/responses/#/surveys/{self.dash_id}'
        self.start_pw()

        try:
            self.page.goto(f'{qualtrics_url}/login')
            self.page.get_by_role('textbox', name="Username").fill(self.user)
            self.page.get_by_placeholder('Password').fill(self.password)
            self.page.get_by_role('button', name='Sign In').click()
            self.page.wait_for_url(f'{qualtrics_url}/homepage/ui')
            self.page.wait_for_selector('#profile-data-intro', state='visible')

            self.page.goto(dashboard)

            # click en exportar
            self.page.get_by_test_id("export-and-import-menu").click()
            # click en exportar datos
            self.page.get_by_role("menuitem", name="Export Data...").click()

            # click en CSV tab
            csv_tab = self.page.get_by_role("tab", name="CSV")
            csv_tab.wait_for(state="visible")
            csv_tab.click()

            self.page.get_by_text('Use choice text', exact=True).click()

            # check if checked and if not, click
            checkbox = self.page.get_by_test_id("export-all-fields-checkbox")
            if not checkbox.is_checked():
                checkbox.check()
            self.page.get_by_role("button", name="More options").click()

            checkbox = self.page.get_by_test_id("export-compress-checkbox")
            if checkbox.is_checked():
                checkbox.uncheck()

            checkbox = self.page.get_by_test_id("export-replace-newline-checkbox")
            if not checkbox.is_checked():
                checkbox.check()

            checkbox = self.page.get_by_test_id('export-use-qid-header-checkbox')
            if not checkbox.is_checked():
                checkbox.check()

            with self.page.expect_download(timeout=700 * 1000) as download_info:
                self.page.get_by_role("button", name="Download", exact=True).click()
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
