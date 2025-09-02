from os import getenv
from pathlib import Path

from downloaders.downloader import Downloader


class FeedbackIntranetDownloader(Downloader):
    def __init__(self, user: str, password: str, **kwargs):
        super().__init__(**kwargs)
        self.user = user
        self.password = password

    def download(self):
        intranet_base_url = getenv('INTRANET_BASE_URL')
        intranet_url = f'{intranet_base_url}/WebIntranetPublico/Default.aspx'
        feedback_url = f'{intranet_base_url}/webintranetpublico/IntranetMvc/incidencias/wfListadoIncidencia.aspx?strAccesoIncidencia=1&strejecalidad=0&strrespOpera=0&strAgente=0&strrespcalidad=1&strLider=0'
        fecha_ini, fecha_fin = self.generate_range_dates()

        self.start_pw()

        self.page.goto(intranet_url)
        self.page.get_by_placeholder('Usuario').fill(self.user)
        self.page.get_by_placeholder('Contraseña').fill(self.password)
        self.page.get_by_text('INGRESAR').click()
        self.page.wait_for_selector('#dvNombreEmpleadoMaster', state='visible', timeout=90000)
        # Custom url para la descarga de feedback
        self.page.goto(feedback_url)
        # selecciona el filtro por FECHA
        self.page.locator('#ddlsancion').select_option('4')
        # Usa JavaScript para hacer bypass a readonly inputs
        self.page.evaluate(f"document.getElementById('txtFechaInicial').value = '{fecha_ini.format('DD/MM/YYYY')}'")
        self.page.evaluate(f"document.getElementById('txtFechaFinal').value = '{fecha_fin.format('DD/MM/YYYY')}'")
        #wait until button #btnBuscar is clickable
        self.page.wait_for_timeout(1000)
        self.page.wait_for_selector('#btnBuscar', state='visible') #.wait_for_element_state(state='enabled')
        self.page.get_by_text('Todos').click()  # selecciona "Todos"
        self.page.wait_for_timeout(1000)
        self.page.get_by_text('Buscar').click()

        selector = '#tb_Servicio > tbody > tr:nth-child(2) > td:nth-child(1) > center > a > img'
        self.page.wait_for_selector(selector, timeout=600 * 1000)

        self.page.wait_for_timeout(2000)
        with self.page.expect_download(timeout=600 * 1000) as download_info:
            # self.page.get_by_text('Exportar').wait_for(state='visible')
            export_button = self.page.get_by_text('Exportar')
            export_button.wait_for(state='visible')
            export_button.click()
        download = download_info.value
        file_name = Path(download.suggested_filename).stem
        extension = Path(download.suggested_filename).suffix[1:]
        full_path = self.generate_fullpath_name(extension, file_name)
        download.save_as(full_path)
        return str(full_path)
