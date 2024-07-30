import requests
from os import getenv, makedirs, path
from downloaders.downloader import Downloader


class InternalDashDownloader(Downloader):
    def __init__(self, servicio_id, tipo_reporte, **kwargs):
        super().__init__(**kwargs)
        self.servicio_id = servicio_id
        self.tipo_reporte = tipo_reporte

    def download(self) -> str:
        if self.tipo_reporte not in ['fijo', 'dinamico']:
            raise ValueError('Invalid tipo_reporte')

        fecha_ini, fecha_fin = self.generate_range_dates()

        fecha_ini = fecha_ini.to_date_string()
        fecha_fin = fecha_fin.to_date_string()

        internaldash_url = getenv('INTERNALDASH_URL')
        if internaldash_url is None:
            raise ValueError('INTERNALDASH_URL must be provided')

        tipo_reporte = self.tipo_reporte
        if tipo_reporte is None:
            raise ValueError('tipo_reporte must be provided')
        elif tipo_reporte == 'fijo':
            url = f'{internaldash_url}/ClaroExcel/?ini={fecha_ini}&fin={fecha_fin}&tipo={self.servicio_id}'
        elif tipo_reporte == 'dinamico':
            url = f'{internaldash_url}/ReporteExcel/?ini={fecha_ini}&fin={fecha_fin}&id={self.servicio_id}'
        else:
            raise ValueError('Invalid tipo_reporte')

        r = requests.get(url=url, allow_redirects=True, stream=True, timeout=660)

        full_path = self.generate_fullpath_name('xls', f'{self.tipo_reporte}_{self.servicio_id}')

        # Ensure the directory exists
        makedirs(path.dirname(full_path), exist_ok=True)

        with open(full_path, 'wb') as xls:
            for chunk in r.iter_content(chunk_size=1024):
                # writing one chunk at a time
                if chunk:
                    xls.write(chunk)
        return str(full_path)
