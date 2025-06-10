import os
import logging
import requests
import numpy as np

class PRMTEClient:
    """Simple wrapper for the public PRMTE API."""

    # New API endpoints are now served under ``medidas.api.coordinador.cl``
    # using the ``medidas-v2`` path.  Older endpoints are still kept for
    # backwards compatibility.
    BASE_URL = "https://medidas.api.coordinador.cl/medidas-v2/"

    # Known endpoints.  ``measurement`` is the new endpoint that replaces the
    # old ``medidas`` service.
    ENDPOINTS = [
        'canales',
        'coordinados',
        'puntomedidas',
        'medidas',
        'measurement',
    ]

    def __init__(self, api_key=None):
        self.api_key = api_key or os.environ.get('PRMTE_API_KEY')
        if not self.api_key:
            raise ValueError("API key must be provided or set as an environment variable 'PRMTE_API_KEY'")

    def make_api_call(self, endpoint, params=None, verbose=False):
        if endpoint not in self.ENDPOINTS:
            raise ValueError("Invalid endpoint")

        # Construct the full URL
        url = self.BASE_URL + endpoint
        if params is None:
            params = {}

        params['user_key'] = self.api_key
        response = requests.get(url, params=params)
        if verbose: print(response.url)

        # Check for successful status code
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"{response.status_code}: {response.text}")
            return None

    def get_15min_readings(self, idPuntoMedida, periodo, last_reading=False):
        """
        Calls the "medidas" endpoint and fetches all 15min readings for a given period.
        For now, hardcoded to include only active energy channels (injections and withdrawls).

        Args: 
            idPuntoMedida (str): the measure point unique identifier.
            periodo (str): measurement period (monthly granularity) in YYYYMM format.

        Returns:
            List of records (tuples) with the format (idPuntoMedida, idCanal, date, value).
            idCanal = 1 corresponds to active energy withdrawls, while idCanal = 3 is for 
            active energy injections.
        
        Units:
            Date comes in "YYYY-MM-DD HH:MM:SS" format. 
            Value is in kWh units.
        """
        params = {
            'idPuntoMedida': idPuntoMedida,
            'periodo': periodo,
            'idCanal': '1,3'
        }

        res = self.make_api_call('medidas', params=params)
        if not res: return 0, 0 # ToDo: raise custom error

        canales = res[0]['canales']
        medidas = res[0]['mediciones']
        last_reading = res[0]['fechaUltimaLectura']
        records = [(idPuntoMedida, c['idCanal'], m['intervalo'], m[f'canalVal{c["idCanal"]}']) for c in canales for m in medidas]
        if last_reading: return records, res[0]['fechaUltimaLectura']
        return records

    def get_total_period_energy(self, idPuntoMedida, periodo):
        """
        Get the total energy for every channels for a given measurement point and period.

        Args:
            idPuntoMedida (str): the measure point unique identifier.
            periodo (str): measurement period (monthly granularity) in YYYYMM format.

        Returns: 
            A tuple containing (nan_values, last_reading, net_active_energy, net_reactive_energy). 
            nan_values is a boolean indicating wether there are NaN values in the period or not. 
            last_reading is the last available measurement in YYYY-MM-DD HH:MM:SS format. Usually less
            than 24 hours, but depends on meter connectivity and other variables from the PRMTE service.
            Other two are net energy (injections - withdrawls) for the whole period, as Numpy 
            arrays, in kWh units.
        """
        params = {
            'idPuntoMedida': idPuntoMedida,
            'periodo': periodo,
            'idCanal': '1,2,3,4'
        }
        res = self.make_api_call('medidas', params=params)
        if not res: return 0 # ToDO: Raise custom error 

        medidores = res[0]['medidores']
        last_reading = res[0]['fechaUltimaLectura']
        activa = np.array([medida['canalVal3'] - medida['canalVal1'] if medida['canalVal1'] != None and medida['canalVal3'] != None else np.nan for medida in res[0]['mediciones']])
        reactiva = np.array([medida['canalVal4'] - medida['canalVal2'] if medida['canalVal4'] != None and medida['canalVal2'] != None else np.nan for medida in res[0]['mediciones']])
        nan_values = np.isnan(activa).any() | np.isnan(reactiva).any()
        return nan_values, last_reading, np.nansum(activa), np.nansum(reactiva)

    def get_measurements(self, measure_point_id, period, end_period=None):
        """Fetch raw measurements using the new ``measurement`` endpoint.

        Parameters
        ----------
        measure_point_id : str
            Identifier of the measurement point.
        period : str
            Starting period in ``YYYYMMDDhhmm`` format.
        end_period : str, optional
            If provided, the API will return measurements for the range
            ``period`` - ``end_period``.

        Returns
        -------
        Tuple[list, str]
            List of tuples ``(measurePointId, channelId, date, value)`` and
            the last reading timestamp provided by the API.  Returns an empty
            list if no data is returned.
        """

        params = {
            'measurePointId': measure_point_id,
            'period': period,
        }
        if end_period:
            params['endPeriod'] = end_period

        res = self.make_api_call('measurement', params=params)
        if not res:
            return [], None

        records = []
        last_reading = None
        for series in res:
            last_reading = series.get('lastReadingDate')
            for measurement in series.get('measurement', []):
                ts = measurement['dateRange']
                for ch in series.get('channel', []):
                    cid = ch['channelId']
                    val = measurement.get(f'channel{cid}', 0)
                    records.append((series['measurePointId'], cid, ts, val))

        return records, last_reading
