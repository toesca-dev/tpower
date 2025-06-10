import pandas as pd 
from datetime import datetime
from dateutil.relativedelta import relativedelta

from .core import PRMTEClient

def get_coordinados(client: PRMTEClient):
    """ 
    Get all "coordinados" in the PRMTE database. 
    
    A "coordinado" is a company that operates in the Chilean electricity market 
    under the instruction of the Coordinador Electrico Nacional (Chile's ISO).

    Args:
        client: PRMTEClass object.

    Returns: 
        A list of dictionaries containing all idCoordinados in the PRMTE database.
    """
    return client.make_api_call('coordinados')

def get_canales(client: PRMTEClient):
    """ 
    Get all available channels in a standard PRMTE meter.

    Args:
        client: PRMTEClass object.

    Returns:
        A list of dictionaries containing all available channels and their description.
    """
    return client.make_api_call('canales')

def get_puntomedidas(client: PRMTEClient, idCoordinado: str):
    """ 
    Get all measure points associated to a single idCoordinado.

    Args:
        client: PRMTEClass object.
        idCoordinado (str): unique identifier for coordinated company.

    Returns:
        A list of dictionaries, each containing a measure point with the 
        following structure and information:
            {
              'idPuntoMedida': the measure point unique identifier.
              'idCoordinado':  the coordinated company id.
              'subestacion':   name of the transmission/distribution
                               substation associated with the measure point.
              'tension':       voltage of the substation (in kV)
              'region' :       region for the measure point.
            }

    """
    return client.make_api_call('puntomedidas', params={'idCoordinado': idCoordinado})

def get_measurements(client: PRMTEClient, idPuntoMedida: str, period: str, end_period=None, granularity='1H', df_format='consolidated'):
    """Fetch measurements for a single point between two periods.

    Parameters
    ----------
    client : :class:`PRMTEClient`
        Authenticated client instance.
    idPuntoMedida : str
        Identifier for the measure point.
    period : str
        Start period in ``YYYYMM`` format.
    end_period : str, optional
        End period in ``YYYYMM`` format. When omitted, only ``period`` is
        retrieved.
    granularity : str, optional
        Resampling rule understood by ``pandas.DataFrame.resample``.
    df_format : str, optional
        Output format for :func:`transform_records`.

    Returns
    -------
    pandas.DataFrame
        Measurements indexed by date.
    """

    # Build list of periods to query
    periods = []
    start = datetime.strptime(period, "%Y%m")
    end = datetime.strptime(end_period, "%Y%m") if end_period else start

    current = start
    while current <= end:
        periods.append(current.strftime("%Y%m"))
        current += relativedelta(months=1)

    all_frames = []
    last_reading = None

    for per in periods:
        records, last_reading = client.get_measurements(idPuntoMedida, per)
        if not records:
            continue
        df = transform_records(records, last_reading, format=df_format)
        if granularity:
            df = df.resample(granularity).sum()
        all_frames.append(df)

    if not all_frames:
        return pd.DataFrame()

    return pd.concat(all_frames).sort_index()


def get_measurements_range(
    client: PRMTEClient,
    idPuntoMedida: str,
    start_period: str,
    end_period: str | None = None,
    granularity: str = '1H',
    df_format: str = 'consolidated',
):
    """Fetch measurements from ``start_period`` up to ``end_period``.

    When ``end_period`` is omitted the current month is used.
    Parameters are otherwise the same as :func:`get_measurements`.
    """

    if end_period is None:
        end_period = datetime.utcnow().strftime('%Y%m')

    return get_measurements(
        client,
        idPuntoMedida=idPuntoMedida,
        period=start_period,
        end_period=end_period,
        granularity=granularity,
        df_format=df_format,
    )
    

def get_historic_measurements(client: PRMTEClient, idPuntoMedida: str, broken_periods=[], granularity='1H', df_format='consolidated'):
    """
    Get all measurements up until the last measurement available. 

    Args:
        client: PRMTEClass object.
        idPuntoMedida (str): the measure point unique identifier.
        broken_periods (list): some periods are unavailable in the PRMTE database for some reason
            (server error). We deliberately skip these records.
        granularity (str): resampling rule as of Pandas documentation (see 
            https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases)
        df_format (str): either 'consolidated' or 'columns'. See 'transform_records' function in 
            data package for more information on this parameter.
        

    Returns: 
        A Pandas DataFrame with the historic data for that measurement point.
    """
    i = 0
    now = datetime.now()
    total = pd.DataFrame()

    while True:
        period = (now - relativedelta(months=i)).strftime("%Y%m"); i += 1
        records, last_reading = client.get_15min_readings(idPuntoMedida, period)

        if not records:
            if period in broken_periods: continue 
            else: break

        df = transform_records(records, last_reading, format=df_format).reset_index()
        df = df.drop(columns=['idPuntoMedida']).set_index('date').resample(granularity).sum()
        total = pd.concat([total, df], axis=0)

    return total


def get_daily_energy(client: PRMTEClient, idPuntoMedida: str, period: str, end_period=None):
    """Convenience wrapper returning a daily energy profile.

    Parameters are the same as :func:`get_measurements`.  The resulting
    DataFrame is resampled to daily totals.
    """

    df = get_measurements(
        client,
        idPuntoMedida=idPuntoMedida,
        period=period,
        end_period=end_period,
        granularity='D',
    )
    return df


def save_measurements_csv(client: PRMTEClient, idPuntoMedida: str, period: str, filename: str, **kwargs):
    """Download measurements and save them to ``filename`` in CSV format."""

    df = get_measurements(client, idPuntoMedida, period, **kwargs)
    df.to_csv(filename)
    return filename


def measurements_to_excel(
    client: PRMTEClient,
    assets: dict,
    period: str,
    end_period: str | None = None,
    filename: str = 'measurements.xlsx',
    granularity: str = '1H',
    df_format: str = 'long',
):
    """Download measurements for multiple assets and export them to Excel.

    Parameters
    ----------
    client : :class:`PRMTEClient`
        Authenticated client instance.
    assets : dict
        Mapping ``asset_name -> idPuntoMedida``.
    period : str
        Start period in ``YYYYMM`` format.
    end_period : str, optional
        End period in ``YYYYMM`` format.  Defaults to the current month when
        omitted.
    filename : str, optional
        Destination file name for the Excel workbook.
    granularity : str, optional
        Resampling rule for the measurements.
    df_format : {"long", "wide"}, optional
        Output table format.  ``long`` produces one row per asset and date,
        ``wide`` pivots assets into columns.
    """

    internal_format = 'consolidated' if df_format.lower() == 'long' else 'columns'

    mapping_df = pd.DataFrame(list(assets.items()), columns=['asset_name', 'idPuntoMedida'])
    frames = []

    for asset_name, mp_id in assets.items():
        df = get_measurements_range(
            client,
            mp_id,
            start_period=period,
            end_period=end_period,
            granularity=granularity,
            df_format=internal_format,
        ).reset_index()
        if df.empty:
            continue
        df['asset_name'] = asset_name
        frames.append(df)

    measurements = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if df_format.lower() == 'wide' and not measurements.empty:
        measurements = measurements.pivot_table(index='date', columns='asset_name', values='value')

    with pd.ExcelWriter(filename) as writer:
        mapping_df.to_excel(writer, sheet_name='assets', index=False)
        measurements.to_excel(writer, sheet_name='measurements')

    return filename



