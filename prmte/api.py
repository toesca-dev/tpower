import pandas as pd 
from datetime import datetime
from dateutil.relativedelta import relativedelta

def get_coordinados(client):
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

def get_canales(client):
    """ 
    Get all available channels in a standard PRMTE meter.

    Args:
        client: PRMTEClass object.

    Returns:
        A list of dictionaries containing all available channels and their description.
    """
    return client.make_api_call('canales')

def get_puntomedidas(client, idCoordinado):
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

def get_measurements(client, idPuntoMedida, period, end_period=None, granularity='1H', df_format='consolidated'):
    """ 
    Get all measurements for a single measurement point, given a period and optionally an end period.

    Args:
        client: PRMTEClass object.
        idPuntoMedida (str): the measure point unique identifier.
        periodo (str): measurement period (monthly granularity) in YYYYMM format.
        end_period (str): end measurement period in YYYYMM format (optional).
        granularity (str): resampling rule as of Pandas documentation (see 
            https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases)
        df_format (str): either 'consolidated' or 'columns'. See 'transform_records' function in 
            data package for more information on this parameter.

    Returns:
        A Pandas DataFrame containing all measurements for a single measurement point. If end_period is not None, 
        the DataFrame contains all periods between 'period' and 'end_period'.
    """
    pass
    

def get_historic_measurements(client, idPuntoMedida, broken_periods=[], granularity='1H', df_format='consolidated'):
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
        records, last_reading = client.get_15min_readings('COCHRCAS_013_PMGD4_PSE', period)

        if not records:
            if period in broken_periods: continue 
            else: break

        df = transform_records(records, last_reading, format=df_format).reset_index()
        df = df.drop(columns=['idPuntoMedida']).set_index('date').resample(granularity).sum()
        total = pd.concat([total, df], axis=0)

    return total


