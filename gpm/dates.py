import pandas as pd 
from datetime import datetime, timedelta 
from dateutil.parser import parse as parse_date

def get_missing_dates(df, start_date, end_date, plant_id=None, grouping='minute', granularity=1):
    """
    Gets missing dates in a Pandas DataFrame, checking for a complete index. 
    
    Args:
        df (pd.DataFrame): 
        start_date: 
        end_date: 
        plant_id (int):
        grouping (str):
        granularity (int):

    TO DO:
        - add mapper from 'grouping' and 'granularity' to Pandas frequency. 
    """
    def check_datetime_index(df):
        if isinstance(df.index, pd.DatetimeIndex):
            return True
        elif isinstance(df.index, pd.MultiIndex):
            for level in df.index.levels:
                if isinstance(level, pd.DatetimeIndex):
                    return True
        return False 
                
    if plant_id: 
        df = df.loc[plant_id]

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')
        end_date = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S')

    start_date = floor_and_format(start_date)
    end_date = floor_and_format(end_date)

    if not check_datetime_index(df):
        try:
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date')
        except ValueError:
            raise Exception('DataFrame contains no DatetimeIndex or "Date" column.')

    df = df[(df.index.get_level_values('Date') >= start_date) & (df.index.get_level_values('Date') <= end_date)]

    # Check missing, using default granularity (5MIN) for now
    missing_start, missing_end = check_complete_index(df, start_date, end_date)
    return missing_start, missing_end

def check_complete_index(df, start_date, end_date, freq='5MIN'):
    """
    Checks wether a Pandas DataFrame with DateTime index with name 'Date' is complete or not.
    For now only accepts whole dates with daily granularity! (i.e. 2024-01-25T00:00:00)

    Args:
        start_date (str): date in 'YYYY-MM-DDThh:mm:ss' format
        end_date (str): date in 'YYYY-MM-DDThh:mm:ss' format
        freq (str): date frequency expected in the index as of Pandas documentation (see 
            https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases)
        
    Returns: 

    """
    # Parse dates 
    start_date = parse_date(start_date)
    end_date = min(parse_date(end_date), datetime.now() - timedelta(minutes=15))

    # Construct expected index
    # By default GPM starts at 00:05:00 for each day and ends at 00:00:00
    expected_range = pd.date_range(start_date + timedelta(minutes=5), end_date, freq=freq)

    # Calculate difference 
    diff = expected_range.difference(df.index.get_level_values('Date'))
    if diff.empty: return None, None

    # Extract only days from missing dates 
    start = diff[0].normalize()
    end = diff[-1].normalize() + timedelta(days=1)

    return start, end

def expected_measurement_count(start_date, end_date, grouping, granularity):
    """ 
    Get expected number of data points depending on the grouping and granularity chosen.
    """
    if isinstance(start_date, str):
        start_datetime = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S')
    else:
        start_datetime = start_date 
        end_datetime = end_date 
    
    delta = end_datetime - start_datetime

    if grouping == 'minute':
        total_minutes = delta.total_seconds() / 60
        return total_minutes / granularity
    elif grouping == 'hour':
        total_hours = delta.total_seconds() / 3600
        return total_hours / granularity
    elif grouping == 'day':
        total_days = delta.days
        return total_days / granularity
    else:
        return 0
    
def split_dates_in_half(start_date, end_date):
    start_datetime = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S')
    midpoint = start_datetime + (end_datetime - start_datetime) / 2
    midpoint_str = midpoint.strftime('%Y-%m-%dT%H:%M:%S')
    return midpoint_str

def floor_and_format(date):
    """ 
    Floors the date's minutes to the closest 5min multiple and formats the date
    to ISO8061 (i.e 2024-01-24T12:15:00)
    """
    round_minutes = (date.minute - date.minute % 5)
    return date.replace(minute=round_minutes, second=0).isoformat(timespec='seconds')