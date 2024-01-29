import pandas as pd 
from datetime import datetime 

from .core import GPMClient 
from .dates import get_missing_dates

def initialize_client(plants_filename: str, total_filename: str, inverter_filename: str, tracker_filename: str,
                      concepts=['total, inverter, tracker'], username=None, password=None):
    """
    Initializes GPM client with all relevant Plant and DataSource information. 
    """
    plants = pd.read_csv(plants_filename)
    total_ds = pd.read_csv(total_filename)
    inverter_ds = pd.read_csv(inverter_filename)
    tracker_ds = pd.read_csv(tracker_filename)

    datasources = {}
    all_datasources = [] 
    for plant_id in plants['PlantId'].unique():
        total = total_ds[total_ds['PlantId'] == plant_id]['DataSourceId'].to_list()
        inverter = inverter_ds[inverter_ds['PlantId'] == plant_id]['DataSourceId'].to_list()
        tracker = tracker_ds[tracker_ds['PlantId'] == plant_id]['DataSourceId'].to_list()

        datasources[plant_id] = {
            'total': total,
            'inverter': inverter,
            'tracker': tracker
        }

        all_datasources.extend(total + inverter + tracker)

    plant_ids = plants['PlantId'].to_list()
    gpm = GPMClient(username, password,
                    plant_ids=plant_ids,
                    concepts=concepts,
                    datasources=datasources)
    gpm.all_datasources = all_datasources

    return gpm

def get_data_for_range(client: GPMClient, start_date: [str, datetime.datetime], end_date: [str, datetime.datetime],
    datasourceids=None, plant_id=None, concept=None, grouping='minute', granularity=1, existing_df=None, return_df=False):
    """ 
    Simple wrapper function that uses "GPMClient.get_data_list_in_batches" to fetch measurements for a given date range.
    
    """
    # Filter DataSourceIds 
    datasourceids = client.filter_datasourceids(
        datasourceids=datasourceids,
        plant_id=plant_id,
        concept=concept
    )
    
    # Check for data in existing DataFrame if given.
    if existing_df is not None:
        start, end = get_missing_dates(existing_df, start_date, end_date,
            plant_id=plant_id, grouping=grouping, granularity=granularity)
        
        # Existing dataframe is complete
        if start is None and end is None:
            return existing_df
        
        # Else we need to fetch missing data 
        start_date = start 
        end_date = end 
        
    # Get data from API
    data = client.get_data_list_in_batches(
        datasourceids, start_date, end_date,
        grouping=grouping, granularity=granularity)
    
    # Return new expanded dataframe 
    if return_df:
        data = pd.concat([existing_df, pd.DataFrame(data)]) if existing_df is not None else pd.DataFrame(data)
        data = data.drop_duplicates(subset=['Date', 'DataSourceId'])
    
    # Else return raw data
    return data

def get_last_data(client: GPMClient, datasourceids=None, plant_id=None, concept=None, return_df=True):
    """
    Fetches last data using "GPMClient.get_last_data_for_all_plants".
    
    TO DO:
        - adapt GPMClient.get_last_data_for_all_plants to use the new DataSourceIds filtering functions.
    """
    pass