import pandas as pd 

field_mapper = {
        'COM STATUS': 'Com Status',
        'COMS STATUS': 'Com Status',
        'Plant Com Status': 'Com Status',
        'Plant Energy': 'Energy',
        'Total Meter Energy': 'Energy',
        'Total Meters Energy': 'Energy',
        'Plant Power': 'Power',
        'Total Meter Power': 'Power',
        'Total Meters Power': 'Power',
        'Plant Irradiance': 'Irradiance'
    }

def merge_and_pivot(df, ds, index_col=['PlantId', 'Date'], merge_col='DataSourceId', field_col='DataSourceName',
                    value_col='Value', duplicate_col=['DataSourceId', 'Date'], field_mapper=None, pivot=True, set_datetime=True):
    """
    Merges two dataframes on a common column and applies a pivot operation, resulting in a dataframe with column
    names that correspond to the values present in the "ds" dataframe for the "field_col" column. Optionally, it 
    modifies and filters these values depending on the "field_mapper" dict, dropping all column names (and 
    therefore all data in the "df" dataframe) that are not present in the dict keys. 

    Args:
        df (Pandas DataFrame): contains data points indexed by PlantId and Date, each data point corresponding to 
            a unique combination of the tuple (Date, DataSourceId).
        ds (Pandas DataFrame): contains DataSource information, like DataSourceId and DataSourceName. 
        index_col (list or str): index columns to set in the pivoted dataframe.
        merge_col (str): common column between the data and information dataframes (DataSourceId by default).
        field_col (str): column to use as column names in the pivoted dataframe.
        value_col (str): column tu use as values in the pivoted dataframe.
        duplicate_col (str): subset of columns to drop duplicated entries.
        field_mapper (dict): dict containing original DataSourceNames as keys and cleaned (homogeneous)
            column names as values. Every data point that das a DataSourceName that is not present in the 
            dictionary gets dropped after the merge operation.
        pivot (bool): to pivot the dataframe or not. If not, returns the merged dataframe.
        set_datetime (bool): to set the "Date" column to DateTime or not in the data dataframe ("df"). 

    Returns:
        A Pandas DataFrame.  
    """
    # Drop duplicate rows
    if duplicate_col:
        df.drop_duplicates(subset=duplicate_col, inplace=True)

    # Set datetime column
    if set_datetime:
        df['Date'] = pd.to_datetime(df['Date'])
        
    # Set merge column as index to speed up merge operation
    df.set_index(merge_col, inplace=True)
    ds.set_index(merge_col, inplace=True)

    # Merge
    merged = pd.merge(df, ds, on=merge_col, how='inner')

    # Clean and filter DataSourceNames if mapper passed 
    if field_mapper:
        merged[field_col] = merged[field_col].map(field_mapper).fillna('drop')
        merged = merged[merged[field_col] != 'drop']

    # Return merged dataframe if chosen
    if not pivot: 
        # Set index column if given
        if index_col:
            return merged.reset_index().set_index(index_col)
        return merged

    # Pivot
    pivoted = merged.pivot_table(index=index_col, columns=field_col, values=value_col)
    pivoted.columns.name = None

    return pivoted

def format_last_data(response, datasourceids, tuples=False):
    """
    Filters a response coming from the 'get_last_data' method in the GPMClient class in core package.
    The method is used because the endpoint 'get_last_data' returns the last available measurement 
    for ALL DataSources associated to a single plant, and we're usually interested in only some of them.

    Args:
        response (list): list of dictionaries containing measurement records.
        datasourceids (list): list of DataSourceIds to keep from the response 

    Returns:
        If 'tuples', returns a list of formatted tuples, else it returns a list of dictionaries.
    """
    # For performance 
    datasourceids = set(datasourceids)

    if not tuples:
        return [
            {
                'DataSourceId': item['DataSourceId'],
                #'DataSourceName': item['DataSourceName'],
                'Date': item['LastValue']['Date'],
                'Value': item['LastValue']['Value']
            }
            for item in response if item['DataSourceId'] in datasourceids
        ]
    
    else:
        return [
            (   
                item['LastValue']['Date'].replace('T', ' ').split('.')[0],
                int(item['DataSourceId']),
                float(item['LastValue']['Value'])
            )
            for item in response if item['DataSourceId'] in datasourceids
        ]

