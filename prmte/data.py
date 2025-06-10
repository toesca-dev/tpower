import pandas as pd 

def transform_records(records, last_reading=None, format='consolidated'):
    """
    Transforms records coming from the 'get_15min_readings' method from PRMTEClient class in core package. 

    Args:
        records (list): list of tuples containing measurement records. See 'get_15min_readings' method.
        format (str): format of output Pandas DataFrame. Either 'consolidated' or 'columns'.

    Returns: 
        A Pandas DataFrame containing all measurement records. Depending on the format chosen:

            if 'consolidated', output DataFrame has a DateTimeIndex (date), with columns "idPuntoMedida"
            (measurement point unique id) and "value" (net energy flow for that interval, in kWh units.
            The value is negative for net imports and positive for net exports.)

            if 'columns', output DataFrame has columns "idPuntoMedida", date (DateTime column), "Retiros"
            for energy withdrawls in kWh and "Inyecciones" for energy injections during that interval.
    """
    df = pd.DataFrame(records, columns=['idPuntoMedida', 'canalVal', 'date', 'value'])
    df['date'] = pd.to_datetime(df['date'])
    if last_reading:
        df = df.set_index('date').sort_index().loc[:last_reading].reset_index()
    else:
        df = df.set_index('date').sort_index().reset_index()
    
    # By default withdrawls are negative values indicating the direction of energy flow
    df['value'] = df.apply(lambda row: -row['value'] if row['canalVal'] == 1 else row['value'], axis=1)

    if format == 'consolidated':
        # Not actually invalid, you can inject and withdraw energy during the same period
        #invalid_groups = df.groupby(['idPuntoMedida', 'date']).filter(lambda group: group['value'].ne(0).sum() > 1)
        df = df.groupby(['idPuntoMedida', 'date']).sum().reset_index().drop(columns=['canalVal'])
        df.set_index('date', inplace=True)

    elif format == 'columns':
        df = (
            df.pivot_table(
                index=['idPuntoMedida', 'date'],
                columns='canalVal',
                values='value',
                fill_value=0,
            )
            .reset_index()
        )
        df.rename(columns={1: 'Retiros', 3: 'Inyecciones'}, inplace=True)

    return df
