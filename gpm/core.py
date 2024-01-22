import requests
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

class GPMClient:
    BASE_URL = 'https://webapisungrow.horizon.greenpowermonitor.com'

    def __init__(self, username, password):
        self.headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        self.credentials = {
            'username': username or os.environ.get('GPM_API_USERNAME'),
            'password': password os.environ.get('GPM_API_PASSWORD')
        }
        
        if not self.credentials['username'] or not self.credentials['password']:
            raise ValueError("GPM API authentication parameters must be provided or set as environment variables 'GPM_API_USERNAME' and 'GPM_API_PASSWORD'")
        
        self.authenticate()

    def authenticate(self):
        """
        Internal method for API authentication via bearer token.
        """
        url = self.BASE_URL + '/api/Account/Token'
        response = requests.post(url, headers=self.headers, json=self.credentials)
        if response.status_code == 200:
            res = response.json()
            self.headers['Authorization'] = 'Bearer ' + res['AccessToken']
        else:
            logging.error(f'[GPM]: Failed to authenticate user. Status code: {response.status_code}')

    def make_api_call(self, endpoint, data):
        """
        Makes a call to the GPM API. Documentation available at 
        https://webapisungrow.horizon.greenpowermonitor.com/swagger/ui/index#/

        Args:
            endpoint (str): back-end endpoint to call
            data (dict): dictionary containing endpoint specific parameters.

        Returns: 
            Response JSON if the call is succesfull, else status code. 
        """
        url = self.BASE_URL + endpoint
        try:
            if endpoint == '/api/Account/Token':
                response = requests.post(url, headers=self.headers, json=data)
            else:
                response = requests.get(url, headers=self.headers, params=data)

            if response.status_code in [200, 216]:
                return response.json()
            elif response.status_code == 416:
                return 416
            else:
                logging.warning(f'[GPM]: Got response with status code {response.status_code}')
                return response.json()
            
        except Exception as e:
            logging.error(f'[GPM]: Exception in API call: {e}')
            return None

    def get_data_list(self, datasources, start_date, end_date, grouping='minute', granularity=1):
        """ 
        Get data list with measurements using the /api/DataList/v2 endpoint. 

        Args:
            datasources (list): List of DataSourceIds to query.
            start_date (str): Start date in ISO8061 format.
            end_date (str): End date in ISO8061 format.
            grouping (str): Time-grouping for response.
            granularity (int): Granularity for the time-grouping parameter.

        Returns:
            The response, which is a list of dictionaries, each representing
            a record of the form:

                {
                    "DataSourceId": 122382,
                    "Date": "2024-01-22T15:00:00.753Z",
                    "Value": 55.43
                }
        """
        params = {
            'startDate': start_date,
            'endDate': end_date,
            'grouping': grouping,
            'granularity': granularity,
            'dataSourceIds': ','.join(map(str, datasources))
        }

        response = self.make_api_call('/api/DataList/v2', params)

        # HTTP 416 - Range not satisfiable
        if response == 416:
            start_datetime = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')
            end_datetime = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S')
            midpoint = start_datetime + (end_datetime - start_datetime) / 2
            midpoint_str = midpoint.strftime('%Y-%m-%dT%H:%M:%S')
            first_half = self.get_data_list(datasources, start_date, midpoint_str, grouping, granularity)
            second_half = self.get_data_list(datasources, midpoint_str, end_date, grouping, granularity)
            return first_half + second_half if first_half and second_half else []
        
        return response

    def get_data_list_in_batches(self, datasources, start_date, end_date, grouping='minute', granularity=5, max_retries=3):
        """ 
        Uses the get_data_list method to fetch data for multiple DataSourceIds. Handles incomplete responses
        (HTTP 206) by making subsequent calls and waiting until all datasources have complete data.

        Args:
            datasources (list): List of DataSourceIds to query.
            start_date (str): Start date in ISO8061 format.
            end_date (str): End date in ISO8061 format.
            grouping (str): Time-grouping for response.
            granularity (int): Granularity for the time-grouping parameter.
            max_retries (int): Max. number of retries in case of 206 response.

        Returns:
            The response, which is a list of dictionaries, each representing
            a record of the form:

                {
                    "DataSourceId": 122382,
                    "Date": "2024-01-22T15:00:00.753Z",
                    "Value": 55.43
                }
        """
        def get_expected_count(start_date, end_date, grouping, granularity):
            """ 
            Get expected number of data points depending on the grouping and granularity chosen.
            """
            start_datetime = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')
            end_datetime = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S')
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
            
        def fetch_data(sources):
            """
            Fetches data in parrallel, batching all DataSourceIds into a maximum of 10 per call (API internal limit).
            """
            batches = [sources[i:i + 10] for i in range(0, len(sources), 10)]
            
            # Make parallel calls
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(self.get_data_list, batch, start_date, end_date, grouping, granularity) for batch in batches]
                results = [future.result() for future in futures]

            # Combine all responses
            combined = []
            for result in results: 
                if result is not None:
                    combined.extend(result)
            return combined

        # Expected number of datapoints for each DataSourceId
        expected_count = get_expected_count(start_date, end_date, grouping, granularity)

        retry_count = 0
        complete_data = []
        while datasources and retry_count < max_retries:
            data = fetch_data(datasources)
            complete_data.extend(data)

            # Count datapoints per DataSourceId
            response_counts = {ds: 0 for ds in datasources}
            for item in data:
                if item['DataSourceId'] in response_counts:
                    response_counts[item['DataSourceId']] += 1

            # Check condition and filter already complete DataSourceIds
            datasources = [ds for ds, count in response_counts.items() if count < expected_count]
            retry_count += 1

        return complete_data