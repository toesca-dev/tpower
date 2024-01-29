import requests
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from .data import format_last_data
from .dates import expected_measurement_count, split_dates_in_half

class GPMClient:
    BASE_URL = 'https://webapisungrow.horizon.greenpowermonitor.com'

    def __init__(self, username=None, password=None, plant_ids=None, concepts=None, datasources=None):
        self.headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        self.credentials = {
            'username': username or os.environ.get('GPM_API_USERNAME'),
            'password': password or os.environ.get('GPM_API_PASSWORD')
        }

        if not self.credentials['username'] or not self.credentials['password']:
            raise ValueError("GPM API credentials must be provided or set as environment variables 'GPM_API_USERNAME' and 'GPM_API_PASSWORD'")
        
        self.authenticate()

        self.plant_ids = plant_ids
        self.concepts = concepts
        self.datasources = datasources

    def filter_datasourceids(self, datasourceids=None, plant_id=None, concept=None):
        """
        Used to filter the DataSourceIds to fetch. Can pass either a list or dictionary
        
        """
        data_dict = self.datasources
        if datasourceids is None and plant_id is None and concept is None:
            all_datasources = []
            for plant in data_dict.values():
                for datasources in plant.values():
                    all_datasources.extend(datasources)
            return list(set(all_datasources))
        
        result = []

        if datasourceids:
            if isinstance(datasourceids, list):
                return datasourceids 
            elif isinstance(datasourceids, dict):
                for item in data_dict.values():
                    for key, value in item.items():
                        if key in datasourceids:
                            result.extend(value)
            
        elif isinstance(plant_id, (int, list)) and not concept:
            if isinstance(plant_id, int):
                plant_id = [plant_id]
            for pid in plant_id:
                for item in data_dict.get(pid, {}).values():
                    result.extend(item)
        
        elif isinstance(concept, (str, list)) and not plant_id:
            if isinstance(concept, str):
                concept = [concept]
            for item in data_dict.values():
                for key, value in item.items():
                    if key in concept:
                        result.extend(value)
        
        elif plant_id and concept:
            if isinstance(plant_id, list) and isinstance(concept, list):
                for pid in plant_id:
                    for c in concept:
                        item = data_dict.get(pid, {}).get(c, [])
                        result.extend(item)

            elif isinstance(plant_id, int) and isinstance(concept, str):
                item = data_dict.get(plant_id, {}).get(concept, [])
                result.extend(item)

            elif isinstance(plant_id, int) and isinstance(concept, list):
                for c in concept:
                    item = data_dict.get(plant_id, {}).get(c, [])
                    result.extend(item)

            elif isinstance(plant_id, list) and isinstance(concept, str):
                for pid in plant_id:
                    item = data_dict.get(pid, {}).get(concept, [])
                    result.extend(item)

        return result

    def authenticate(self):
        """
        Internal method for API authentication via bearer token.
        """
        url = self.BASE_URL + '/api/Account/Token'
        response = requests.post(url, headers=self.headers, json=self.credentials)
        if response.status_code == 200:
            res = response.json()
            self.headers['Authorization'] = 'Bearer ' + res['AccessToken']
            logging.info(f'[GPM]: Authentication successful')
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

            if response.status_code in [200, 206]:
                return response.json()
            elif response.status_code == 416:
                return 416
            else:
                logging.warning(f'[GPM]: Got response with status code {response.status_code}')
                return response.json()
            
        except Exception as e:
            logging.error(f'[GPM]: Exception in API call: {e}')
            return None
        
    def get_plant_information(self):
        """ 
        Get all plants associated to the API user and store PlantIds in a list.
        
        Returns:
            A list of dictionaries, each containing plant specific information.
        """
        res = self.make_api_call('/api/Plant', None)
        self.plant_ids = [plant['Id'] for plant in res]
        return res 
    
    def get_plant_elements(self, plant_id):
        """
        Get all elements associated to a specific PlantId.

        Args:
            plant_id (int): plant identifier (unique for each plant the user has access to)

        Returns:
            A list of dictionaries, each containing an element object.
        """
        res = self.make_api_call(f'/api/Plant/{plant_id}/Element', None)
        return res 
    
    def get_plant_datasources(self, plant_id):
        """
        Get all DataSources associated to a specific PlantId.

        Args:
            plant_id (int): plant identifier (unique for each plant the user has access to)

        Returns:
            A list of dictionaries, each containing a DataSource object.
        """
        res = self.make_api_call(f'/api/Plant/{plant_id}/Datasource', None)
        return res
    
    def get_plant_kpis(self, plant_id):
        """
        Get all KPIs associated to a specific PlantId.

        Args:
            plant_id (int): plant identifier (unique for each plant the user has access to)

        Returns:
            A list of dictionaries, each containing a KPI object.
        """
        res = self.make_api_call(f'/api/Plant/{plant_id}/KPI', None)
        return res
    
    def get_last_data(self, plant_id, datasourceids, tuples=False):
        """
        Gets last available measurements for the given DataSourceIds.

        Args:
            plant_id (int): plant identifier (unique for each plant the user has access to)
            datasourceids (list or dict): DataSourceIds to filter

        Returns:
            If 'datasourceids' is a list, it returns a list of records as per the 'format_last_data' 
            function in the data package. Else, if it's a dictionary containing DataSourceId lists 
            separated per concept, it returns a dictionary of measurements separated per concept.
        """
        res = self.make_api_call(f'/api/Plant/{plant_id}/LastData', None) 

        # TO DO: handle different errors
        if not res: return

        if type(datasourceids) == list:
            formatted_data = format_last_data(res, datasourceids, tuples=tuples)
        elif type(datasourceids) == dict:
            formatted_data = {}
            for concept, ds_ids in datasourceids.items():
                formatted_data[concept] = format_last_data(res, ds_ids, tuples=tuples)
        return formatted_data
    
    def get_last_data_for_all_plants(self):
        """
        Gets last available measurement for all DataSources and all plants. The 'self.datasources' object 
        must be initialized as a dictionary of the form:

            { plant_id: { 'concept': [datasource_ids] } }

        Returns:
            A dictionary containing all measurements per concept, PlantId. It has the form:

                { plant_id: { 'concept': [{'DataSourceId', 'Date', 'Value'}, ...] } }
        
        """
        data = {plant_id: {concept: [] for concept in self.concepts} for plant_id in self.plant_ids}

        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self.get_last_data, plant_id, self.datasources[plant_id]): plant_id for plant_id in self.plant_ids}

            for future in as_completed(futures):
                plant_id = futures[future]
                try:
                    result = future.result()
                    for key in result:
                        data[plant_id][key].extend(result[key])
                except Exception as exc:
                    print(f'PlantId {plant_id} generated an exception: {exc}')

        return data

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

        res = self.make_api_call('/api/DataList/v2', params)

        if res == 416: # HTTP 416 - Range not satisfiable
            midpoint = split_dates_in_half(start_date, end_date)
            first_half = self.get_data_list(datasources, start_date, midpoint, grouping, granularity)
            second_half = self.get_data_list(datasources, midpoint, end_date, grouping, granularity)
            return first_half + second_half if first_half and second_half else []
        
        return res

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
        expected_count = expected_measurement_count(start_date, end_date, grouping, granularity)

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