import requests
from airflow.exceptions import AirflowException

def fetch_data(config, **kwargs):
    lat = config['location']['lat']
    lon = config['location']['lon']
    dist = config['location']['radius_nm']
    
    url = f"https://opendata.adsb.fi/api/v3/lat/{lat}/lon/{lon}/dist/{dist}"
    
    response = requests.get(url)
    
    if response.status_code in [400, 401, 403, 404, 429]:
        raise AirflowException(f"API Request failed with status {response.status_code}. To avoid IP bans from adsb.fi, the task is aborted.")
        
    response.raise_for_status()
    
    data = response.json()
    
    ti = kwargs['ti']
    ti.xcom_push(key='raw_data', value=data)
