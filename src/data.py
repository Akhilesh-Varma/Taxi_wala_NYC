from pathlib import Path
import requests
import pandas as pd
from tqdm import tqdm
import numpy as np
import os
from datetime import datetime, timedelta
from typing import Optional, List

from src.paths import RAW_DATA_DIR, TRANSFORMED_DATA_DIR

def download_one_file_of_raw_data(year :int, month:int) -> Path:

    """
    Download Parquet file with historical taxi rides for the given 'year' and 'month'
    
    """
    URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    response = requests.get(URL)

    if response.status_code == 200:
        path = RAW_DATA_DIR  / f'rides_{year}-{month:02d}.parquet'
        open(path,'wb').write(response.content)
        return path

    else:
        raise Exception(f'{URL} is not available' )



def validate_raw_data(
    rides:pd.DataFrame,
    year:int,
    month:int,

) -> pd.DataFrame:
    """
    Removes rows with pickup_date outside their valid range
    """

    # keep rides for this month only
    this_month_start = f'{year}-{month:02d}-01'
    next_month_start = f'{year}-{month+1:02d}-01' if month<12 else f'{year+1}-01-01'
    rides = rides[rides.pickup_datetime >= this_month_start]
    rides = rides[rides.pickup_datetime < next_month_start]

    return rides


def load_raw_data(
    year:int,
    months: Optional[List[int]] = None):
        """
        
        """
        rides = pd.DataFrame()

        if months is None:
            #download data only for months that are specified by 'months'
            months = list(range(1,13))
        elif isinstance(months, int):
            # download data for entire year (all months)
            months = [months]

        for month in months:
            local_file = RAW_DATA_DIR / f'rides_{year}-{month:02d}.parquet'

            if not local_file.exists():
                try:

                    # download file from NYC website
                    print(f'Downloading file {year}-{month:02d}')
                    download_one_file_of_raw_data(year, month)

                except:
                    print(f'{year}-{month:02d} file is not available')
                    continue

            else:
                print(f'File {year}-{month:02d} was already in the local storage')


            #load file into Pandas
            rides_one_month= pd.read_parquet(local_file)

            # rename cols

            rides_one_month = rides_one_month[['tpep_pickup_datetime','PULocationID']]

            rides_one_month.rename(columns = {
                'tpep_pickup_datetime' : 'pickup_datetime',
                'PULocationID' : 'pickup_location_id'
            }, inplace=True)

            #validate the file 
            rides_one_month = validate_raw_data(rides_one_month,year, month)


            # appending to existing data
            rides = pd.concat([rides, rides_one_month])

        # keep only time and origin of ride

        rides = rides[['pickup_datetime','pickup_location_id']]

        return rides


