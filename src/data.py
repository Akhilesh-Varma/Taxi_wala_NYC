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




def add_missing_slots(agg_rides : pd.DataFrame) ->pd.DataFrame:
    location_ids = agg_rides['pickup_location_id'].unique()
    full_range_dates = pd.date_range(
        agg_rides['pickup_hour'].min(), agg_rides['pickup_hour'].max (), freq = 'H'
    )

    output = pd.DataFrame()
    for location_id in location_ids:

        # keep only rides for this location ids
        agg_rides_i = agg_rides.loc[agg_rides['pickup_location_id']== location_id, ['pickup_hour', 'rides']]

        # fill 0's
        agg_rides_i.set_index('pickup_hour', inplace=True)
        agg_rides_i.index = pd.DatetimeIndex(agg_rides_i.index)
        agg_rides_i = agg_rides_i.reindex(full_range_dates, fill_value=0)

        # add back location_id columns
        agg_rides_i['pickup_location_id'] = location_id

        output = pd.concat([output, agg_rides_i])


    # move purchase_day from index to dataframe column

    output = output.reset_index().rename(columns={'index':"pickup_hour"})

    return output




def transform_raw_data_into_ts_data(
    rides: pd.DataFrame) -> pd.DataFrame:
    """

    """

    # sum rides per locationa and per hour

    
    rides['pickup_hour'] = rides['pickup_datetime'].dt.floor('H')
    agg_rides = rides.groupby(['pickup_hour', 'pickup_location_id']).size().reset_index()
    print(agg_rides.head())
    print(agg_rides.columns)
    agg_rides = agg_rides.rename(columns={0:'rides'})

    print(agg_rides.columns)
    # add rows for (location , pickup_hours) with 0 rides 
    print(agg_rides.shape)
    agg_rides_all_slots  = add_missing_slots(agg_rides)

    return agg_rides_all_slots


def get_cutoff_indices_features_target(
    data:pd.DataFrame,
    n_features:int,
    step_size:int
) ->list:

    stop_position = len(data) - 1

    # start the first sub-sequence at index postion 0
    subseq_first_idx= 0
    subseq_mid_idx = n_features
    subseq_last_idx = n_features +1

    indices = []

    while subseq_last_idx <= stop_position:
        indices.append((subseq_first_idx, subseq_mid_idx, subseq_last_idx))

        subseq_first_idx += step_size
        subseq_mid_idx += step_size
        subseq_last_idx += step_size
    
    return indices


def transform_ts_data_into_features_and_target(
    ts_data: pd.DataFrame, 
    input_seq_len: int, 
    step_size: int) -> pd.DataFrame:
    """
    Slices and transposes data from time-series format to a (features, target)
    format that we can use to train Supervised ML models.
    """

    assert set(ts_data.columns) == {'pickup_hour', 'rides', 'pickup_location_id'}
    location_ids = ts_data['pickup_location_id'].unique()
    features = pd.DataFrame()
    targets = pd.DataFrame()
    for location_id in tqdm(location_ids):
        ts_data_one_location = ts_data.loc[ts_data.pickup_location_id == location_id,
                                           ['pickup_hour', 'rides']]

        indices = get_cutoff_indices_features_target(
            ts_data_one_location,
            input_seq_len,
            step_size
        )

        n_examples = len(indices)
        x = np.ndarray(shape=(n_examples, input_seq_len), dtype=np.float32)
        y = np.ndarray(shape=(n_examples), dtype=np.float32)
        pickup_hours = []
        for i, idx in enumerate(indices):
            x[i, :] = ts_data_one_location.iloc[idx[0]:idx[1]]['rides'].values
            y[i] = ts_data_one_location.iloc[idx[1]:idx[2]]['rides'].values
            pickup_hours.append(ts_data_one_location.iloc[idx[1]]['pickup_hour'])

        # numpy -> pandas
        features_one_location = pd.DataFrame(
            x,
            columns=[f'rides_previous_{i+1}_hour' for i in reversed(range(input_seq_len))]
        )

        features_one_location['pickup_hour'] = pickup_hours
        features_one_location['pickup_location_id'] = location_id

        # numpy -> pandas
        targets_one_location = pd.DataFrame(y, columns=['target_rides_next_hour'])

        # concatenate results
        features = pd.concat([features, features_one_location])
        targets = pd.concat([targets, targets_one_location])

    features.reset_index(inplace=True, drop=True)
    targets.reset_index(inplace=True, drop=True)

    return features, targets['target_rides_next_hour']

