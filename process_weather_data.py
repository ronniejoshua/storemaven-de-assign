#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import concurrent.futures
from functools import partial
import time

# Importing the CSV. Importing relevant columns and converting each column to its actual
# actual data type significantly improves processing performance.

relevant_cols = [
    'postal_code',
    'date',
    'avg_temperature_air_2m_f',
    'avg_humidity_relative_2m_pct'
]

df = pd.read_csv(
    filepath_or_buffer='w_data.csv',
    parse_dates=True,
    infer_datetime_format=True,
    index_col=['postal_code', 'date'],
    usecols=relevant_cols,
    dtype={
        'avg_temperature_air_2m_f': np.float64,
        'avg_humidity_relative_2m_pct': np.int32}
)

# Generating the first order difference and renaming the columns
df = df.sort_index(ascending=True, inplace=False)
rdf = df.groupby(['postal_code'])[['avg_temperature_air_2m_f', 'avg_humidity_relative_2m_pct']].diff(1)
rdf.columns = ['delta_tempreture_previous_day', 'delta_humidity_previous_day']


# As there are 3000 plus files need to be generated and uploaded we leverage multiprocessing
# generating chunks of postal code to process at a time and making sure we don't repeat processing
# it again that's why we create a lookup dictionary to log finished tasks.

def create_potal_code_chunks(result_df: pd.DataFrame, chunk_size):
    postal_code_dict = dict()
    postal_codes = list(result_df.index.unique(level='postal_code'))
    postal_codes = list(map(str, postal_codes))
    chunks = [postal_codes[x:x + chunk_size] for x in range(0, len(postal_codes), chunk_size)]
    for chunk in chunks:
        lookupkey = f'{chunk[0]}-{chunk[-1]}'
        postal_code_dict[lookupkey] = chunk
    print(len(postal_code_dict.keys()))
    return postal_code_dict


postal_code_keys = create_potal_code_chunks(rdf, 100)
# we could have also write the output to a file and then read from there
# to pick from where we left. This is widely used in data engineering.
# defining a processing state
processed_chunk = dict()

for postal_code_key in postal_code_keys.keys():
    # print(postal_code_key)

    # checks if the chuck was already processed if it has than doesn't reprocess it
    # which makes the code rerun from any point

    if not processed_chunk.get(postal_code_key):
        list_postal_codes = postal_code_keys.get(postal_code_key)
        print(list_postal_codes)
        sdf = rdf.loc[df.index.isin(list_postal_codes, level=0)]

        # file generating function. I wanted to partial to fix the argument for `sdf`
        # from the previous step but for some reason it was able to run as part of
        # executor.map, hence i haven't isolate the function.
        # I pass the sdf as default argument. Also I haven't converted the date in to pandas object
        # data type as it would consume more memory
        def p_generate_files(postal_code: str, result_df: pd.DataFrame = sdf) -> None:
            tmp_df = result_df.filter(like=str(postal_code), axis=0)
            start_date = tmp_df.index.unique(level='date').min().to_pydatetime().strftime('%b').upper()
            end_date = tmp_df.index.unique(level='date').max().to_pydatetime().strftime('%b').upper()
            file_name_path = f'data/COVID-WEATHER-{postal_code}-{start_date}-{end_date}-2020.json'
            tmp_df.reset_index(inplace=True)

            tmp_df.to_json(
                file_name_path,
                orient="records",
                index=True,
                lines=True,
                date_format='iso')


        with concurrent.futures.ProcessPoolExecutor() as executor:
            executor.map(p_generate_files, list_postal_codes)

        time.sleep(15)

        processed_chunk[postal_code_key] = f"Successful Processed {postal_code_key}"
    else:
        print(processed_chunk[postal_code_key])
