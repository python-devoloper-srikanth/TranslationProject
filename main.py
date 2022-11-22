# This is a sample Python script which processes the translation data
# Typer is a library for building CLI applications - easy to use, scalable 

from datetime import timedelta  # built in module
from pathlib import Path  # built in module

import pandas as pd  # installed module
import typer  # installed module


def main(translation_data_input_file_name: str, window_size: int):
    '''
    :param translation_data_input_file_name:
    :param window_size:
    :return: it prints the moving average data for every minute time stamp on the console
    '''
    # validate inputs
    if input_validation(translation_data_input_file_name, window_size) is False:
        return None

    # read json file and get dataframe
    df_translation_data = get_dataframe_from_json_file(translation_data_input_file_name)

    # consider only required columns for the processing
    df_translation_data = df_translation_data[['timestamp', 'duration']]

    # process the data in dataframe and calculate the moving average
    list_date_movingaverage = calculate_moving_average(df_translation_data, window_size)

    # print on console
    [print(dict_date_movingaverage) for dict_date_movingaverage in list_date_movingaverage]


def input_validation(translation_data_input_file_name: str, window_size: int):
    # input validation
    if Path(translation_data_input_file_name).exists() is False:
        print('given file is not there')
        return False

    if 'json' not in translation_data_input_file_name:
        print('given file is not json')
        return False

    if isinstance(window_size, int) is False:
        print('invalid window size is given')
        return False

    return True


def get_dataframe_from_json_file(translation_data_file_name: str):
    '''
    Reads the json file and returns the dataframe
    :param translation_data_file_name: 
    :return: 
    '''
    df_translation_data = pd.read_json(translation_data_file_name)
    return df_translation_data


def calculate_moving_average(df_translation_data: pd.DataFrame, window_size: int):
    '''
    this method calculates the moving average for every minute from start to end timestamp
    :param df_translation_data:
    :param window_size:
    :return: list of dictioneries with date and moving average
    '''
    # get start_date_time and end_date_time from dataframe
    # get rid of seconds and milli seconds
    start_date_time = df_translation_data['timestamp'].min().replace(second=0, microsecond=0)
    end_date_time = df_translation_data['timestamp'].max().replace(second=0, microsecond=0) + timedelta(minutes=1)

    list_date_movingaverage = []

    # loop through start date time to end date time
    # for every minute(from start_date_time to end_date_time)
    # calculating the average during for window size
    while start_date_time <= end_date_time:
        list_dates = []
        list_duration = []
        # get the
        for ind in df_translation_data.index:
            date_time = df_translation_data['timestamp'][ind]
            time_window_size = date_time + timedelta(minutes=window_size)
            if date_time < start_date_time and time_window_size > start_date_time:
                list_dates.append(date_time)
                list_duration.append(df_translation_data['duration'][ind])

        # calculate average
        average = sum(list_duration) / len(list_duration) if list_duration else 0

        dict_date_movingaverage = {}
        dict_date_movingaverage['date'] = start_date_time.strftime('%Y-%m-%d %H:%M:%S')
        dict_date_movingaverage['average_delivery_time'] = average
        list_date_movingaverage.append(dict_date_movingaverage)

        # increase a minute and update start_date_time
        start_date_time = start_date_time + timedelta(minutes=1)

    return list_date_movingaverage


if __name__ == "__main__":
    typer.run(main)
    # main('translation_input_data.json', 10)
