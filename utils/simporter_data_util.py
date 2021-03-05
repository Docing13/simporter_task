from typing import List, Dict

from pandas import (
    DataFrame,
    read_csv,
    to_datetime,
    Index,
    Grouper,
    concat)

import numpy as np

from constants.api_constants import (
    GROUPING_MONTHLY,
    GROUPING_WEEKLY,
    JSON_DATE,
    JSON_VALUE,
    TYPE_CUMULATIVE,
    TYPE_USUAL,
    GROUPING_BI_WEEKLY)

from constants.data_constants import (
    TIMESTAMP,
    DATE_LEN,
    SEPARATOR)


class SimporterDataUtil:
    __START_DATE_LIMIT = "2000-01-01"
    __END_DATE_LIMIT = "2100-01-01"

    @staticmethod
    def load_data_csv(path: str,
                      sep: str = SEPARATOR) -> DataFrame:

        data = read_csv(filepath_or_buffer=path, sep=sep)

        data[TIMESTAMP] = to_datetime(arg=data[TIMESTAMP], unit="s")
        return data

    @staticmethod
    def data_attrs(data: DataFrame) -> Index:
        return data.columns

    @staticmethod
    def filter_category_value(
            data: DataFrame,
            **attrs_values) -> DataFrame:

        filtered_data = data
        for attr_name, attr_value in attrs_values.items():

            if attr_value is None:
                continue
            else:
                if attr_value.isdigit():
                    attr_value = int(attr_value)

                filter_request = filtered_data[attr_name]
                filtered_data = filtered_data[filter_request == attr_value]

        return filtered_data

    @staticmethod
    def filter_date_range(
            data: DataFrame,
            date_start: str,
            date_end: str) -> DataFrame:

        if date_start is None:
            date_start = SimporterDataUtil.__START_DATE_LIMIT
        if date_end is None:
            date_end = SimporterDataUtil.__END_DATE_LIMIT

        mask = (data[TIMESTAMP] > date_start) & (data[TIMESTAMP] < date_end)

        return data.loc[mask]

    @staticmethod
    def timeline_group_data(
            data: DataFrame,
            group_type: str) -> List[DataFrame]:

        def group_bi_weekly(data_to_group: List[DataFrame]) -> List[DataFrame]:

            if len(data_to_group) % 2 != 0:
                data_to_group.pop(-1)
            first_weeks = data_to_group[0::2]
            second_weeks = data_to_group[1::2]
            bi_weeks = zip(first_weeks, second_weeks)

            data_to_group.clear()

            for first, second in bi_weeks:
                bi_week = concat([first, second])
                data_to_group.append(bi_week)

            return data_to_group

        if group_type == GROUPING_MONTHLY:
            freq = "M"
        elif group_type == GROUPING_WEEKLY or group_type == GROUPING_BI_WEEKLY:
            freq = "W"
        else:
            raise Exception(f"Not supported grouping type {group_type}")

        split_data = [group for _, group
                      in data.groupby(Grouper(key=TIMESTAMP, freq=freq))]

        if group_type == GROUPING_BI_WEEKLY:
            split_data = group_bi_weekly(split_data)

        return split_data

    @staticmethod
    def data_events_nums_with_dates(
            list_data: List[DataFrame],
            data_type: str) -> List[Dict[str, int]]:

        if data_type != TYPE_CUMULATIVE and data_type != TYPE_USUAL:
            raise Exception(f'Unsupported data type: {data_type}')

        event_date_list = []
        cumulative = 0

        for data in list_data:
            events_count = len(data)

            if events_count != 0:

                if data_type == TYPE_CUMULATIVE:
                    cumulative += events_count

                elif data_type == TYPE_USUAL:
                    cumulative = events_count

                # using zero index to get first date
                events_dates_str = data[TIMESTAMP].unique()[0].__str__()
                events_date = events_dates_str[:DATE_LEN]

                event_date_list.append({JSON_DATE: events_date,
                                        JSON_VALUE: cumulative})
        return event_date_list

    @staticmethod
    def list_date(data: np.ndarray) -> List[str]:
        dates = []
        for v in data:
            dates.append(v.__str__()[:DATE_LEN])
        return dates

    @staticmethod
    def group_type_data(data: DataFrame,
                        data_type: str) -> DataFrame:

        # TODO add processing for data by data type
        if data_type == TYPE_CUMULATIVE:
            pass
        elif data_type == TYPE_USUAL:
            pass
        else:
            raise Exception(f'This type is not supported: {data_type}')
        return data
