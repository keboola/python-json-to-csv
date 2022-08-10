from itertools import chain
from typing import Dict

from .configuration import ParserConfiguration


def normalize_parsed_data(configuration: ParserConfiguration, data: Dict) -> Dict:
    data = _fill_in_missing_keys_to_data(data)
    if configuration.table_column_mapping:
        data = _parse_data_with_specified_fields(configuration, data)
    else:
        data = _remove_parents_from_data(data)
    data = _normalize_headers(data)
    data = _fix_table_names(configuration, data)
    return data


def _remove_parents_from_data(csv_data):
    for data_name in csv_data:
        for i, row in enumerate(csv_data[data_name]):
            for key in list(csv_data[data_name][i].keys()):
                new_key = key.replace(f"{data_name}.", "")
                csv_data[data_name][i][new_key] = csv_data[data_name][i].pop(key)
    return csv_data


def _parse_data_with_specified_fields(configuration, csv_data):
    for data_name in csv_data:
        if current_table_fields := configuration.table_column_mapping.get(data_name):
            # TODO raise exception if current_table_fields does not exist
            for i, row in enumerate(csv_data[data_name]):
                for key in list(csv_data[data_name][i].keys()):
                    if key in current_table_fields:
                        csv_data[data_name][i][current_table_fields[key]] = csv_data[data_name][i].pop(key)
                    elif not configuration.ignore_undefined_columns:
                        new_key = key.replace(f"{data_name}.", "")
                        csv_data[data_name][i][new_key] = csv_data[data_name][i].pop(key)
                    else:
                        csv_data[data_name][i].pop(key)
    return csv_data


def _normalize_headers(csv_data):
    for data_name in csv_data:
        for i, row in enumerate(csv_data[data_name]):
            for key in list(row.keys()):
                csv_data[data_name][i][key.replace(".", "_")] = csv_data[data_name][i].pop(key)
    return csv_data


def _fix_table_names(configuration, csv_data):
    for data_name in list(csv_data.keys()):
        table_name = configuration.all_tables.get(data_name)
        if not table_name and configuration.ignore_undefined_tables:
            csv_data.pop(data_name)
            continue
        elif not table_name:
            table_name = f"{data_name.replace('.', '_')}.csv"
        csv_data[table_name] = csv_data.pop(data_name)
    return csv_data


def _fill_in_missing_keys_to_data(csv_data):
    for data_name in csv_data:
        keys = set(chain.from_iterable(csv_data[data_name]))
        for i, item in enumerate(csv_data[data_name]):
            csv_data[data_name][i].update({key: None for key in keys if key not in item})
    return csv_data
