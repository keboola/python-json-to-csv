from enum import Enum, auto
from typing import List, Dict
from .configuration import ParseListStrategy, ParseDictStrategy, ParserConfiguration
from .value_object import Column


class ObjectParseStrategy(Enum):
    CHILD_TABLE = "list of dictionaries"
    DICT_TO_COLUMNS = "dictionary (to columns)"
    DICT_TO_STRING = "dictionary (to string)"
    UNDEFINED_CHILD_TABLE_TO_IGNORE = "ignored table"
    LIST_TO_COLUMNS = "list (to columns)"
    LIST_TO_ROWS = "list (to rows)"
    LIST_TO_STRING = "list (to string)"
    TO_STRING_COLUMN = "simple datatype"


def get_parse_strategy(column: Column, parser_config: ParserConfiguration) -> ObjectParseStrategy:
    object_is_dict = _object_is_dict(column.data)
    object_is_list = _object_is_list(column.data)

    always_array = parser_config.always_array
    column_is_array = bool(always_array and column.object_name in always_array)

    mapping_direction = _get_mapping_direction(parser_config, column)

    parse_strategy = ObjectParseStrategy.TO_STRING_COLUMN

    if _is_object_child_table(column, parser_config.all_tables):
        parse_strategy = ObjectParseStrategy.CHILD_TABLE
    elif object_is_dict and column_is_array and not parser_config.ignore_undefined_tables:
        parse_strategy = ObjectParseStrategy.CHILD_TABLE
    elif _is_list_of_dicts(column.data) and parser_config.ignore_undefined_tables:
        parse_strategy = ObjectParseStrategy.UNDEFINED_CHILD_TABLE_TO_IGNORE
    elif _is_list_of_dicts(column.data) and not parser_config.ignore_undefined_tables:
        parse_strategy = ObjectParseStrategy.CHILD_TABLE
    elif object_is_dict and mapping_direction == ParseDictStrategy.TO_COLUMNS:
        parse_strategy = ObjectParseStrategy.DICT_TO_COLUMNS
    elif object_is_dict and mapping_direction == ParseDictStrategy.TO_STR:
        parse_strategy = ObjectParseStrategy.DICT_TO_STRING
    elif (object_is_list or column_is_array) and mapping_direction == ParseListStrategy.TO_COLUMNS:
        parse_strategy = ObjectParseStrategy.LIST_TO_COLUMNS
    elif (object_is_list or column_is_array) and mapping_direction == ParseListStrategy.TO_ROWS:
        parse_strategy = ObjectParseStrategy.LIST_TO_ROWS
    elif (object_is_list or column_is_array) and mapping_direction == ParseListStrategy.TO_STR:
        parse_strategy = ObjectParseStrategy.LIST_TO_STRING
    return parse_strategy


def _get_mapping_direction(parser_config, column):
    parse_list_mapping = parser_config.parse_list_mapping
    mapping_direction = ""
    parse_dict_mapping = parser_config.parse_dict_mapping
    if parse_dict_mapping and column.object_name in list(parse_dict_mapping.keys()):
        mapping_direction = parse_dict_mapping[column.object_name]
    elif parse_list_mapping and column.object_name in list(parse_list_mapping.keys()):
        mapping_direction = parse_list_mapping[column.object_name]
    elif parse_list_mapping and column.object_name in list(parse_list_mapping.keys()):
        mapping_direction = parse_list_mapping[column.object_name]
    elif _object_is_dict(column.data):
        mapping_direction = parser_config.default_parse_dict
    elif _object_is_list(column.data):
        mapping_direction = parser_config.default_parse_list

    return mapping_direction


def _object_is_dict(_object) -> bool:
    return isinstance(_object, Dict)


def _object_is_list(_object) -> bool:
    return isinstance(_object, List)


def _is_object_child_table(column: Column, all_tables) -> bool:
    return column.object_name in list(all_tables.keys())


def _is_list_of_dicts(object_) -> bool:
    return all(isinstance(i, dict) for i in object_) if isinstance(object_, List) else False
