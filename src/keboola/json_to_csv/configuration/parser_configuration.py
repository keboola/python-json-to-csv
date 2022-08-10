import json
from enum import Enum, auto
from typing import List, Dict


class ParseListStrategy(Enum):
    TO_STR = auto()
    TO_ROWS = auto()
    TO_COLUMNS = auto()


class ParseDictStrategy(Enum):
    TO_STR = auto()
    TO_COLUMNS = auto()


def convert_str_to_parse_list_option(input_str: str) -> ParseListStrategy:
    if input_str.lower() == "to_str":
        return ParseListStrategy.TO_STR
    elif input_str.lower() == "to_rows":
        return ParseListStrategy.TO_ROWS
    elif input_str.lower() == "to_columns":
        return ParseListStrategy.TO_COLUMNS


def convert_str_to_parse_dict_option(input_str: str) -> ParseDictStrategy:
    if input_str.lower() == "to_str":
        return ParseDictStrategy.TO_STR
    elif input_str.lower() == "to_columns":
        return ParseDictStrategy.TO_COLUMNS


DEFAULT_PARSE_LIST = ParseListStrategy.TO_ROWS
DEFAULT_PARSE_DICT = ParseDictStrategy.TO_COLUMNS


class ParserConfigurationError(Exception):
    pass


class ParserConfiguration:
    """
            Args:
                json_configuration_file:
                    Optional[str] : Path to JSON configuration file for the JSONParser settings
                configuration_dict:
                    Optional[Dict] : Configuration of the JSONParser settings in Dict format
                parent_table:
                    Dict : Parent table definition containing the key value pair of parent object name and the
                    resulting csv name. e.g. {"parent_object": "parent_table_name.csv"}
                child_tables:
                    Optional[dict]: Child table definitions containing the key value pairs of child object and their
                    corresponding csv name. Keys being the object name (with all parent objects separated by periods),
                    and the value being the name of the output table. e.g. {"parent_object.child_object": "child.csv"}
                table_primary_keys:
                    Optional[dict]: definition of all primary keys of all tables. Key value pairs where
                    Keys being the object name (with all parent objects separated by periods) and values being
                    key value pairs of objects that should be primary keys of the table and values being the resulting
                    names of the columns in the CSV
                    e.g. {"order.order-item" : {"order.id" : "order_id", "order.order-items.item_id" : "item_id"}}
                table_column_mapping:
                    Optional[Dict[str, Dict[str, str]]] : definition of objects is tables and their mapping to columns.
                    The input is a dictionary where the keys are a parent table object or a child table object and the
                    values are dictionaries holding key value pairs of the elements within the objects and the values
                    being the resulting column names.
                    e.g. {'parent_object': {'parent_object.element': 'element_column_name'}}
                root_node:
                    Optional[str] : name of any root nodes of the data e.g. if data is:
                    {"root_el": {"orders": {"order": [{}]}}}, then root_node should be root_el.orders.order
                default_parse_list:
                    Optional[ParseListStrategy] : The default strategy of the parser to parse lists
                default_parse_dict:
                    Optional[ParseDictStrategy] : The default strategy of the parser to parse dictionaries
                parse_list_mapping:
                    Optional[Dict[str,ParseListStrategy]] :  A dictionary holding key value pairs, where the keys are
                    specific elements that are Lists in the JSON data and values being their Parsing strategy.
                    This is used when a specific list should be parsed differently than the default strategy.
                parse_dict_mapping:
                    Optional[Dict[str,ParseDictStrategy]] :  A dictionary holding key value pairs, where the keys are
                    specific elements that are Dictionaries in the JSON data and values being their Parsing strategy.
                    This is used when a specific dictionary should be parsed differently than the default strategy.
                ignore_undefined_tables:
                    Optional[bool] : If True, tables that are not defined in child or parent table definitions will not
                     be processed or output by the parser.
                ignore_undefined_columns:
                    Optional[bool] : If True, columns that are not defined in table column mapping will not be
                    processed or output by the parser.
                always_array:
                    Optional[List] : List of element names that should always be treated as arrays/lists even though
                    they might sometimes be of a different.
            Raises:
                JSONParserError - on parsing errors.
            """

    def __init__(self,
                 json_configuration_file: str = None,
                 configuration_dict: Dict = None,
                 parent_table: Dict[str, str] = None,
                 child_tables: Dict[str, str] = None,
                 table_primary_keys: Dict[str, List[str]] = None,
                 table_column_mapping: Dict[str, Dict[str, str]] = None,
                 root_node: str = "",
                 default_parse_list: ParseListStrategy = None,
                 default_parse_dict: ParseDictStrategy = None,
                 parse_list_mapping: Dict[str, ParseListStrategy] = None,
                 parse_dict_mapping: Dict[str, ParseDictStrategy] = None,
                 ignore_undefined_tables: bool = False,
                 ignore_undefined_columns: bool = False,
                 always_array: List = None) -> None:

        self.parent_table = parent_table
        self.child_tables = child_tables
        self.table_primary_keys = table_primary_keys
        self.root_node = root_node
        self.table_column_mapping = table_column_mapping
        self.default_parse_list = default_parse_list
        self.default_parse_dict = default_parse_dict
        self.parse_list_mapping = parse_list_mapping
        self.parse_dict_mapping = parse_dict_mapping
        self.ignore_undefined_tables = ignore_undefined_tables
        self.ignore_undefined_columns = ignore_undefined_columns
        self.always_array = always_array

        if self.parent_table:
            self.all_tables = self.parent_table.copy()
        if self.child_tables and self.parent_table:
            self.all_tables.update(self.child_tables)

        self._set_variables(json_configuration_file, configuration_dict)

    def _set_variables(self, json_file, configuration_dict):
        if json_file:
            self._set_variables_from_json_file(json_file)
        elif configuration_dict:
            self._set_variables_from_dict(configuration_dict)

    def _set_variables_from_json_file(self, json_file_path):
        with open(json_file_path) as json_file:
            config = json.load(json_file)
        self._set_variables_from_dict(config)

    def _set_variables_from_dict(self, configuration_dict):
        self.parent_table = configuration_dict.get("parent_table")
        if not self.parent_table:
            self.parent_table = {}
        self.child_tables = configuration_dict.get("child_tables")
        if not self.child_tables:
            self.child_tables = {}
        self.table_primary_keys = configuration_dict.get("table_primary_keys")
        if not self.table_primary_keys:
            self.table_primary_keys = {}
        self.root_node = configuration_dict.get("root_node")

        self.table_column_mapping = configuration_dict.get("table_column_mapping")

        self.all_tables = self.parent_table.copy()
        if self.child_tables:
            self.all_tables.update(self.child_tables)

        default_parse_list = configuration_dict.get("default_parse_list")
        if default_parse_list:
            default_parse_list = convert_str_to_parse_list_option(default_parse_list)
        default_parse_dict = configuration_dict.get("default_parse_dict")
        if default_parse_dict:
            default_parse_dict = convert_str_to_parse_dict_option(default_parse_dict)
        parse_list_mapping = configuration_dict.get("parse_list_mapping")
        if parse_list_mapping:
            parse_list_mapping = self.map_list_parsing_options(parse_list_mapping)
        parse_dict_mapping = configuration_dict.get("parse_dict_mapping")
        if parse_dict_mapping:
            parse_dict_mapping = self.map_dict_parsing_options(parse_dict_mapping)

        self.default_parse_list = default_parse_list or DEFAULT_PARSE_LIST
        self.default_parse_dict = default_parse_dict or DEFAULT_PARSE_DICT
        self.parse_list_mapping = parse_list_mapping
        self.parse_dict_mapping = parse_dict_mapping

        self.ignore_undefined_tables = configuration_dict.get("ignore_undefined_tables")
        self.ignore_undefined_columns = configuration_dict.get("ignore_undefined_columns")

        self.always_array = configuration_dict.get("always_array")

    @staticmethod
    def map_list_parsing_options(parse_list_mapping):
        for key in parse_list_mapping:
            parse_list_mapping[key] = convert_str_to_parse_list_option(parse_list_mapping[key])
        return parse_list_mapping

    @staticmethod
    def map_dict_parsing_options(parse_dict_mapping):
        for key in parse_dict_mapping:
            parse_dict_mapping[key] = convert_str_to_parse_dict_option(parse_dict_mapping[key])
        return parse_dict_mapping
