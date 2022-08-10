import logging
import json
import hashlib
from typing import List, Dict
from typing import Optional, Union
from .data_utils import normalize_parsed_data
from .configuration import ParserConfiguration
from .parse_strategy import ObjectParseStrategy, get_parse_strategy, ParseListStrategy, ParseDictStrategy
from .value_object import Column, Key
from .exception import InvalidRootNodeException, MissingDataException, InconsistentDataTypesException


class JSONParserError(Exception):
    pass


class JSONParser:
    def __init__(self,
                 json_configuration_file=None,
                 configuration_dict=None,
                 parent_table: Dict[str, str] = None,
                 table_primary_keys: Dict[str, List[str]] = None,
                 child_tables: Dict[str, str] = None,
                 table_column_mapping: Dict[str, Dict[str, str]] = None,
                 root_node: str = "",
                 default_parse_list: ParseListStrategy = None,
                 default_parse_dict: ParseDictStrategy = None,
                 parse_list_mapping=None,
                 parse_dict_mapping=None,
                 ignore_undefined_tables=False,
                 ignore_undefined_columns=False,
                 always_array=None) -> None:

        self.configuration = ParserConfiguration(json_configuration_file=json_configuration_file,
                                                 configuration_dict=configuration_dict,
                                                 parent_table=parent_table,
                                                 table_primary_keys=table_primary_keys,
                                                 child_tables=child_tables,
                                                 table_column_mapping=table_column_mapping,
                                                 root_node=root_node,
                                                 default_parse_list=default_parse_list,
                                                 default_parse_dict=default_parse_dict,
                                                 parse_list_mapping=parse_list_mapping,
                                                 parse_dict_mapping=parse_dict_mapping,
                                                 ignore_undefined_tables=ignore_undefined_tables,
                                                 ignore_undefined_columns=ignore_undefined_columns,
                                                 always_array=always_array)
        self.warnings = {}
        self.processed_columns = {}

    def parse_data(self, data: Dict) -> Dict:
        """

        Args:
            data (Dict): A dictionary containing the data to be parsed.

        Returns:
            parsed_data (Dict) : dictionary of parsed data with key value pairs, where keys are names of csv files, and
            values are lists of flat dictionaries.

        """
        self._reset_warnings()
        self._reset_processed_columns()
        data_to_parse = self._get_data_to_parse(data, self.configuration.root_node)
        parsed_data = {}
        for row in data_to_parse:
            parsed_row = self._parse_row_to_tables(row)
            for table_name in parsed_row:
                if table_name not in parsed_data:
                    parsed_data[table_name] = []
                parsed_data[table_name].extend(parsed_row[table_name])
        self._log_warnings()
        parsed_data = normalize_parsed_data(self.configuration, parsed_data)
        return parsed_data

    def _get_primary_keys(self, data: Dict, parent_table: str) -> List[Key]:
        primary_key_definitions = self.configuration.table_primary_keys.get(parent_table)
        if primary_key_definitions:
            primary_keys = self._get_primary_key_values(primary_key_definitions, data, parent_table)
        else:
            self.warnings[f"{parent_table}.pkey"] = f"Object {parent_table} does not have primary keys set, " \
                                                    f"generating them automatically"
            primary_keys = [self._generate_primary_key_from_data(parent_table, data)]
        return primary_keys

    @staticmethod
    def _get_primary_key_values(primary_key_definitions, data, parent_table):
        primary_keys = []
        key_prefix = "".join([parent_table, "."])
        for primary_key_definition in primary_key_definitions:
            if primary_key_definition.replace(key_prefix, "") in data:
                key_data = data[primary_key_definition.replace(key_prefix, "")]
                primary_key = Key(name=primary_key_definition, value=key_data, table=parent_table)
                primary_keys.append(primary_key)
        return primary_keys

    def _generate_primary_key_from_data(self, parent_table: str, data: Dict) -> Key:
        hash_key = "generated_pkey"
        if parent_table:
            hash_key = ".".join([parent_table, hash_key])
        hash_value = f"key_{self._hash_data_row(data)}"
        return Key(name=hash_key, value=hash_value, table=parent_table)

    @staticmethod
    def _hash_data_row(data_row: Dict) -> str:
        dict_str = json.dumps(data_row, sort_keys=True, ensure_ascii=True, default=str)
        return hashlib.sha256(dict_str.encode("utf-8")).hexdigest()

    def _parse_row_to_tables(self, data_object: Dict) -> Dict:
        row_data = {}
        parent_table = list(self.configuration.parent_table.keys())[0]
        row_data = self._parse_nested_dict(row_data, data_object, parent_table)
        return row_data

    def _parse_nested_dict(self, row_data: Dict, data: Dict, parent_table: str = "", table_index: int = 0,
                           foreign_keys: Optional[List[Key]] = None) -> Dict:

        if not foreign_keys:
            foreign_keys = []
        primary_keys = self._get_primary_keys(data, parent_table)

        if parent_table not in row_data:
            row_data[parent_table] = []

        for index, object_name in enumerate(data):
            if self._turn_dict_to_list(object_name, parent_table, data[object_name]):
                data[object_name] = [data[object_name]]
            column = Column(object_name, parent_table, data[object_name])

            parse_strategy = get_parse_strategy(column, self.configuration)
            self._check_object_consistency(column, parse_strategy)
            row_data = self._process_object(row_data, index, column, parse_strategy, primary_keys, foreign_keys,
                                            table_index)

        for i, primary_key in enumerate(primary_keys):
            if primary_key.name not in self.configuration.table_primary_keys.get(parent_table, []):
                primary_key_column = Column("generated_pkey", parent_table, primary_keys[i].value)
                row_data = self._parse_primitive(row_data, primary_key_column, 1, foreign_keys)

        return row_data

    def _check_object_consistency(self, column: Column, parse_strategy: ObjectParseStrategy) -> None:
        if column.object_name not in list(self.processed_columns.keys()):
            self.processed_columns[column.object_name] = parse_strategy
        elif self.processed_columns[column.object_name] != parse_strategy:
            raise InconsistentDataTypesException(column=column,
                                                 parse_strategy=parse_strategy,
                                                 processed_columns=self.processed_columns)

    def _process_object(self, row_data: Dict, index: int, column: Column, parse_strategy: ObjectParseStrategy,
                        primary_keys: List[Key], foreign_keys: List[Key], table_index: int) -> Dict:

        if parse_strategy == ObjectParseStrategy.CHILD_TABLE:
            row_data = self._parse_list_of_dicts(row_data, column, primary_keys, foreign_keys)
        elif parse_strategy == ObjectParseStrategy.DICT_TO_COLUMNS:
            row_data = self._flatten_simple_dict(row_data, column, table_index, foreign_keys)
        elif parse_strategy == ObjectParseStrategy.DICT_TO_STRING:
            column.data = json.dumps(column.data)
            row_data = self._parse_primitive(row_data, column, index, foreign_keys)
        elif parse_strategy == ObjectParseStrategy.UNDEFINED_CHILD_TABLE_TO_IGNORE:
            self.warnings[column] = f'Warning : Possible table "{column}" will be ignored as it is not specified ' \
                                    "in the configuration of the parser."
        elif parse_strategy == ObjectParseStrategy.LIST_TO_COLUMNS:
            column_list = [Column(f"{column.name}.{str(i)}", column.table, item) for i, item in
                           enumerate(column.data)]
            for column_row in column_list:
                row_data = self._parse_primitive(row_data, column_row, index, foreign_keys)
        elif parse_strategy == ObjectParseStrategy.LIST_TO_ROWS:
            if column.object_name not in row_data:
                row_data[column.object_name] = []
            for column_data in column.data:
                sub_column = Column(column.name, column.object_name, column_data)
                row_data = self._parse_primitive(row_data, sub_column, 0, primary_keys)
        else:
            row_data = self._parse_primitive(row_data, column, index, foreign_keys)
        return row_data

    def _parse_list_of_dicts(self, row_data: Dict, column: Column, primary_keys: List[Key],
                             foreign_keys: List[Key]) -> Dict:
        primary_keys.extend(foreign_keys)
        for index, d in enumerate(column.data):
            row_data = self._parse_nested_dict(row_data, d, column.object_name, foreign_keys=primary_keys,
                                               table_index=index)
        return row_data

    @staticmethod
    def _parse_primitive(row_data: Dict, column: Column, index: int, foreign_keys: List[Key]) -> Dict:

        if column.table not in row_data:
            row_data[column.table] = []

        if index == 0:
            row_data[column.table].append({})

        table_size = len(row_data[column.table])
        if foreign_keys:
            for foreign_key in foreign_keys:
                row_data[column.table][table_size - 1][foreign_key.name] = foreign_key.value
        row_data[column.table][table_size - 1][column.object_name] = column.data
        return row_data

    @staticmethod
    def _flatten_simple_dict(row_data: Dict, column: Column, index: int, foreign_keys: List[Key]) -> Dict:
        for d_key in column.data:
            new_key = f"{column.object_name}.{d_key}"
            if len(row_data[column.table]) < index + 1:
                row_data[column.table].append({})
            row_data[column.table][index][new_key] = column.data[d_key]

        if foreign_keys:
            for foreign_key in foreign_keys:
                row_data[column.table][index][foreign_key.name] = foreign_key.value
        return row_data

    def _column_is_always_array(self, parent_table: str, column: str) -> bool:
        column_definition = f"{parent_table}.{column}"
        if column_definition in list(self.configuration.all_tables.keys()):
            return True
        always_array = self.configuration.always_array
        if not always_array:
            return False
        return bool(always_array and column_definition in always_array)

    @staticmethod
    def _get_data_to_parse(data: Dict, root_node: str) -> List[Dict]:
        root_nodes = root_node.split(".")
        if len(root_nodes) == 1 and not root_nodes[0]:
            raise InvalidRootNodeException()
        try:
            for root_node in root_nodes:
                data = data.get(root_node)
        except AttributeError as attr_err:
            raise InvalidRootNodeException() from attr_err

        if not data:
            raise MissingDataException()

        # TODO turn dict to list
        if not isinstance(data, List):
            raise JSONParserError(
                "Invalid root node. Data extracted from JSON using the root node should be a list of dictionaries.")
        return data

    # TODO rename to something more understandable
    def _turn_dict_to_list(self, object_name: str, parent_table: str, object_data: Union[List, Dict]) -> bool:
        return bool(not isinstance(object_data, List) and self._column_is_always_array(parent_table, object_name))

    def _reset_warnings(self) -> None:
        self.warnings = {}

    def _reset_processed_columns(self) -> None:
        self.processed_columns = {}

    def _log_warnings(self) -> None:
        for warning in self.warnings:
            logging.warning(self.warnings[warning])
