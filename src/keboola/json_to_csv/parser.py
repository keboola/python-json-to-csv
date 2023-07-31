import hashlib
from csv import DictWriter
from typing import Any, Dict, List, Optional, Union

from .analyzer import Analyzer
from .csv_row import CsvRow
from .mapping import TableMapping
from .node import NodeType
from .table import Table
from .utils import is_dict, is_list, is_scalar
from .exceptions import JsonParserException


class Parser:
    """
    JSON to CSV Parser.

    This class is responsible for converting JSON data to CSV files based on a specified table mapping.
    It provides methods for parsing JSON data, analyzing it to generate table mappings, and saving the
    parsed CSV data to files.

    """

    def __init__(self,
                 main_table_name: Optional[str] = None,
                 table_mapping: Optional[TableMapping] = None,
                 analyze_further: bool = True) -> None:
        """
        Initialize the JSON Parser.

        Parameters:
            main_table_name (Optional[str]): The name of the main table to use for the CSV files.
            table_mapping (Optional[TableMapping]): The mapping of tables and their columns, primary keys,
                                                    and force types.
            analyze_further (bool): If True, performs further analysis of data that is not specified
                                    in the table mapping.
        """
        self.analyze_further = analyze_further
        self.main_table_name = main_table_name
        self.table_mapping = table_mapping

        self._csv_file_results = {}
        self.analyzer = Analyzer(root_name=main_table_name, table_mapping=table_mapping)

    def parse_data_to_csv(self,
                          input_data: Union[Dict, List[Dict]],
                          destination_path: str,
                          root_name: Optional[str] = None) -> None:
        """
        Parse input data to CSV files and save them to the specified destination path.

        Parameters:
            input_data (Union[Dict, List[Dict]]): The JSON data to parse.
            destination_path (str): The path to save the generated CSV files.
            root_name (Optional[str]): The root node name to use for parsing the data (default: None).
        """
        data_to_parse = self._get_parseable_data_from_input_data(input_data, root_name)
        parsed_data = self._parse_data(data_to_parse)
        for table_id, table in parsed_data.items():
            with open(f"{destination_path}/{table.name}.csv", "w") as outfile:
                writer = DictWriter(outfile, table.headers)
                writer.writeheader()
                writer.writerows(table.rows)

    def parse_data(self, input_data: Union[Dict, List[Dict]], root_name: Optional[str] = None) -> Dict:
        """
        Parse input data and return a dictionary of CSV rows as flat dictionaries.

        Parameters:
            input_data (Union[Dict, List[Dict]]): The JSON data to parse.
            root_name (Optional[str]): The root node name to use for parsing the data (default: None).

        Returns:
            Dict: A dictionary containing the parsed CSV data.
        """
        data_to_parse = self._get_parseable_data_from_input_data(input_data, root_name)
        self._parse_data(data_to_parse)
        return {key: self._csv_file_results[key].rows for key in self._csv_file_results}

    def get_table_mapping(self):
        """
        Get the table mapping used by the parser.

        Returns:
            TableMapping: The table mapping used by the parser.
        """
        return self.analyzer.get_mapping_dict_fom_structure()

    def analyze_data(self, input_data: List[Dict], node_path: Optional[List[str]] = None) -> Dict[str, Table]:
        """
        Analyze input data and return a dictionary of Table objects.

        Parameters:
            input_data (List[Dict]): The JSON data to analyze.
            node_path (Optional[List[str]]): The path to the current node being analyzed (default: None).

        Returns:
            Dict[str, Table]: A dictionary containing Table objects representing the analyzed data.
        """
        if not node_path:
            node_path = []
        for row in input_data:
            if is_scalar(row):
                row = {"data": row}
            self._parse_row(row, node_path)
        return self.analyzer.get_mapping_dict_fom_structure()

    @staticmethod
    def _get_parseable_data_from_input_data(input_data: Union[Dict, List[Dict]],
                                            root_name: Optional[str]) -> List[Dict]:
        """
            Get the parseable data from the input JSON data based on the root name.

            This static method processes the input JSON data to obtain a list of dictionaries that can be parsed
            by the parser. It handles cases where a specific root node is provided as the entry point for parsing.

            Parameters:
                input_data (Union[Dict, List[Dict]]): The JSON data to process.
                root_name (Optional[str]): The root node name to use for parsing the data (default: None).

            Returns:
                List[Dict]: A list of dictionaries that can be parsed by the parser.

            Raises:
                JsonParserException: If the specified root node is invalid and not found in the input data.
            """
        if root_name:
            if root_name == ".":
                input_data = [input_data]
            else:
                for node in root_name.split("."):
                    if node not in input_data:
                        raise JsonParserException(f"The root node '{root_name}' is invalid")
                    input_data = input_data.get(node)
        if is_dict(input_data):
            input_data = [input_data]
        return input_data

    def _parse_data(self, input_data: List[Dict],
                    node_path: Optional[List[str]] = None,
                    file_name: Optional[str] = None,
                    parent_data: Optional[Dict[str, str]] = None) -> Dict[str, Table]:
        """
        Parse the input JSON data recursively and generate Table objects.

        This private method is responsible for recursively parsing the input JSON data and converting it into Table objects.
        The parsed CSV data is stored in the `_csv_file_results` dictionary as Table objects.

        Parameters:
            input_data (List[Dict]): The JSON data to parse.
            node_path (Optional[List[str]]): The path to the current node being analyzed (default: None).
            file_name (Optional[str]): The name of the CSV file (table) to associate the parsed data (default: None).
            parent_data (Optional[Dict[str, str]]): A dictionary containing parent-level data to be merged with the current row.

        Returns:
            Dict[str, Table]: A dictionary containing Table objects representing the parsed CSV data.

        Notes:
            - If `file_name` is not provided, the main table name specified during initialization will be used.
            - The `node_path` parameter is used for internal tracking during the recursive parsing process.
            - The `parent_data` parameter allows for merging parent-level data into the current row.

        """
        if not file_name:
            file_name = self.main_table_name
        if not node_path:
            node_path = []
        csv_file = self._create_csv_file(file_name)
        if not parent_data:
            parent_data = {}
        for row in input_data:
            if is_scalar(row):
                row = {"data": row}
            if parent_data:
                row.update(parent_data)
                for key in parent_data:
                    new_node_path = node_path + [key]
                    self.analyzer.add_node(new_node_path, NodeType.SCALAR)
            csv_row = self._parse_row(row, node_path)
            csv_file.save_row(csv_row.get_row())
            csv_file.headers = csv_row.get_headers()

        return self._csv_file_results

    def _parse_row(self,
                   row: Dict[str, Any],
                   current_path: Optional[List[str]] = None,
                   outer_object_hash: Optional[str] = None) -> CsvRow:
        current_path = current_path or []
        if self.analyze_further:
            for name, value in row.items():
                self.analyzer.analyze_object(current_path, name, value)
        columns = self.analyzer.get_column_types(current_path)
        array_parent_id = self._generate_column_hash(row, current_path, outer_object_hash)
        primary_key_values = self._get_primary_key_values(data_row=row, node_path=current_path)
        column_map = self.analyzer.get_column_mappings_at_path(current_path)
        csv_row = CsvRow(column_map)
        for column, (data_type, force_type) in dict(columns).items():
            self._parse_field_by_type(row, csv_row, str(column), data_type, current_path, array_parent_id,
                                      primary_key_values, force_type)
        return csv_row

    def _parse_field_by_type(self,
                             row: Dict[str, Any],
                             csv_row: CsvRow,
                             column: str,
                             data_type: NodeType,
                             node_path: List[str],
                             array_parent_id: str,
                             primary_key_values: Dict[str, str],
                             force_type: bool) -> None:
        """
            Parse a field of JSON data based on its data type and other properties.

            This private method is responsible for parsing a field of JSON data based on its data type, node path, and other
            properties like primary keys and force type. It then stores the parsed value in the corresponding CSV row.

            Parameters:
                row (Dict[str, Any]): The JSON data for the current row.
                csv_row (CsvRow): The CsvRow object representing the current row in the CSV.
                column (str): The column name of the current field in the JSON data.
                data_type (NodeType): The data type of the current field based on the node structure.
                node_path (List[str]): The path to the current field in the JSON data.
                array_parent_id (str): The ID of the parent object (for array elements) used for CSV table relations.
                primary_key_values (Dict[str, str]): A dictionary containing primary key values for the table.
                force_type (bool): A flag indicating whether to force the data to be unparsed.

            Notes:
                - If the column data is None, it tries to fetch the default value from the node in the analyzer.
                - For lists and dictionaries, it calls the corresponding private methods to handle them recursively.
                - For scalars or when force type is True, it sets the value directly in the CSV row.
                - The `whole_path` variable is used for setting the value in the CSV row with the correct key (dot-separated).

            """

        column_data = row.get(column)
        if column_data is None:
            path = node_path + [column]
            node = self.analyzer.get_node_dict(path)
            column_data = node.get("node").default_value

        if column_data is None:
            pass
        elif data_type in [NodeType.LIST, NodeType.LIST_OF_DICTS, NodeType.LIST_OF_SCALARS] and not force_type:
            self._parse_list_field(column_data, csv_row, node_path, column, array_parent_id, primary_key_values)
        elif data_type == NodeType.DICT and not force_type:
            # add primary_key_values to parse dict field?
            self._parse_dict_field(column_data, csv_row, node_path, column, array_parent_id)
        elif is_scalar(column_data) or force_type:
            whole_path = node_path + [column]
            csv_row.set_value(".".join(whole_path), column_data)

    def _parse_list_field(self,
                          column_data: Any,
                          csv_row: CsvRow,
                          node_path: List[str],
                          column: str,
                          array_parent_id: str,
                          primary_key_values: Dict[str, str]) -> None:
        """

        Parse a list field of JSON data.

        This private method is responsible for parsing a list field of JSON data, handling array elements, and
        recursively processing the list items. It creates separate tables for each nested list, maintains the parent-child
        relationship in the CSV data, and applies primary key values if available.

        Parameters:
            column_data (Any): The JSON data for the current column (list field).
            csv_row (CsvRow): The CsvRow object representing the current row in the CSV.
            node_path (List[str]): The path to the current field in the JSON data.
            column (str): The column name of the current field in the JSON data.
            array_parent_id (str): The ID of the parent object (for array elements) used for CSV table relations.
            primary_key_values (Dict[str, str]): A dictionary containing primary key values for the table.

        """
        if not is_list(column_data):
            column_data = [column_data]
        if array_parent_id or primary_key_values:
            new_path = self.analyzer.create_path_to_child_object(node_path, column)
            new_table_name = self.analyzer.get_table_name(new_path)
            parent_data = {"JSON_parentId": array_parent_id}
            if primary_key_values:
                parent_data = primary_key_values
            else:
                child_path = self.analyzer.create_path_to_child_object(node_path, column)
                sf = self.analyzer.get_node_dict(child_path).get("node").header_name
                csv_row.set_value(sf, array_parent_id)

            self._parse_data(column_data, new_path, new_table_name, parent_data=parent_data)

    def _parse_dict_field(self,
                          column_data: Dict,
                          csv_row: CsvRow,
                          node_path: List[Union[Any, str]],
                          column: str,
                          array_parent_id: str) -> None:

        new_path = self.analyzer.create_path_to_child_object(node_path, column)
        child_row = self._parse_row(column_data, new_path, outer_object_hash=array_parent_id)
        for key, value in child_row.get_row().items():
            csv_row.set_value(key, value)

    def _create_csv_file(self, file_name: str) -> Table:
        if file_name not in self._csv_file_results:
            self._csv_file_results[file_name] = Table(file_name, [])
        return self._csv_file_results[file_name]

    def _generate_column_hash(self, data_row: Dict[str, Any], node_path: List[Union[Any, str]],
                              outer_object_hash: Optional[str] = None) -> str:
        node_path = [self.main_table_name] + node_path
        clean_node_string = ".".join(list(filter(lambda val: val != "[]", node_path)))
        hashed_values = hashlib.md5(str(data_row).encode() + str(outer_object_hash).encode()).hexdigest()
        return f"{clean_node_string}_{hashed_values}"

    def _get_primary_key_values(self, data_row: Dict[str, Any], node_path: List[Union[Any, str]]) -> Dict[str, str]:
        current_table_primary_keys = {}
        children_nodes = self.analyzer.get_node_dict(path_to_object=node_path).get("children")
        for child_node in children_nodes:
            if children_nodes.get(child_node).get("node").is_primary_key:
                # TODO FIX WHEN ID IS NESTED e.g. some_dict.id
                name = "_".join([self.main_table_name] + node_path + [child_node])
                current_table_primary_keys[name] = data_row.get(child_node)

        return current_table_primary_keys
