from typing import Dict, List, Optional


class TableMapping:
    """
        Table Mapping for Converting JSON to CSV.

        This class represents the mapping of tables and their columns, primary keys, and force types. It is used for
        converting JSON data into CSV format with the specified table structure.

        Attributes:
            table_name (str): The name of the table.
            column_mappings (Dict[str, str]): A dictionary mapping JSON keys to CSV headers for each column in the table.
            primary_keys (List[str]): A list of primary key column names in the table.
            child_tables (Dict[str, TableMapping]): A dictionary mapping child table names to their corresponding TableMapping objects.
            force_types (List[str]): A list of column names that require forcing specific data types.
            user_data (Dict[str, str]): Additional user-defined data associated with the table mapping.

        """

    def __init__(self,
                 table_name: str,
                 column_mappings: Dict[str, str],
                 primary_keys: List[str],
                 child_tables: Dict[str, 'TableMapping'],
                 force_types: List[str],
                 user_data: Dict[str, str]) -> None:

        self.table_name: str = table_name
        self.column_mappings: Dict = column_mappings
        self.primary_keys: List = primary_keys
        self.child_tables: Dict = child_tables
        self.force_types = force_types
        self.user_data = user_data

    @classmethod
    def build_from_legacy_mapping(cls,
                                  legacy_mapping: dict,
                                  user_data: Optional[Dict[str, str]] = None) -> "TableMapping":
        """
        Build a TableMapping object from a legacy mapping in the old format (PHP JsonParser mapping).

        This class method allows constructing a TableMapping object from a legacy mapping in the old format.

        Parameters:
            legacy_mapping (dict): The legacy mapping dictionary in the old format.
            user_data (Optional[Dict[str, str]]): Additional user-defined data associated with the table mapping (default: None).

        Returns:
            TableMapping: A TableMapping object representing the table mapping in the new format.

        """

        if not user_data:
            user_data = {}

        # TODO Add delimiter options
        table_name = list(legacy_mapping.keys())[0]

        column_mappings = {}
        primary_keys = []
        force_types = []

        child_tables = {}

        for node_id, node in legacy_mapping.get(table_name).items():
            # if the value is string it is a simplified column mapping
            if isinstance(node, str) or node.get("type") == "column" or not node.get("type"):
                raw_name = node_id
                destination_name = node if isinstance(node, str) else node.get("mapping").get("destination")
                column_mappings[raw_name] = destination_name

            # it is not simplified mapping
            if isinstance(node, dict):
                if node.get("type") == "column":
                    if node.get("mapping").get("primaryKey"):
                        primary_keys.append(raw_name)
                    if node.get("forceType"):
                        force_types.append(raw_name)
                if node.get("type") == "table":
                    child_mapping = {node_id: node.get("tableMapping")}
                    child_table_mapping = cls.build_from_legacy_mapping(child_mapping)
                    child_tables[node_id] = child_table_mapping

        return cls(table_name=table_name,
                   column_mappings=column_mappings,
                   primary_keys=primary_keys,
                   child_tables=child_tables,
                   force_types=force_types,
                   user_data=user_data)

    @classmethod
    def build_from_mapping_dict(cls, mapping: Dict) -> "TableMapping":
        """
        Build a TableMapping object from a dictionary representation of the mapping.

        This class method allows constructing a TableMapping object from a dictionary representation of the mapping.

        Parameters:
            mapping (Dict): The dictionary representation of the table mapping.

        Returns:
            TableMapping: A TableMapping object representing the table mapping.

        """
        child_tables = {}
        for child in mapping.get("child_tables"):
            child_tables[child] = cls.build_from_mapping_dict(mapping.get("child_tables").get(child))
        return cls(table_name=mapping.get("table_name"),
                   column_mappings=mapping.get("column_mappings"),
                   primary_keys=mapping.get("primary_keys"),
                   child_tables=child_tables,
                   force_types=mapping.get("force_types"),
                   user_data=mapping.get("user_data", {}))
