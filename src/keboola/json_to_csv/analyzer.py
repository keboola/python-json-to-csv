from typing import Any, Dict, List, Optional, Union, Tuple
import copy
from .exceptions import JsonParserException
from .mapping import TableMapping
from .utils import is_dict, is_list, is_scalar
from .node import Node, NodeType


class Analyzer:
    """
    The Analyzer class is designed to analyze a data structure and build a node hierarchy
    that represents the structure and data types of the objects within it. It can be used
    to create mappings for tabular data and handle complex nested structures.

    The Analyzer works based on a node hierarchy, where each node represents an object or
    a field in the data structure. The hierarchy is built by analyzing the provided data
    using the `analyze_object` method, or by having it be initialized by a predefined table
    mapping by the user.

    The Analyzer supports different data types, such as dictionaries, lists, scalars, and
    nested structures. It can handle various scenarios, including lists of dictionaries,
    lists of scalars, and more. The data types are represented using the `NodeType` enum.

    To use the Analyzer, you can initialize an instance with an optional root name and
    TableMapping. The TableMapping helps to specify the mapping of the data structure to
    tabular data. Then, call the `analyze_object` method to analyze each object within
    the data structure. The `get_mapping_dict_fom_structure` method can be used to get
    the resulting mapping dictionary.

    The class also provides methods to upgrade node data types and get column mappings at
    specific paths in the hierarchy.

    """

    def __init__(self,
                 table_mapping: Optional[TableMapping] = None,
                 root_name: Optional[str] = None) -> None:
        """
        Initialize the Analyzer object.

        Args:
            table_mapping (Optional[TableMapping]): The TableMapping object to use for the initialization of the
                                                    node_hierarchy
            root_name (Optional[str]): The root name for the data structure (optional).
        """

        self.root_name = root_name
        self.node_hierarchy = {"children": {}, "node": Node([], root_name, NodeType.LIST)}

        if table_mapping:
            self.update_with_table_mapping(table_mapping, None)

    def get_mapping_dict_fom_structure(self):
        return self._get_table_mapping_of_node_hierarchy()

    def analyze_object(self, path_to_object: List[Union[Any, str]], name: str, value: Any) -> None:
        """
            Analyzes an object within the data structure and updates the node hierarchy accordingly.

            This method is a recursive function that analyzes each object and its nested objects
            within the data structure. It updates the node hierarchy with the appropriate data types
            and mappings for each object.

            Args:
                path_to_object (List[Union[Any, str]]): The path to the current object within the data structure.
                    This list represents the sequence of keys or indices to reach the current object from the root.
                name (str): The name of the current object. In the case of dictionaries, it represents the key name.
                    In the case of lists, it represents the index.
                value (Any): The value of the current object. It can be a scalar, dictionary, list, or None..
            """

        object_path = self.create_path_to_child_object(path_to_object, name)

        expected_node = self.get_node_dict(object_path)
        if not expected_node:
            real_type = self.get_value_type(path_to_object, value)
            real_node = self.add_node(object_path, real_type)
        else:
            real_type = self.get_value_type(path_to_object, value)
            expected_type = expected_node.get("node").data_type
            if real_type != expected_type and not expected_node.get("node").force_type:
                if self._check_node_type_upgrade(expected_node.get('node'), expected_type, real_type):
                    self._perform_node_type_upgrade(expected_node.get('node'), expected_type, real_type)
            real_node = self.get_node_dict(object_path)

        if real_node["node"].data_type == NodeType.DICT:
            for sub_obj_name, sub_obj_value in value.items():
                self.analyze_object(object_path, sub_obj_name, sub_obj_value)

    def get_value_type(self, path_to_object: List[Union[Any, str]],
                       value: Any) -> NodeType:
        """
           Get the data type of the given value within the data structure.

           This method determines the NodeType of the provided value based on its data characteristics.
           It checks whether the value is a scalar, a dictionary, a list, or None. In the case of lists,
           it further analyzes the elements to determine if it's a list of scalars or a list of dictionaries.

           Args:
               path_to_object (List[Union[Any, str]]): The path to the current object within the data structure.
                   This list represents the sequence of keys or indices to reach the current object from the root.
               value (Any): The value to determine the data type for.
           """
        if is_scalar(value):
            node_type = NodeType.SCALAR
        elif is_dict(value):
            node_type = NodeType.DICT
        elif value is None:
            node_type = NodeType.NULL
        elif is_list(value) and len(value) > 0:
            final_element_type = self.get_value_type_of_array_elements(path_to_object, value)
            if final_element_type == NodeType.DICT:
                node_type = NodeType.LIST_OF_DICTS
            elif final_element_type == NodeType.SCALAR:
                node_type = NodeType.LIST_OF_SCALARS
            else:
                node_type = NodeType.LIST
        elif is_list(value):
            node_type = NodeType.LIST
        else:
            raise JsonParserException(f"Unsupported data in path {path_to_object}")
        return node_type

    def get_value_type_of_array_elements(self, path_to_object: List[Any], element_list: List[Any]) -> NodeType:
        """
        Get the common data type of elements within a list in the data structure.

        This method determines the common NodeType of elements in the provided list. It iterates
        through the elements and calls the `get_value_type` method to determine the data type of
        each element. If all elements have the same data type, it returns that data type; otherwise,
        it raises a JsonParserException indicating that the list has inconsistent element data types.

        Args:
            path_to_object (List[Any]): The path to the current object within the data structure.
                This list represents the sequence of keys or indices to reach the current object from the root.
            element_list (List[Any]): The list containing elements to analyze.

        Returns:
            NodeType: The common data type of elements within the list.
        """
        final_element_type = NodeType.NULL
        for element in element_list:
            element_type = self.get_value_type(path_to_object, element)
            if element_type != final_element_type:
                if final_element_type == NodeType.NULL or element_type == NodeType.NULL:
                    final_element_type = final_element_type if final_element_type != NodeType.NULL else element_type
                else:
                    raise JsonParserException(f"Value types of list {path_to_object} are inconsistent : {element_list}")
        return final_element_type

    def _perform_node_type_upgrade(self, node, expected_type, real_type):
        """
        Upgrade the data type of a node in the node hierarchy.

        This method upgrades the data type of the provided node to the expected data type based
        on the comparison of the current data type (`real_type`) and the expected data type
        (`expected_type`). The method checks for specific scenarios where upgrading the data type
        is possible, such as converting a NodeType.LIST to NodeType.LIST_OF_SCALARS or
        NodeType.LIST_OF_DICTS.

        Args:
            node: The node object to upgrade in the node hierarchy.
            expected_type (NodeType): The expected data type of the node.
            real_type (NodeType): The current data type of the node.
        """
        if expected_type == NodeType.NULL:
            self.upgrade_node_type(node, real_type)
        elif expected_type == NodeType.LIST and real_type == NodeType.LIST_OF_SCALARS:
            self.upgrade_node_type(node, NodeType.LIST_OF_SCALARS)
        elif expected_type == NodeType.LIST and real_type == NodeType.LIST_OF_DICTS:
            self.upgrade_node_type(node, NodeType.LIST_OF_DICTS)
        elif expected_type == NodeType.LIST and real_type == NodeType.SCALAR:
            self.upgrade_node_type(node, NodeType.LIST_OF_SCALARS)
        elif expected_type == NodeType.LIST and real_type == NodeType.DICT:
            self.upgrade_node_type(node, NodeType.LIST_OF_DICTS)
        elif expected_type == NodeType.DICT and real_type == NodeType.LIST_OF_DICTS:
            self.upgrade_node_type(node, NodeType.LIST_OF_DICTS)
        elif expected_type == NodeType.DICT and real_type in NodeType.LIST:
            self.upgrade_node_type(node, NodeType.LIST_OF_DICTS)
        elif expected_type == NodeType.SCALAR and real_type == NodeType.LIST_OF_SCALARS:
            self.upgrade_node_type(node, NodeType.LIST_OF_SCALARS)
        elif expected_type == NodeType.SCALAR and real_type == NodeType.LIST:
            self.upgrade_node_type(node, NodeType.LIST_OF_SCALARS)

    @staticmethod
    def _check_node_type_upgrade(node, expected_type, real_type):
        """
        Check if upgrading the data type of a node is compatible.

        This method checks whether upgrading the data type of the provided node to the expected
        data type is a compatible operation. It analyzes the current data type (`real_type`) and
        the expected data type (`expected_type`) and determines if the upgrade scenario is supported.

        Args:
            node: The node object to check for data type upgrade compatibility.
            expected_type (NodeType): The expected data type of the node.
            real_type (NodeType): The current data type of the node.

        Returns:
            bool: True if the data type upgrade is compatible; otherwise, raises a JsonParserException.

        """
        varying_node_types = [expected_type, real_type]

        one_type_is_dict = NodeType.DICT in varying_node_types
        one_type_is_scalar = NodeType.SCALAR in varying_node_types
        one_type_is_list_of_scalars = NodeType.LIST_OF_SCALARS in varying_node_types
        one_type_is_list_of_dicts = NodeType.LIST_OF_DICTS in varying_node_types

        if one_type_is_dict and one_type_is_scalar:
            is_compatible = False
        elif one_type_is_dict and one_type_is_list_of_scalars:
            is_compatible = False
        elif one_type_is_scalar and one_type_is_list_of_dicts:
            is_compatible = False
        elif one_type_is_list_of_scalars and one_type_is_list_of_dicts:
            is_compatible = False
        else:
            is_compatible = True

        if not is_compatible:
            raise JsonParserException(f"Incompatible types of {expected_type} and {real_type} "
                                      f"in node_path {node.path}")
        return True

    def update_with_table_mapping(self, table_mapping: TableMapping, parent_path: Optional[List] = None) -> None:
        if not parent_path:
            parent_path = []

        self._process_column_mappings(table_mapping, parent_path)
        self._process_user_data_mappings(table_mapping, parent_path)
        self._process_child_table_mappings(table_mapping, parent_path)

    def get_node_dict(self, path_to_object: List) -> Dict:
        if len(path_to_object) > 0:
            node = self.node_hierarchy.get("children").get(path_to_object[0], {})
        else:
            node = self.node_hierarchy
        if len(path_to_object) > 1:
            for path_step in path_to_object[1:]:
                node = node.get("children").get(path_step, {})
        return node or None

    def add_node(self, path_to_object: List[str], value_type: NodeType, force_type: bool = False,
                 destination_name: Optional[str] = None,
                 is_primary_key: bool = False, default_value: Optional[str] = None) -> Dict[str, Node]:
        def recursive_add(node_dict, path, value_node):
            if len(path) == 1:
                node_dict[path[0]] = value_node
            else:
                first_level = path[0]
                if first_level not in node_dict:
                    node_dict[first_level] = {"children": {}}
                recursive_add(node_dict[first_level]["children"], path[1:], value_node)

        parent_name = None
        if len(path_to_object) > 1:
            parent_node = self.get_node_dict(path_to_object[:-1])
            if parent_node.get("node").data_type == NodeType.DICT:
                parent_name = parent_node.get("node").header_name

        new_node = {"node": Node(path_to_object, path_to_object[-1], value_type,
                                 parent_name=parent_name, force_type=force_type, destination_name=destination_name,
                                 is_primary_key=is_primary_key, default_value=default_value),
                    "children": {}}

        # Start the recursive addition
        recursive_add(self.node_hierarchy["children"], path_to_object, new_node)

        return new_node

    def upgrade_node_type_recursive(self, hierarchy, path, new_node_type):
        if len(path) == 1:
            hierarchy["children"][path[0]]["node"].data_type = new_node_type
        else:
            next_level = hierarchy["children"][path[0]]
            self.upgrade_node_type_recursive(next_level, path[1:], new_node_type)

    def upgrade_node_type(self, node, new_node_type):
        node_dict_to_update = self.get_node_dict(node.path)

        node_dict_to_update["node"].data_type = new_node_type

        self.upgrade_node_type_recursive(self.node_hierarchy, node.path, new_node_type)

    @staticmethod
    def create_path_to_child_object(parent_path: List[str], child_name: str) -> List[str]:
        new_path = copy.copy(parent_path)
        new_path.append(child_name)
        return new_path

    @staticmethod
    def is_nested_node_name(node_name: str) -> bool:
        return "." in node_name

    def get_column_types(self, path: List[str]) -> Dict[str, Tuple[NodeType, bool]]:
        nodes = self.get_node_dict(path)
        if not nodes:
            return {}
        headers = {}
        for node in nodes.get("children"):
            decoded_name = nodes.get("children")[node].get("node").data_name

            data_type = nodes.get("children")[node].get("node").data_type
            force_type = nodes.get("children")[node].get("node").force_type
            headers[decoded_name] = (data_type, force_type)
        return headers

    def get_table_name(self, path: List[str]) -> str:
        name = self.root_name
        for p in path:
            name += f"_{p}"
        return name

    def _process_column_mappings(self, table_mapping: TableMapping, parent_path: List[str]) -> None:
        for column_name in table_mapping.column_mappings:
            if self.is_nested_node_name(column_name):
                self._process_nested_column_mapping(column_name, parent_path, table_mapping)
            else:
                self._process_column_mapping(column_name, parent_path, table_mapping)

    def _process_column_mapping(self, column_name: str, parent_path: List[str],
                                table_mapping: TableMapping) -> None:
        path = self.create_path_to_child_object(parent_path, column_name)

        force_type = column_name in table_mapping.force_types
        is_primary_key = column_name in table_mapping.primary_keys
        destination_name = table_mapping.column_mappings.get(column_name)

        self.add_node(path, NodeType.SCALAR, force_type=force_type, destination_name=destination_name,
                      is_primary_key=is_primary_key)

    def _process_nested_column_mapping(self, column_name: str, parent_path: List[Any],
                                       table_mapping: TableMapping) -> None:
        split_name = column_name.split(".")
        paths_added = []

        for i, item in enumerate(split_name):
            paths_added.append(item)
            node = self.get_node_dict(paths_added)
            if not node:
                path = parent_path.copy()
                path.extend(paths_added)
                force_type = column_name in table_mapping.force_types
                is_primary_key = column_name in table_mapping.primary_keys
                destination_name = table_mapping.column_mappings.get(column_name)

                node_type = NodeType.DICT if i + 1 != len(split_name) else NodeType.SCALAR
                self.add_node(path, node_type, force_type=force_type, destination_name=destination_name,
                              is_primary_key=is_primary_key)

    def _process_user_data_mappings(self, table_mapping: TableMapping, parent_path: List[str]) -> None:
        for user_data_name, default_value in table_mapping.user_data.items():
            path = parent_path + [user_data_name]
            self.add_node(path, NodeType.SCALAR, destination_name=user_data_name, default_value=default_value)

    def _process_child_table_mappings(self, table_mapping: TableMapping, parent_path: List[str]) -> None:
        for child_table in table_mapping.child_tables:
            path = parent_path.copy()
            path.append(child_table)
            self.add_node(path, NodeType.LIST)
            self.update_with_table_mapping(table_mapping.child_tables.get(child_table), path)

    def _get_table_mapping_of_node_hierarchy(self, node_hierarchy=None):
        if not node_hierarchy:
            node_hierarchy = self.node_hierarchy
        table_name = node_hierarchy.get("node").header_name
        primary_keys = []
        force_types = []
        columns = {}
        child_tables = {}
        for child in node_hierarchy.get("children"):
            child_columns, child_child_tables, child_primary_keys, child_force_types = self._analyze_child_node_mapping(
                node_hierarchy, child)
            columns.update(child_columns)
            child_tables.update(child_child_tables)
            force_types.extend(child_force_types)
            primary_keys.extend(child_primary_keys)
        return {"table_name": table_name,
                "column_mappings": columns,
                "primary_keys": primary_keys,
                "force_types": force_types,
                "child_tables": child_tables}

    def _analyze_child_node_mapping(self, node_hierarchy, child_name):
        primary_keys = []
        force_types = []
        columns = {}
        child_tables = {}

        child_node = node_hierarchy.get("children").get(child_name).get("node")
        if child_node.data_type == NodeType.SCALAR:
            columns[child_name] = child_node.header_name
            if child_node.force_type:
                force_types.append(child_name)
            if child_node.is_primary_key:
                primary_keys.append(child_name)
        if child_node.data_type in [NodeType.LIST, NodeType.LIST_OF_SCALARS, NodeType.LIST_OF_DICTS]:
            child_tables[child_name] = self._get_table_mapping_of_node_hierarchy(
                node_hierarchy=node_hierarchy.get("children").get(child_name))
        if child_node.data_type == NodeType.DICT:
            child_columns, child_child_tables, child_primary_keys, child_force_types = self._get_table_mapping_of_dict_node(
                node_hierarchy.get("children").get(child_name))
            columns.update(child_columns)
            child_tables.update(child_child_tables)
            force_types.extend(child_force_types)
            primary_keys.extend(child_primary_keys)

        return columns, child_tables, primary_keys, force_types

    def _get_table_mapping_of_dict_node(self, node_thing):
        dict_node_name = node_thing.get("node").data_name
        primary_keys = []
        force_types = []
        columns = {}
        child_tables = {}
        for child in node_thing.get("children"):
            child_columns, child_child_tables, child_primary_keys, child_force_types = self._analyze_child_node_mapping(
                node_thing, child)
            columns.update(child_columns)
            child_tables.update(child_child_tables)
            force_types.extend(child_force_types)
            primary_keys.extend(child_primary_keys)

        primary_keys = self.add_prefix_to_list_items(primary_keys, dict_node_name + ".")
        force_types = self.add_prefix_to_list_items(force_types, dict_node_name + ".")
        columns = self.add_prefix_to_dict_keys(columns, dict_node_name + ".")
        child_tables = self.add_prefix_to_dict_keys(child_tables, dict_node_name + ".")
        return columns, child_tables, primary_keys, force_types

    def get_column_mappings_at_path(self, node_path: List[str]) -> Dict[str, str]:
        headers = {}
        node_data = self.get_node_dict(node_path) or {}
        if "node" in node_data:
            node_type = node_data.get("node").data_type
            if node_type == NodeType.SCALAR:
                headers[".".join(node_data.get("node").path)] = node_data.get("node").header_name

        for node_name, data in node_data.get("children").items():
            if data.get("node").data_type == NodeType.DICT:
                ch = self.get_column_mappings_at_path(self.create_path_to_child_object(node_path, node_name))
                headers.update(ch)
            elif data.get("node").data_type == NodeType.LIST:
                continue
            else:
                headers[".".join(data.get("node").path)] = data.get("node").header_name

        return headers

    @staticmethod
    def add_prefix_to_list_items(input_list, prefix):
        return [prefix + item for item in input_list]

    @staticmethod
    def add_prefix_to_dict_keys(input_dict, prefix):
        return {prefix + key: value for key, value in input_dict.items()}
