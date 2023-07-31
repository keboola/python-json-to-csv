from enum import Enum, auto
from typing import List, Optional


class NodeType(Enum):
    """
    Enumerates the types of nodes in the JSON data.

    This enumeration represents the various types of nodes that can be encountered while processing JSON data.
    Each node type corresponds to a different data structure in the JSON.

    Enumeration Values:
        DICT: Represents a dictionary node in the JSON.
        NULL: Represents a null value node in the JSON.
        LIST: Represents a list node in the JSON.
        LIST_OF_DICTS: Represents a list of dictionaries node in the JSON.
        LIST_OF_SCALARS: Represents a list of scalars node in the JSON.
        SCALAR: Represents a scalar node (e.g., int, bool, float, string) in the JSON.

    """
    DICT = auto()
    NULL = auto()
    LIST = auto()
    LIST_OF_DICTS = auto()
    LIST_OF_SCALARS = auto()
    SCALAR = auto()

    @staticmethod
    def from_str(label: str) -> 'NodeType':
        """
        Convert a string label to its corresponding NodeType.

        This static method allows converting a string representation of a node type to its corresponding
        NodeType enumeration value.

        Parameters:
            label (str): The string label representing the node type.

        Returns:
            NodeType: The NodeType enumeration value corresponding to the given label.

        Raises:
            NotImplementedError: If the given label is not recognized as a valid NodeType.
        """
        if label in ('int', 'integer'):
            return NodeType.SCALAR
        elif label in ('bool', 'boolean'):
            return NodeType.SCALAR
        elif label == 'float':
            return NodeType.SCALAR
        elif label == 'dict':
            return NodeType.DICT
        elif label == 'list':
            return NodeType.LIST
        elif label in ('str', 'string'):
            return NodeType.SCALAR
        else:
            raise NotImplementedError


class Node:
    """
    Node Representation in the JSON data structure.

    This class represents a node in the JSON data structure. Each node can have various properties such as its path,
    header name, data type, parent node name, and more.

    Attributes:
        path (List[str]): The path to the current node in the JSON data.
        header_name (str): The header name representing the node in the CSV output.
        data_name (str): The name of the data represented by the node.
        data_type (NodeType): The data type of the node.
        parent_name (Optional[str]): The name of the parent node (if any) that contains this node (default: None).
        is_primary_key (bool): A flag indicating whether the node is a primary key (default: False).
        force_type (bool):  A flag indicating whether to not parse the data further (save it as is) (default: False).
        default_value (Optional[str]): The default value for the node if the data is missing (default: None).

    """

    def __init__(self,
                 path: List[str],
                 data_name: str,
                 data_type: NodeType,
                 parent_name: Optional[str] = None,
                 is_primary_key: bool = False,
                 force_type: bool = False,
                 destination_name: Optional[str] = None,
                 default_value: Optional[str] = None) -> None:
        """
        Initialize a Node object.

        Parameters:
            path (List[str]): The path to the current node in the JSON data.
            data_name (str): The name of the data represented by the node.
            data_type (NodeType): The data type of the node.
            parent_name (Optional[str]): The name of the parent node (if any) that contains this node (default: None).
            is_primary_key (bool): A flag indicating whether the node is a primary key (default: False).
            force_type (bool): A flag indicating whether to not parse the data further (save it as is) (default: False).
            destination_name (Optional[str]): An alternative header name to use in the CSV output (default: None).
            default_value (Optional[str]): The default value for the node if the data is missing (default: None).
        """
        self.path: List = path
        self.header_name: str = data_name
        if parent_name:
            self.header_name = f"{parent_name}_{data_name}"

        if destination_name:
            self.header_name = destination_name

        self.data_name = data_name
        self.data_type = data_type
        self.parent_name = parent_name
        self.is_primary_key = is_primary_key
        self.force_type = force_type
        self.default_value = default_value

    def __eq__(self, other) -> bool:
        """
        Check if two Node objects are equal.

        This method compares two Node objects and checks whether they are equal based on their properties.

        Parameters:
            other: The other Node object to compare.

        Returns:
            bool: True if the two Node objects are equal; otherwise, False.
        """
        equals = True
        if not other:
            raise TypeError("Trying to compare node to a NoneType")
        if self.path != other.path:
            equals = False
        if self.header_name != other.header_name:
            equals = False
        if self.data_type != other.data_type:
            equals = False
        return equals
