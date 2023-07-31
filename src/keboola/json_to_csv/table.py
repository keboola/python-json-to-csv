from typing import Any, Dict, List, Optional


class Table:
    """
    CSV Table Representation.

    This class represents a CSV table where the parsed data will be stored before saving it to a CSV file.

    Attributes:
        name (str): The name of the table.
        headers (Optional[List[Any]]): A list containing the headers of the CSV table (default: None).
        rows (List[Dict[str, Any]]): A list of rows where each row is represented as a dictionary.

    Methods:
        __init__(self, name: str, headers: Optional[List[Any]] = None) -> None:
            Initialize a Table object.

        save_row(self, row: Dict[str, Any]) -> None:
            Save a row of data to the Table.

    """

    def __init__(self, name: str, headers: Optional[List[Any]] = None) -> None:
        """
        Initialize a Table object.

        Parameters:
            name (str): The name of the table.
            headers (Optional[List[Any]]): A list containing the headers of the CSV table (default: None).
        """
        self.name = name
        self.headers = headers or []
        self.rows = []

    def save_row(self, row: Dict[str, Any]) -> None:
        """
        Save a row of data to the Table.

        This method appends a row of data represented as a dictionary to the list of rows in the Table.

        Parameters:
            row (Dict[str, Any]): A dictionary representing a single row of data to be saved in the Table.
        """
        self.rows.append(row)
