from typing import Dict, List, Any


class CsvRow:
    """
    CSV Row Representation.

    This class represents a single row of data in a CSV file. It is used for converting JSON data into CSV format.
    The class maintains a mapping between JSON keys and CSV headers, allowing for proper CSV output.

    """
    def __init__(self, column_map: Dict[str, str]) -> None:
        """
        Initialize the CsvRow with a given column mapping.

        Parameters:
            column_map (Dict[str, str]): A dictionary mapping JSON keys to CSV headers.
        """
        self.column_map = column_map
        self.data = {column: None for column in list(column_map.values())}

    def set_value(self, column: str, value: Any) -> None:
        """
        Set the value of a specific column in the CsvRow.

        Parameters:
            column (str): The column key (JSON or CSV header) to set the value for.
            value (Any): The value to be set for the specified column.
        """
        if column in self.column_map:
            self.data[self.column_map[column]] = value
        else:
            self.data[column] = value

    def get_row(self) -> Dict[str, Any]:
        """
        Get the data in the CsvRow as a dictionary representing a single row of CSV data.

        Returns:
            Dict[str, Any]: A dictionary representing the CsvRow's data as a single row of CSV data.
        """
        return self.data

    def get_headers(self) -> List[str]:
        """
        Get the headers of the CsvRow as a list.

        Returns:
            List[str]: A list of CSV headers representing the CsvRow.
        """
        return list(self.data.keys())

