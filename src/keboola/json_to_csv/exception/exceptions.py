class JSONParserException(Exception):
    pass


class InvalidRootNodeException(JSONParserException):
    DEFAULT_MESSAGE = "Invalid root node, could not parse JSON file"

    def __init__(self, msg=DEFAULT_MESSAGE, *args, **kwargs):
        super().__init__(msg, *args, **kwargs)


class MissingDataException(JSONParserException):
    DEFAULT_MESSAGE = "No data was found based on the specified root node"

    def __init__(self, msg=DEFAULT_MESSAGE, *args, **kwargs):
        super().__init__(msg, *args, **kwargs)


class InconsistentDataTypesException(JSONParserException):
    def __init__(self, msg=None, column=None, parse_strategy=None, processed_columns=None, *args, **kwargs):
        if not msg and column and parse_strategy and processed_columns:
            msg = f"""Object {column.object_name} has an inconsistent data parsing strategy once the type/strategy was '{parse_strategy.value}' another time type/strategy was '{processed_columns[column.object_name].value}'. If there are inconsistant data types use the 'always_array' option in the configuration of the JSONParser"""
        super().__init__(msg, *args, **kwargs)
