class Column:
    def __init__(self, name, table, column_data):
        self.name = name
        self.table = table
        self.data = column_data

    @property
    def object_name(self):
        return f"{self.table}.{self.name}"


