from keboola.json_to_csv import Parser, TableMapping

data = [
    {
        "name": "John Doe",
        "email": "sss",
        "first": {
            "nested": 50
        },
        "child_table": [
            {
                "value": "Blossom Avenue",
                "second": {
                    "nested": 51.509865
                }
            },
            {
                "value": 2,
                "second": {
                    "nested": 51.509865
                }
            }
        ]
    }
]

mapping_dict = {
    "table_name": "root_table",
    "column_mappings": {
        "name": "name",
        "email": "email",
        "first.nested": "first_nested"
    },
    "primary_keys": [],
    "force_types": [],
    "child_tables": {
        "child_table": {
            "table_name": "contacts",
            "column_mappings": {
                "value": "value",
                "second.nested": "second_nested"
            },
            "primary_keys": [],
            "force_types": [],
            "child_tables": {}
        }
    }
}

mapping = TableMapping.build_from_mapping_dict(mapping_dict)
parser = Parser(main_table_name="root_table", table_mapping=mapping, analyze_further=True)

result = parser.parse_data(data)

print(result)

result_mapping = parser.get_table_mapping().as_dict()

print(result_mapping)
