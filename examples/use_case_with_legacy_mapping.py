from keboola.json_to_csv import Parser, TableMapping

data = [
    {
        "id": 123,
        "name": "John Doe",
        "details": {
            "weight": 50,
            "height": 150,
            "hair_color": "brown"
        },
        "addresses": [
            {
                "index": 1,
                "street": "Blossom Avenue",
                "country": "United Kingdom"
            },
            {
                "index": 2,
                "street": "Whiteheaven Mansions",
                "city": "London",
                "country": "United Kingdom"
            }
        ]
    }
]

legacy_mapping_dict = {"user": {
    "id": {
        "type": "column",
        "mapping": {
            "destination": "user_id",
            "primaryKey": True
        }
    },
    "name": {
        "type": "column",
        "mapping": {
            "destination": "name"
        }
    },
    "details.hair_color": {
        "type": "column",
        "mapping": {
            "destination": "hair_color"
        }
    }
}}

mapping = TableMapping.build_from_legacy_mapping(legacy_mapping_dict)
parser = Parser(main_table_name="user", table_mapping=mapping, analyze_further=False)
parsed_data = parser.parse_data(data)
