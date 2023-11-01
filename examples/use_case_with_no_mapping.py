from keboola.json_to_csv import Parser

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

parser = Parser(main_table_name="user")
parsed_data = parser.parse_data(data)

mapping = parser.table_mapping
