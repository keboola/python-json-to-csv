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

mapping_dict = {'table_name': 'user',
                'column_mappings': {'id': 'id',
                            'name': 'name',
                            'details.weight': 'user_weight',
                            'details.height': 'user_height',
                            'details.hair_color': 'user_hair_color'},
                'primary_keys': ['id'],
                'force_types': [],
                'child_tables': {
                    'addresses': {'table_name': 'addresses',
                                  'column_mappings': {
                                      'user_id': 'user_id',
                                      'index': 'index',
                                      'street': 'street',
                                      'country': 'country',
                                      'city': 'city'},
                                  'primary_keys': ['user_id', 'index'],
                                  'force_types': [],
                                  'child_tables': {}}}}

mapping = TableMapping.build_from_mapping_dict(mapping_dict)
parser = Parser(main_table_name="user", table_mapping=mapping, analyze_further=False)
parsed_data = parser.parse_data(data)