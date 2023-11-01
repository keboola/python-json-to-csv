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
                "country": "United Kingdom",
                "coordinates": {
                    "latitude": 51.509865,
                    "longitude": -0.118092
                }
            },
            {
                "index": 2,
                "street": "Whiteheaven Mansions",
                "city": "London",
                "country": "United Kingdom",
                "coordinates": {
                    "latitude": 51.509865,
                    "longitude": -0.118092
                }
            }
        ]
    }
]

mapping_dict = {
    'table_name': 'user',
    'column_mappings': {
        'id': 'id',
        'name': 'name',
        'details.weight': 'user_weight',
        'details.height': 'user_height',
        'details.hair_color': 'user_hair_color'
    },
    'primary_keys': ['id'],
    'force_types': [],
    'child_tables': {
        'addresses': {
            'table_name': 'address_of_user',
            'column_mappings': {
                'user_id': 'user_id',
                'index': 'index',
                'street': 'street',
                'country': 'country',
                'city': 'CITY',
            },
            'primary_keys': ['user_id', 'index'],
            'force_types': [],
            'child_tables': {
                'coordinates': {
                    'table_name': 'coordinates',
                    'column_mappings': {
                        'latitude': 'latitude',
                        'longitude': 'longitude'
                    },
                    'primary_keys': [],
                    'force_types': [],
                    'child_tables': {}
                }
            }
        }
    }
}

mapping = TableMapping.build_from_mapping_dict(mapping_dict)
parser = Parser(main_table_name="user", table_mapping=mapping, analyze_further=False)
parsed_data = parser.parse_data(data)

# KCOFAC-2624 - flatten result for debugging
flattened_result_tables = parser.table_mapping.table_mappings_flattened_by_key()
# now we can get result table names par key
for key, data in parsed_data.items():
    print(f"Data result Key: {key} has result table name: {flattened_result_tables[key].table_name}")

# KCOFAC-2623 - store mapping in statefile
# store in state
mapping_dict = parser.table_mapping.as_dict()


# restore from state
mapping = TableMapping.build_from_mapping_dict(mapping_dict)
parser = Parser(main_table_name="user", table_mapping=mapping, analyze_further=False)
parsed = parser.parse_data(data)
print(parsed)
