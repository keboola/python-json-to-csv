# Python JSON to CSV Library

## Introduction

JSON-to-CSV is a library that parses any JSON file into one or multiple CSV files.

The JSON-to-CSV library is developed the Keboola Data Services team and is officially supported by Keboola.

## How it works

The parser is first configured using arguments that define the data and how to parse it. The JSON file is 
loaded into a dictionary and sent to the parser which recursively parses the elements in the JSON file. Each
key in the dictionary is processed to figure out the strategy that will be used to parse it. Once all data is parsed it 
is can be returned as tables in the form of a list containing flat dictionaries.

## Configurable parameters
* json_configuration_file
  * Optional[str] : Path to JSON configuration file for the JSONParser settings
* configuration_dict 
  * Optional[Dict] : Configuration of the JSONParser settings in Dict format
* parent_table
  * Path to JSON configuration file for the JSONParser settings
* child_tables
  * Optional[dict]: Child table definitions containing the key value pairs of child object and their corresponding csv name. Keys being the object name (with all parent objects separated by periods), and the value being the name of the output table. e.g. {"parent_object.child_object": "child.csv"}
* child_tables
  * Optional[dict]: definition of all primary keys of all tables. Key value pairs where Keys being the object name (with all parent objects separated by periods) and values being  key value pairs of objects that should be primary keys of the table and values being the resulting  names of the columns in the CSV  e.g. {"order.order-item" : {"order.id" : "order_id", "order.order-items.item_id" : "item_id"}}
* table_column_mapping
  * Optional[Dict[str, Dict[str, str]]] : definition of objects is tables and their mapping to columns. The input is a dictionary where the keys are a parent table object or a child table object and the values are dictionaries holding key value pairs of the elements within the objects and the values being the resulting column names. e.g. {'parent_object': {'parent_object.element': 'element_column_name'}}
* root_node
  * Optional[str] : name of any root node of the data e.g. if data is: {"root_el": {"orders": {"order": [{}]}}}, then root_node should be root_el.orders.order
* default_parse_list
  * Optional[ParseListStrategy] : The default strategy of the parser to parse lists
* default_parse_dict
  * Optional[ParseDictStrategy] : The default strategy of the parser to parse dictionaries
* parse_list_mapping
  * Optional[Dict[str,ParseListStrategy]] :  A dictionary holding key value pairs, where the keys are specific elements that are Lists in the JSON data and values being their Parsing strategy. This is used when a specific list should be parsed differently than the default strategy.
* parse_dict_mapping
  * Optional[Dict[str,ParseDictStrategy]] :  A dictionary holding key value pairs, where the keys are specific elements that are Dictionaries in the JSON data and values being their Parsing strategy. This is used when a specific dictionary should be parsed differently than the default strategy.
* ignore_undefined_tables
  * Optional[bool] : If True, tables that are not defined in child or parent table definitions will not be processed or output by the parser.
* ignore_undefined_columns
  * Optional[bool] : If True, columns that are not defined in table column mapping will not be processed or output by the parser.
* always_array
  * Optional[List] : List of element names that should always be treated as arrays/lists even though they might sometimes be of a different.

## Quick start

### Installation

The package can be installed via `pip` using:

```
pip install keboola.json-to-csv
```

#### Initialization

```python
from keboola.json_to_csv import JSONParser
```

#### Usage Example with JSON configuration file

```python
import json
import csv
import keboola.json_to_csv as jc

parser = jc.JSONParser(json_file="config.json")
with open("input.json") as json_file:
    data = json.load(json_file)

parsed_data = parser.parse_data(data)

for csv_name in parsed_data:
    with open(csv_name, 'w') as csv_file:
        dict_writer = csv.DictWriter(csv_file, list(parsed_data[csv_name][0].keys()))
        dict_writer.writeheader()
        dict_writer.writerows(parsed_data[csv_name])
```

#### Usage Example with set parameters

```python
import json
import csv
import keboola.json_to_csv as jc

parent_table = {"users": "users.csv"}
child_tables = {"users.products_owned": "products_owned.csv"}
table_primary_keys = {"users": ["users.id"], "users.products_owned": ["users.id", "users.products_owned.id"]}
table_column_mapping = {
    "users": {
        "users.id": "user_id",
        "users.name": "name",
        "users.last_name": "last_name"
    },
    "users.products_owned": {
        "users.id": "user_id",
        "users.products_owned.id": "product_id",
        "users.products_owned.owned_since": "owned_since"
    }
}

root_node = "data.users"
ignore_undefined_columns = True

parser = jc.JSONParser(parent_table=parent_table,
                       child_tables=child_tables,
                       table_primary_keys=table_primary_keys,
                       root_node=root_node,
                       table_column_mapping=table_column_mapping,
                       ignore_undefined_columns=ignore_undefined_columns)

with open("input.json") as json_file:
    data = json.load(json_file)

parsed_data = parser.parse_data(data)

for csv_name in parsed_data:
    with open(csv_name, 'w') as csv_file:
        dict_writer = csv.DictWriter(csv_file, list(parsed_data[csv_name][0].keys()))
        dict_writer.writeheader()
        dict_writer.writerows(parsed_data[csv_name])
```