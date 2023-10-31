import os
import json
import csv
import unittest
import logging
from itertools import chain
from pathlib import Path
import src.keboola.json_to_csv as jc
from keboola.json_to_csv import TableMapping, Parser


class TestParser(unittest.TestCase):

    def test_deeply_nested_obj_mapping_from_state(self):
        data = [
            {
                "name": "John Doe",
                "first": {
                    "nested": 50
                },
                "child_table": [
                    {
                        "value": "Blossom Avenue",
                        "second": {
                            "nested": 51.509865
                        },
                        "deep_child": [{"value": 1}]
                    },
                    {
                        "value": 2,
                        "second": {
                            "nested": 51.509865
                        },

                        "deep_child": [{"value": 2}]
                    }
                ]
            }
        ]

        mapping_dict = {
            "table_name": "root_table",
            "column_mappings": {
                "name": "NAME",
                "first.nested": "first_nested"
            },
            "primary_keys": [],
            "force_types": [],
            "child_tables": {
                "child_table": {
                    "table_name": "contacts",
                    "column_mappings": {
                        "value": "VALUE",
                        "second.nested": "second_nested"
                    },
                    "primary_keys": [],
                    "force_types": [],
                    "child_tables": {"deep_child": {
                        "table_name": "deep",
                        "column_mappings": {
                            "value": "VALUE"
                        },
                        "primary_keys": [],
                        "force_types": [],
                        "child_tables": {}
                    }}
                }
            }
        }

        mapping = TableMapping.build_from_mapping_dict(mapping_dict)
        parser = Parser(main_table_name="root", table_mapping=mapping, analyze_further=False)

        result = parser.parse_data(data)
        # header match
        self.assertTrue('root' in result)
        self.assertTrue('root_child_table' in result)
        self.assertTrue('root_child_table_deep_child' in result)

        root_expected = [
            {'NAME': 'John Doe', 'child_table': 'root_475dfe7acd28692ec47fd8d9ca06dd6c', 'first_nested': 50}]
        root_child_table_expected = [{'VALUE': 'Blossom Avenue', 'second_nested': 51.509865,
                                      'JSON_parentId': 'root_475dfe7acd28692ec47fd8d9ca06dd6c',
                                      'deep_child': 'root.child_table_41ea4f8aa54fc5ae0c0b2e75c1817894'},
                                     {'VALUE': 2, 'second_nested': 51.509865,
                                      'JSON_parentId': 'root_475dfe7acd28692ec47fd8d9ca06dd6c',
                                      'deep_child': 'root.child_table_cbf7276c62fef9533f08d488d81a8e8f'}]
        root_child_table_deep_child_expected = [
            {'JSON_parentId': 'root.child_table_41ea4f8aa54fc5ae0c0b2e75c1817894', 'VALUE': 1},
            {'JSON_parentId': 'root.child_table_cbf7276c62fef9533f08d488d81a8e8f', 'VALUE': 2}]

        self.assertListEqual(result['root'], root_expected)
        self.assertListEqual(result['root_child_table_deep_child'], root_child_table_deep_child_expected)
        self.assertListEqual(result['root_child_table'], root_child_table_expected)
