import os
import json
import csv
import unittest
import logging
from itertools import chain
from pathlib import Path
import src.keboola.json_to_csv as jc


class JSONCaseTester(unittest.TestCase):
    def __init__(self, folder):
        super(JSONCaseTester, self).__init__(methodName="perform_test")
        self.config = self._read_json_file(os.path.join(folder, "config.json"))
        self.input_data = self._read_json_file(os.path.join(folder, "input", self.config.get("file_name")))
        self.output_data = self.load_output_data(os.path.join(folder, "output"))
        self.test_name = folder

    def load_output_data(self, output_folder):
        all_data = {}
        output_files = self._get_files_in_dir(output_folder)
        for folder in output_files:
            filename = os.path.basename(folder)
            all_data[filename] = self._read_csv_to_list(folder)
        return all_data

    @staticmethod
    def _read_csv_to_list(csv_file_name):
        all_data = []
        with open(csv_file_name) as f:
            all_data.extend(iter(csv.DictReader(f, skipinitialspace=True)))
        return all_data

    @staticmethod
    def _read_json_file(file_name):
        with open(file_name) as json_file:
            return json.load(json_file)

    @staticmethod
    def _get_files_in_dir(dir_with_files):
        return [x for x in Path(dir_with_files).iterdir() if x.is_file()]

    def perform_test(self):
        logging.info(f"\nTesting {self.test_name}")

        json_parser = jc.JSONParser(configuration_dict=self.config)
        parsed_data = json_parser.parse_data(self.input_data)
        expected_tables = set(self.output_data.keys())
        real_tables = set(parsed_data.keys())

        logging.info("Comparing result table names to expected table names")
        self.assertTrue(self._compare_table_names(expected_tables, real_tables))
        logging.info("Comparing result table columns to expected table columns")
        self.assertTrue(self._compare_table_columns(self.output_data, parsed_data))
        logging.info("Comparing result tables to expected tables")
        self.assertTrue(self._compare_table_row_length(self.output_data, parsed_data))
        self.assertTrue(self._compare_table_values(self.output_data, parsed_data))

    @staticmethod
    def _compare_table_names(expected_tables, real_tables):
        test_passed = True
        if expected_tables != real_tables:
            logging.error(f"Expected tables {expected_tables} do not match real output {real_tables}")
            test_passed = False
        return test_passed

    @staticmethod
    def _compare_table_row_length(expected_data, real_data):
        test_passed = True
        for data_name in real_data:
            num_rows_expected = len(expected_data[data_name])
            num_rows_real = len(real_data[data_name])
            if num_rows_expected != num_rows_real:
                logging.error(
                    f"Row mismatch in {data_name}. Expected number of rows {num_rows_expected}, "
                    f"real number of rows {num_rows_real}")
                test_passed = False
        return test_passed

    @staticmethod
    def _compare_table_columns(expected_data, real_data):
        test_passed = True
        for data_name in real_data:
            real_keys = set(chain.from_iterable(real_data[data_name]))
            expected_keys = set(chain.from_iterable(expected_data[data_name]))
            if expected_keys != real_keys:
                logging.error(
                    f"Column mismatch in {data_name}. Expected keys {expected_keys}, real keys are {real_keys}")
                test_passed = False
        return test_passed

    def _compare_table_values(self, expected_data, real_data):
        test_passed = True
        for data_name in real_data:
            for i, row in enumerate(real_data[data_name]):
                for key, real_value in row.items():
                    expected_value = expected_data[data_name][i][key] or ""
                    real_value = real_value or ""
                    if str(real_value) != str(expected_value):
                        logging.error(f"Column mismatch in {data_name}. "
                                      f"Expected key:value {key}:{expected_value} got {real_value} instead")
                        test_passed = False
        return test_passed

    @staticmethod
    def _compare_list_of_dicts(list_1, list_2):
        row_is_different = False
        if len(list_1) != len(list_2):
            logging.error(
                f"Row count of true output {len(list_1)} is different from expected and reality: {len(list_2)}")
            return False
        for i, value in enumerate(list_1):
            for key in list_1[i]:
                if key not in list(list_2[i].keys()):
                    row_is_different = True
                else:
                    value_1 = str(list_1[i][key]) if list_2[i][key] else ''
                    value_2 = str(list_2[i][key]) if list_2[i][key] else ''
                    if value_1 != value_2:
                        row_is_different = True
            if row_is_different:
                logging.error(f"Row {i} is different from expected and reality: \n{list_1[i]}\n{list_2[i]}")
                return False
        return True
