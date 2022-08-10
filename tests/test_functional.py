import unittest
import os
import logging
from tests.json_case_tester.case_tester import JSONCaseTester

TEST_LOCATION = "../tests/test-cases"


class TestJSONParser(unittest.TestCase):
    def test_json_test_cases(self):
        logging.getLogger().setLevel(logging.INFO)
        suite = _build_dir_test_suite(TEST_LOCATION)
        test_runner = unittest.TextTestRunner(verbosity=3)
        result = test_runner.run(suite)
        if not result.wasSuccessful():
            raise AssertionError(f'Functional test suite failed. {result.errors}')


def _build_dir_test_suite(testing_dir):
    """
    Creates a test suite for a directory, each test is added using addTest to pass through parameters

    Args:
        testing_dir: directory that holds testing directories

    Returns:
        Unittest Suite containing all functional tests

    """
    folders = [f.path for f in os.scandir(testing_dir) if f.is_dir()]
    suite = unittest.TestSuite()

    for test_folder in folders:
        suite.addTest(JSONCaseTester(test_folder))
    return suite


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
