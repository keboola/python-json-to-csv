# Python JSON to CSV Library

## Introduction

JSON-to-CSV is a Python library designed to simplify the process of parsing JSON data and converting it into CSV files. It provides a flexible and efficient solution for handling JSON data with complex structures and converting it into structured CSV files for easy analysis and storage. The library offers a user-friendly interface for specifying table mappings, primary keys, and data types, allowing users to customize the output CSV format to suit their specific needs. Whether you are dealing with large-scale JSON datasets or small JSON objects, JSON-to-CSV offers powerful tools to streamline the parsing and conversion process, making it an indispensable tool for data engineers and analysts.

## How it Works

The parser in JSON-to-CSV operates in three main steps. First, it analyzes the JSON data and constructs a mapping of tables, columns, and data types based on the user-provided table mapping or through automatic inference if not explicitly specified. Next, it processes the JSON data and organizes it into a structured format according to the defined table mapping. During this step, the parser performs additional analysis for data that is not explicitly specified in the table mapping if the user chooses to do so. Finally, the parsed data is saved as CSV files, with each table represented as a separate CSV file, preserving the hierarchical relationships between the tables.

## Quickstart



### Installation

```
pip install keboola.json-to-csv
```

### Usage

See Examples folder for usage

