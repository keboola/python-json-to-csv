import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="keboola.json-to-csv",
    version="0.0.1",
    author="Keboola KDS Team",
    setup_requires=['flake8'],
    tests_require=[],
    install_requires=[],
    author_email="support@keboola.com",
    description="General utility library for Python applications running in Keboola Connection environment",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/keboola/python-utils",
    packages=['keboola.json_to_csv', 'keboola.json_to_csv.configuration', 'keboola.json_to_csv.exception',
              'keboola.json_to_csv.value_object'],
    package_dir={'': 'src'},
    include_package_data=True,
    zip_safe=False,
    test_suite='tests',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Education",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
    python_requires='>=3.7'
)
