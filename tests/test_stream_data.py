import types
from produce import CSVInputStream
import csv
import pytest
import sys
sys.path.append("..")


def test_get_input_stream():
    source = "../data/input/abalone_full.csv"
    data = CSVInputStream(source)
    data_stream = data.get_stream_data()
    # checking if the object returned is a generator
    assert isinstance(data_stream, types.GeneratorType)


def test_process_infants_with_more_than_14_rings_data(process_data):

    data = process_data(file_name="infants_with_more_than_14_rings.csv")
    header_fields = list(next(data).keys())
    assert header_fields == ["ID", "Sex", "Length",	"Diameter", "Height", "Whole_weight",
                             "Shucked_weight", "Viscera_weight", "Shell_weight", "Class_number_of_rings"]
    # checking type and logic of data produced
    for row in data:
        assert(isinstance(row['ID'], str))
        assert(isinstance(row['Sex'], str))
        assert(row['Sex'] == "I")
        assert(isinstance(row['Length'], float))
        assert(isinstance(row['Diameter'], float))
        assert(isinstance(row['Height'], float))
        assert(isinstance(row['Whole_weight'], float))
        assert(isinstance(row['Shucked_weight'], float))
        assert(isinstance(row['Viscera_weight'], float))
        assert(isinstance(row['Shell_weight'], float))
        assert(isinstance(row['Class_number_of_rings'], int))
        assert(row['Class_number_of_rings'] >= 14)


def test_males_heavy_and_short_data(process_data):

    data = process_data(file_name="males_heavy_and_short.csv")
    header_fields = list(next(data).keys())
    assert header_fields == ["ID", "Sex", "Length",	"Diameter", "Height", "Whole_weight",
                             "Shucked_weight", "Viscera_weight", "Shell_weight", "Class_number_of_rings"]
    # checking type and logic of data produced
    for row in data:
        assert(isinstance(row['ID'], int))
        assert(isinstance(row['Sex'], str))
        assert(row['Sex'] == "M")
        assert(isinstance(row['Length'], float))
        assert(row["Length"] < 0.5)
        assert(isinstance(row['Diameter'], float))
        assert(isinstance(row['Height'], float))
        assert(isinstance(row['Whole_weight'], float))
        assert(row["Whole_weight"] > 0.4)
        assert(isinstance(row['Shucked_weight'], float))
        assert(isinstance(row['Viscera_weight'], float))
        assert(isinstance(row['Shell_weight'], float))
        assert(isinstance(row['Class_number_of_rings'], int))


def test_shell_humidity_data(process_data):

    data = process_data(file_name="shell_humidity.csv")
    header_fields = list(next(data).keys())
    assert header_fields == ["ID", "Sex", "Length",	"Diameter", "Height", "Whole_weight",
                             "Shucked_weight", "Viscera_weight", "Shell_weight", "Class_number_of_rings", "Shell_humidity_weight"]

    # checking type and logic of data produced
    for row in data:
        assert(isinstance(row['ID'], int))
        assert(isinstance(row['Sex'], str))
        assert(isinstance(row['Length'], float))
        assert(isinstance(row['Diameter'], float))
        assert(isinstance(row['Height'], float))
        assert(isinstance(row['Whole_weight'], float))
        assert(isinstance(row['Shucked_weight'], float))
        assert(isinstance(row['Viscera_weight'], float))
        assert(isinstance(row['Shell_weight'], float))
        assert(isinstance(row['Class_number_of_rings'], int))
        assert(isinstance(row['Shell_humidity_weight'], float))
        assert(row['Shell_humidity_weight'] >= 0)
