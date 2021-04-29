import os
from process import *
import types
from produce import CSVInputStream
import csv
import pytest
import sys
sys.path.append("..")


async def test_process_infants_with_more_than_14_rings(create_process_instance, test_data_location):
    # test to check whether respective output file is created
    # by process_infants_with_more_than_14_rings or not

    source = "test_abalone_full.csv"
    sink = "test_infants_with_more_than_14_rings.csv"
    test_infant_processor = create_process_instance(
        source=source, sink=sink, type_="EnrichInfantAbalone")
    # as we are testing async functions
    await test_infant_processor
    assert os.path.exists(test_data_location+sink) == True
    # removing the test csv file as it's no longer required
    os.remove(test_data_location+sink)


async def test_process_males_heavy_and_short(create_process_instance, test_data_location):
    # test to check whether respective output file is created
    # by process_males_heavy_and_short or not
    source = "test_abalone_full.csv"
    sink = "test_males_heavy_and_short.csv"
    test_male_processor = create_process_instance(
        source=source, sink=sink, type_="EnrichMaleAbalone")
    # as we are testing async functions
    await test_male_processor
    assert os.path.exists(test_data_location+sink) == True
    # removing the test csv file as it's no longer required
    os.remove(test_data_location+sink)


async def test_process_shell_humidity(create_process_instance, test_data_location):
    # test to check whether respective output file is created
    # by process_shell_humidity or not
    source = "test_abalone_full.csv"
    sink = "test_shell_humidity.csv"
    test_shell_processor = create_process_instance(
        source=source, sink=sink, type_="FilterAbalone")
    # as we are testing async functions
    await test_shell_processor
    assert os.path.exists(test_data_location+sink) == True
    # removing the test csv file as it's no longer required
    os.remove(test_data_location+sink)
