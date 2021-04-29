from process import *
from produce import CSVInputStream
import pytest
import csv
import os
import sys
sys.path.append("..")


@pytest.fixture(scope="module")
# fixture to get input data location
def input_data_location():
    return 'data/input/'


@pytest.fixture(scope="module")
# fixture to get output data location
def output_data_location():
    return 'data/output/'


@pytest.fixture(scope='module')
# fixture to get test data location
def test_data_location():
    return 'data/test/'


@pytest.fixture(scope="function")
# fixture to get create data to be processed via get_stream_data
def process_data(input_data_location, output_data_location, test_data_location):

    output_files = os.listdir(output_data_location)
    input_files = os.listdir(input_data_location)

    def _get_file(file_name):
        if file_name in output_files:
            output_stream = CSVInputStream(output_data_location+file_name)
            data = output_stream.get_stream_data()
        elif file_name in input_files:
            input_stream = CSVInputStream(input_data_location+file_name)
            data = input_stream.get_stream_data()
        else:
            test_stream = CSVInputStream(file_name)
            data = test_stream.get_stream_data()

        return data

    yield _get_file


@pytest.fixture(scope='function')
# fixture which creates a processor as required
def create_process_instance(test_data_location, process_data):
    # nested async fixture to go for specific type_ , as it is testing async functions
    async def _call_specific_instance(source, sink, type_):

        source = test_data_location + source
        sink = test_data_location + sink
        data = process_data(file_name=source)
        if type_ == "EnrichInfantAbalone":
            processor = EnrichInfantAbalone(source, sink)
            task = processor.process(data)
            await task
        elif type_ == "EnrichMaleAbalone":
            processor = EnrichMaleAbalone(source, sink)
            task = processor.process(data)
            await task
        else:
            processor = FilterAbalone(source, sink)
            task = processor.process(data)
            await task

        return processor

    yield _call_specific_instance
