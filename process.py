from abc import ABC, abstractmethod
import csv
import time
from produce import CSVInputStream
import asyncio
import time


class Processor(ABC):
    #  Abstract Processor class to process data
    @abstractmethod
    def __init__(self, source, sink):
        # Instantiating required variables which are needed to do the operations
        self.id = 1
        self.sink = sink
        self.source = source
        self.headers = list()
        with open(source, "r") as f:
            d_reader = csv.DictReader(f)
            self.headers = d_reader.fieldnames
            f.close()
        self.headers.insert(0, "ID")
        with open(sink, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(self.headers)
            file.close()

    @abstractmethod
    async def process(self, input_stream: CSVInputStream):
        raise NotImplementedError()


class EnrichInfantAbalone(Processor):

    def __init__(self, source, sink):
        super().__init__(source, sink)
        # Introducing state to help enumerating IDs
        self.state = {}
        # Overriding ID with appropriate structure representing rings
        self.id = "R_"

    async def process(self, input_stream):
        # Simulating each record feeding to the processor
        for record in input_stream:
            # await asyncio.sleep(0.001)
            rings = record["Class_number_of_rings"]
            # Logic to consider appropiate records
            if record["Sex"] == "I" and rings >= 14:
                if rings in self.state:
                    self.state[rings] = self.state[rings] + 1
                else:
                    self.state[rings] = 1
                row_id = self.id
                row_id = row_id + str(rings) + \
                    "_" + str(self.state[rings])

                list_record = list(record.values())
                list_record.insert(0, row_id)
                # Appeding the appropriate data with an extra ID attribute
                with open(self.sink, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(list_record)
                    file.close()


class EnrichMaleAbalone(Processor):

    def __init__(self, source, sink):
        super().__init__(source, sink)

    async def process(self, input_stream):
        # Simulating each record feeding to the processor
        for record in input_stream:
            # Logic to consider appropiate records
            if record["Sex"] == "M" and record["Whole_weight"] > 0.4 and record["Length"] < 0.5:
                list_record = list(record.values())
                list_record.insert(0, self.id)
                self.id = self.id + 1
                # Appending the appropriate data with an extra ID attribute
                with open(self.sink, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(list_record)
                    file.close()


class FilterAbalone(Processor):

    def __init__(self, source, sink):
        super().__init__(source, sink)
        with open(sink, 'w', newline='') as file:
            writer = csv.writer(file)
            # Overriding attributes in csv as we introduce new attribute called "Shell_humidity_weight"
            self.headers.append("Shell_humidity_weight")
            writer.writerow(self.headers)
            file.close()

    async def process(self, input_stream):
        # Simulating each record feeding to the processor
        for record in input_stream:
            shell_humidity_weight = (
                record["Whole_weight"] - record["Shucked_weight"]) - record["Shell_weight"]
            shell_humidity_weight = round(shell_humidity_weight, 3)
            # Logic to consider appropiate records
            if shell_humidity_weight > 0:
                list_record = list(record.values())
                list_record.insert(0, self.id)
                list_record.append(shell_humidity_weight)
                self.id = self.id + 1
                # Appending the appropriate data with extra ID and Shell_humidity_weight attributes
                with open(self.sink, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(list_record)
                    file.close()


if __name__ == '__main__':

    # take source input data
    source = "data/input/abalone_full.csv"

    data = CSVInputStream(source)

    sink1 = "data/output/infants_with_more_than_14_rings.csv"
    infant_processor = EnrichInfantAbalone(source, sink1)

    sink2 = "data/output/males_heavy_and_short.csv"
    males_processor = EnrichMaleAbalone(source, sink2)

    sink3 = "data/output/shell_humidity.csv"
    shell_processor = FilterAbalone(source, sink3)

    # process data with given constraints on all abalone
    shell_processor.process(data.get_stream_data())

    # process data with given constraints on infant abalone
    infant_processor.process(data.get_stream_data())

    # process data with given constraints on male abalone
    males_processor.process(data.get_stream_data())
