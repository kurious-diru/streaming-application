from abc import ABC, abstractmethod
import pandas as pd
import csv


class Processor(ABC):
    #  Abstract Processor class process data
    @abstractmethod
    def __init__(self, source, sink):
        # Instantiating required variables which are needed to do the operations
        self.id = 1
        self.sink = sink
        self.source = source
        with open(sink, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["ID", "Sex", "Length",	"Diameter", "Height", "Whole_weight",
                            "Shucked_weight", "Viscera_weight", "Shell_weight", "Class_number_of_rings"])
            file.close()

    @abstractmethod
    def stream_data(self):
        # Loading data when through generators, avoiding memory error
        for record in pd.read_csv(self.source).to_dict('records'):
            yield record


class Processor1(Processor):

    def __init__(self, source, sink):
        super().__init__(source, sink)
        # Introducing state to help enumerating IDs
        self.state = {}
        # Overriding ID with appropriate structure representing rings
        self.id = "R_"

    def stream_data(self):
        super().stream_data()

    def process_infants_with_more_than_14_rings(self):
        data = super().stream_data()
        # Simulating each record feeding to the processor
        for record in data:
            rings = record["Class_number_of_rings"]
            # Logic to consider appropiate records
            if record["Sex"] == "I" and rings >= 14:
                if rings in self.state:
                    self.state[rings] = self.state[rings] + 1
                else:
                    self.state[rings] = 1

                self.id = self.id + str(rings) + \
                    "_" + str(self.state[rings])

                list_record = list(record.values())
                list_record.insert(0, self.id)
                # Appeding the appropriate data with an extra ID attribute
                with open(self.sink, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(list_record)
                    file.close()
            self.id = "R_"


class Processor2(Processor):

    def __init__(self, source, sink):
        super().__init__(source, sink)

    def stream_data(self):
        super().stream_data()

    def process_males_heavy_and_short(self):
        data = super().stream_data()
        # Simulating each record feeding to the processor
        for record in data:
            # Logic to consider appropiate records
            if record["Sex"] == "M" and record["Whole_weight"] > 0.4 and record["Length"] < 0.5:
                list_record = list(record.values())
                list_record.insert(0, self.id)
                self.id = self.id + 1
                # Appeding the appropriate data with an extra ID attribute
                with open(self.sink, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(list_record)
                    file.close()


class Processor3(Processor):

    def __init__(self, source, sink):
        super().__init__(source, sink)
        with open(sink, 'w', newline='') as file:
            writer = csv.writer(file)
            # Overriding attributes in csv as we introduce new attribute called "Shell_humidity_weight"
            writer.writerow(["ID", "Sex", "Length",	"Diameter", "Height", "Whole_weight",
                            "Shucked_weight", "Viscera_weight", "Shell_weight", "Class_number_of_rings", "Shell_humidity_weight"])
            file.close()

    def stream_data(self):
        super().stream_data()

    def process_shell_humidity(self):
        data = super().stream_data()
        # Simulating each record feeding to the processor
        for record in data:
            shell_humidity_weight = (
                record["Whole_weight"] - record["Shucked_weight"]) - record["Shell_weight"]
            shell_humidity_weight = round(shell_humidity_weight, 3)
            # Logic to consider appropiate records
            if shell_humidity_weight > 0:
                list_record = list(record.values())
                list_record.insert(0, self.id)
                list_record.append(shell_humidity_weight)
                self.id = self.id + 1
                # Appeding the appropriate data with extra ID and Shell_humidity_weight attributes
                with open(self.sink, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(list_record)
                    file.close()


if __name__ == '__main__':

    # take source input data
    source = "data/input/abalone_full.csv"

    sink1 = "data/output/infants_with_more_than_14_rings.csv"
    infant_processor = Processor1(source, sink1)
    infant_processor.process_infants_with_more_than_14_rings()

    sink2 = "data/output/males_heavy_and_short.csv"
    males_processor = Processor2(source, sink2)
    males_processor.process_males_heavy_and_short()

    sink3 = "data/output/shell_humidity.csv"
    shell_processor = Processor3(source, sink3)
    shell_processor.process_shell_humidity()
