from abc import ABC, abstractmethod
import pandas as pd
import asyncio


class Producer(ABC):
    # abstract class which can be implemented based on input stream
    @abstractmethod
    def __init__(self, source):
        self.source = source

    @abstractmethod
    def get_stream_data(self):
        pass


class CSVInputStream(Producer):
    # Input stream class for csv files
    def __init__(self, source):
        super().__init__(source)

    def get_stream_data(self):
        # Loading data through generators, avoiding memory error
        yield from pd.read_csv(self.source).to_dict("records")
