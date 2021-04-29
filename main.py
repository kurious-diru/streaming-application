from process import *
import asyncio
import time
from produce import CSVInputStream


async def main():

    # take source input data
    source = "data/input/abalone_full.csv"
    # getting data from generator
    data = CSVInputStream(source)

    sink3 = "data/output/shell_humidity.csv"
    shell_processor = FilterAbalone(source, sink3)
    # process data with given constraints on all abalone
    task3 = asyncio.create_task(
        shell_processor.process(data.get_stream_data()))

    sink1 = "data/output/infants_with_more_than_14_rings.csv"
    infant_processor = EnrichInfantAbalone(source, sink1)
    # process data with given constraints on infant abalone
    task1 = asyncio.create_task(
        infant_processor.process(data.get_stream_data()))

    sink2 = "data/output/males_heavy_and_short.csv"
    males_processor = EnrichMaleAbalone(source, sink2)
    # process data with given constraints on male abalone
    task2 = asyncio.create_task(
        males_processor.process(data.get_stream_data()))

    # awaiting till all 3 tasks are done asynchronously
    await task3  # takes the most time among all tasks
    await task2
    await task1

asyncio.run(main())
