from process import *
import asyncio
import time


def stream_data(source):
    # Loading data through generators, avoiding memory error
    for record in pd.read_csv(source).to_dict('records'):
        yield record


async def main():

    begin = time.time()
    # take source input data
    source = "data/input/abalone_full.csv"

    data = stream_data(source)

    sink1 = "data/output/infants_with_more_than_14_rings.csv"
    infant_processor = EnrichInfant(source, sink1)

    sink2 = "data/output/males_heavy_and_short.csv"
    males_processor = EnrichMale(source, sink2)

    sink3 = "data/output/shell_humidity.csv"
    shell_processor = FilterAbalone(source, sink3)

    for record in data:
        # process data with given constraints on infant abalone
        task1 = asyncio.create_task(
            infant_processor.process_infants_with_more_than_14_rings(record))

        # process data with given constraints on male abalone
        task2 = asyncio.create_task(
            males_processor.process_males_heavy_and_short(record))

        # process data with given constraints on all abalone
        task3 = asyncio.create_task(
            shell_processor.process_shell_humidity(record))

    # awaiting till all 3 tasks are done asynchronously
        value3 = await task3  # task3 probably has to process more records than other tasks
        value2 = await task2
        value1 = await task1
    end = time.time()

    print(f"Time taken to run the program asynchronously is {end - begin}")


asyncio.run(main())
