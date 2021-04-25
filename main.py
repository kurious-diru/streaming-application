from process import *

# take source input data
source = "data/input/abalone_full.csv"

sink1 = "data/output/infants_with_more_than_14_rings.csv"
infant_processor = Processor1(source, sink1)
# process data with given constraints on infant abalone
infant_processor.process_infants_with_more_than_14_rings()

sink2 = "data/output/males_heavy_and_short.csv"
males_processor = Processor2(source, sink2)
# process data with given constraints on male abalone
males_processor.process_males_heavy_and_short()

sink3 = "data/output/shell_humidity.csv"
shell_processor = Processor3(source, sink3)
# process data with given constraints on all abalone
shell_processor.process_shell_humidity()
