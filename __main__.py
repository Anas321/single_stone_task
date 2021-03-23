"""Main function."""
import preprocessing as pp
import spark_sql as sp

import config

# Code entry point
if __name__ == '__main__':
    # Clean input files
    input_data_cleaned = pp.clean_input_files(input_files=config.INPUT_FILES)
    # Generate json report
    sp.generate_json_report(input_files=input_data_cleaned)
