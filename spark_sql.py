"""Start a spark session to generate the required document."""
from pyspark.sql import SparkSession

import config


def generate_json_report(input_files, report_file_name=config.OUTPUT_FILE):
    """Generate a json report by starting a spark session.

    Args:
        - input_files: input files names.
        - report_file_name: name of the output json file.
    Returns:
        - There are no to direct returns from this function.
    """
    # Start the spark session
    spark = (SparkSession
             .builder
             .appName('SparkApplication')
             .getOrCreate())

    # Loop through the input files
    for file in input_files:
        file_format = file.split('.')[1]
        table_name = file.split('.')[0]
        (spark.read.format(file_format)
         .option("inferSchema", "true")
         .option("header", "true")
         .load(file)
         .select('fname', 'lname', 'cid')).createOrReplaceTempView(table_name)

    # SQL query to join the two tables
    ((spark.sql("""
        SELECT concat(st.fname, ' ', st.lname) as `student name`,
               concat(te.fname, ' ', te.lname) as `teacher name`,
           st.cid as `class ID`
        FROM {0} as st
        JOIN {1} as te
            ON st.cid = te.cid
        """.format(input_files[0].split('.')[0],
                   input_files[1].split('.')[0])))
        .toPandas()
        .to_json(report_file_name))

    # Stop the spark session
    spark.stop()
