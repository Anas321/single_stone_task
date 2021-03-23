"""Preprocessing input files.

This module contatins the functions
needed to preprocess the input files.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# Define global variables
CHUNK_SIZE = 100


def clean_csv_file(chunk):
    """Clean a csv input file.

    Args:
        - chunk: the current chunk of data to clean.
    Returns:
        - chunk_cleaned: the current chunk of data after it is cleaned.
    """
    chunk_cleaned = (chunk[chunk.columns[0]]
                     .str.split('_', expand=True))
    # Clean the columns names
    chunk_cleaned.columns = chunk.columns.str.split('_')[0]
    return chunk_cleaned


def clean_parquet_file(file):
    """Clean a parquet input file.

    Args:
        - file: parquet file name.
    Returns:
        - cleaned_file_name: the name of the cleaned parquet file.
    """
    cleaned_file_name = 'cleaned_' + file
    teachers_df = pd.read_parquet(file)
    teachers_df['id'] = range(1, teachers_df.shape[0]+1)
    teachers_df.to_parquet(cleaned_file_name)
    return cleaned_file_name


def convert_csv_to_parquet(file):
    """Convert an input csv file to a parquet one, after cleaning it.

    Args:
        - file: the name of the csv file to be cleaned.
    Returns:
        - cleaned_file_name: the name of the cleaned csv file.
    """
    cleaned_file_name = 'cleaned_' + file.split('.')[0] + '.parquet'
    csv_stream = pd.read_csv(file, chunksize=CHUNK_SIZE)
    for i, chunk in enumerate(csv_stream):
        # To get the schema
        if i == 0:
            chunk_cleaned = clean_csv_file(chunk)
            # Get the schema
            parquet_schema = pa.Table.from_pandas(df=chunk_cleaned).schema
            # Open parquet file for writing
            parquet_writer = pq.ParquetWriter(
                                            cleaned_file_name,
                                            parquet_schema,
                                            compression='snappy')
        chunk_cleaned = clean_csv_file(chunk)
        # Write csv chunk to the parquet file
        table = pa.Table.from_pandas(
            df=chunk_cleaned, schema=parquet_schema)
        parquet_writer.write_table(table)
    parquet_writer.close()
    return cleaned_file_name


def clean_input_files(input_files):
    """Generate cleaned input files.

    Args:
        - self: just the class self pointer.
    Returns:
        - cleaned_files_names: a list of cleaned files names.
    """
    cleaned_files_names = []
    for file in input_files:
        # Read csv files
        if file.endswith('csv'):
            file_name = convert_csv_to_parquet(file)
            cleaned_files_names.append(file_name)
        # Read parquet files
        elif file.endswith('parquet'):
            file_name = clean_parquet_file(file)
            cleaned_files_names.append(file_name)
    return cleaned_files_names
