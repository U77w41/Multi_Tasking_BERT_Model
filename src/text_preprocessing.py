# Importing necessary libraries
import polars as pl

def processed_df_maker(PATH = "./../data/raw/PubMed_MultiLabel_Dataset.parquet") -> pl.DataFrame:
    """ Basic preprocessing over the raw dataframe

    Args:
        PATH (str, optional): Raw dataframe path. Defaults to "./../data/raw/PubMed_MultiLabel_Dataset.parquet".

    Returns:
        pl.DataFrame: _description_
    """
    
    # Reading the table
    data = pl.read_parquet(PATH)
    # Adding Title and abstractText column to create a single text column
    data = data.with_columns(pl.concat_str(['Title', 'abstractText']).alias("text"))
    # Doing some basic preprocessing
    data = data.with_columns(
                [pl.col("Anatomy").cast(pl.Int8), # changing the column types to int8
                 pl.col("Diseases").cast(pl.Int8), # changing the column types to int8
                 pl.col("Chemicals and Drugs").cast(pl.Int8), # changing the column types to int8
                 pl.col("Phenomena and Processes").cast(pl.Int8), # changing the column types to int8
                 pl.col("Health Care").cast(pl.Int8), # changing the column types to int8
                 pl.col("Named Groups").cast(pl.Int8), # changing the column types to int8
                 pl.col("Diagnostic Techniques and Equipment").cast(pl.Int8), # changing the column types to int8
                 pl.col("Psychiatry and Psychology").cast(pl.Int8), # changing the column types to int8
                 pl.col("text").str.replace_all(r'[^a-zA-Z0-9\s]',' ').str.replace_all(r'\s+', ' ') # Replacing all the non text characters with spaces
                 ]
                )
    # Dropping unnecessary columns
    data = data.drop(['Title', 'abstractText'])
    return data

# Running the function
if __name__ == "__main__":
    PATH = "/home/ujjwal/Downloads/Ujjwal/Personal Projects/Multi_Tasking_BERT_Model/data/raw/PubMed_MultiLabel_Dataset.parquet"
    raw_df = processed_df_maker(PATH)
    path = "/home/ujjwal/Downloads/Ujjwal/Personal Projects/Multi_Tasking_BERT_Model/data/processed/processed_data.parquet"
    raw_df.write_parquet(path)