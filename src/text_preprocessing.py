# Importing necessary libraries
import polars as pl
import os
from processtext import clean
# Getting the parent directory path
PARENT_DIR = os.path.abspath(os.getcwd()) # os.path.abspath(os.path.join(os.getcwd(), os.pardir))

# Creating a function that will remove one character words from a given sentense
def one_word_remover(text_:str)->str:
    """This function removes one character words from the sentense

    Args:
        text_ (str): _description_

    Returns:
        str: _description_
    """
    list_of_words = []
    for word in text_.split(" "):
        if len(word) > 1:
            list_of_words.append(word)
    else:
        pass

    modified_etxt = ' '.join(list_of_words)
    return modified_etxt

def processed_df_maker(PATH = "/data/raw/PubMed_MultiLabel_Dataset.parquet") -> pl.DataFrame:
    """ Basic preprocessing over the raw dataframe

    Args:
        PATH (str, optional): Raw dataframe path. Defaults to "./../data/raw/PubMed_MultiLabel_Dataset.parquet".

    Returns:
        pl.DataFrame: _description_
    """
    
    # Scanning the table
    lazy_df = pl.scan_parquet(PARENT_DIR + PATH)
    # Doing basic preprocessing 
    lazy_query = (lazy_df.drop_nulls().with_columns(
                    [pl.col("Anatomy").cast(pl.Int8), # changing the column types to int8
                     pl.col("Diseases").cast(pl.Int8), # changing the column types to int8
                     pl.col("Chemicals and Drugs").cast(pl.Int8), # changing the column types to int8
                     pl.col("Phenomena and Processes").cast(pl.Int8), # changing the column types to int8
                     pl.col("Health Care").cast(pl.Int8),  # changing the column types to int8
                     pl.col("Named Groups").cast(pl.Int8), # changing the column types to int8
                     pl.col("Diagnostic Techniques and Equipment").cast(pl.Int8), # changing the column types to int8
                     pl.col("Psychiatry and Psychology").cast(pl.Int8), # changing the column types to int8
                     pl.concat_str(['Title', 'abstractText'], separator= ' ').alias("text") # Adding Title and abstractText column to create a single text column
                    ]))
    raw_data = lazy_query.collect()
    # Cleaning the text columns of the raw data
    data = raw_data.with_columns(pl.col("text").map_elements(clean) # Cleaning the text column
                                ).drop(['Title', 'abstractText'])
    # Removing one word characters
    data = data.with_columns(pl.col('text').map_elements(one_word_remover))
    
    return data

# Running the function
if __name__ == "__main__":
    raw_df = processed_df_maker()
    path = PARENT_DIR + "/data/processed/processed_data.parquet"
    raw_df.write_parquet(path)