import pandas as pd
import re
from unidecode import unidecode
import numpy as np
import logging


class DatabaseBuilder:
    def __init__(
        self,
        posts_csv_path,
        tags_csv_path,
        current_dataset,
        implication_csv_path,
    ) -> None:
        self.posts_csv_path = posts_csv_path
        self.tags_csv_path = tags_csv_path
        self.current_dataset = current_dataset
        self.implication_csv_path = implication_csv_path

    def create_pandas_dataframe(self, csv_path):
        # Read the CSV file using Pandas
        return pd.read_csv(csv_path, dtype="object")

    def build_database(self):
        # Load the CSV files into Pandas dataframes
        logging.info("Loading CSV files into Pandas dataframes...")
        new_posts_df = self.create_pandas_dataframe(self.posts_csv_path)
        # current_dataset_df = self.create_pandas_dataframe(self.current_dataset)
        tags_df = self.create_pandas_dataframe(self.tags_csv_path)
        implication_df = self.create_pandas_dataframe(self.implication_csv_path)

        # Add new columns to the dataframes
        new_posts_df["unpruned_artist"] = ""
        new_posts_df["pruned_artist"] = ""
        new_posts_df["tag_string_firstprune"] = ""
        implication_df["antecedent_name_pruned"] = ""
        implication_df["consequent_name_pruned"] = ""
        tags_df["name_firstprune"] = ""
        tags_df["keep"] = ""

        # Create a new dataframe pruned_posts as a copy of new_posts
        pruned_posts_df = new_posts_df.copy()

        # Write the dataframes to Parquet files
        new_posts_df.to_parquet("new_posts.parquet", compression="snappy")
        # current_dataset_df.to_parquet("current_dataset.parquet", compression="snappy")
        tags_df.to_parquet("tags.parquet", compression="snappy")
        implication_df.to_parquet("implication.parquet", compression="snappy")
        pruned_posts_df.to_parquet("pruned_posts.parquet", compression="snappy")

    def first_prune(self):
        # Read the Parquet files into Pandas dataframes
        logging.info("Initiating first prune...")
        tags_df = pd.read_parquet("tags.parquet")
        implication_df = pd.read_parquet("implication.parquet")
        pruned_posts_df = pd.read_parquet("pruned_posts.parquet")

        # Delete rows with NULL values in new_tags
        tags_df = tags_df.dropna(subset=["name"])

        # Update new_tags
        tags_df["name_firstprune"] = (
            tags_df["name"].str.replace(" ", ", ").str.replace("_", " ")
        )

        # Update implication_tags
        implication_df["antecedent_name_pruned"] = (
            implication_df["antecedent_name"]
            .str.replace(" ", ", ")
            .str.replace("_", " ")
        )
        implication_df["consequent_name_pruned"] = (
            implication_df["consequent_name"]
            .str.replace(" ", ", ")
            .str.replace("_", " ")
        )

        # Update new_posts
        pruned_posts_df["tag_string_firstprune"] = (
            pruned_posts_df["tag_string"].str.replace(" ", ", ").str.replace("_", " ")
        )

        pruned_posts_df["tag_string_firstprune"] = pruned_posts_df[
            "tag_string_firstprune"
        ].fillna("")

        tags_df["post_count"] = tags_df["post_count"].astype(int)

        # Strip all rows in pruned_posts_df with 'is_deleted' == 't'
        pruned_posts_df = pruned_posts_df[pruned_posts_df["is_deleted"] != "t"]

        # Write the updated dataframes back to Parquet files
        tags_df.to_parquet("tags.parquet", compression="snappy")
        implication_df.to_parquet("implication.parquet", compression="snappy")
        pruned_posts_df.to_parquet("pruned_posts.parquet", compression="snappy")
