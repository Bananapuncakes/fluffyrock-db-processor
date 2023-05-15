import re
from unidecode import unidecode
import numpy as np
import json
import sqlite3
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor
from unidecode import unidecode
import logging
from rich.logging import RichHandler
import pyarrow.parquet as pq

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)
# Change rich log level to debug
logging.getLogger("rich").setLevel("INFO")

logging = logging.getLogger("rich")


class DatasetPruner:
    def forbidden_tags(self, pruned_posts_df):
        task_number = 1
        # Read the forbidden tags from the JSON file
        with open("json/forbidden_tags.json", "r") as f:
            forbidden_tags = json.load(f)

        # Create a compiled regular expression pattern
        forbidden_tags_pattern = re.compile("|".join(forbidden_tags))

        logging.info(f"Number of rows before filtering: {len(pruned_posts_df)}")

        # Filter out posts containing forbidden tags
        completed_pruned_posts_df = pruned_posts_df[
            ~pruned_posts_df["tag_string_firstprune"]
            .fillna("")
            .str.contains(forbidden_tags_pattern)
        ]

        logging.info(
            f"Number of rows after filtering: {len(completed_pruned_posts_df)}"
        )

        return completed_pruned_posts_df, task_number

    def convert_accent_characters(self, pruned_posts_df):
        task_number = 3
        # Split the tags in the DataFrame
        pruned_posts_df["split_tags"] = pruned_posts_df[
            "tag_string_firstprune"
        ].str.split(", ")

        # Remove accent characters from tags
        pruned_posts_df["split_tags"] = pruned_posts_df["split_tags"].apply(
            lambda tags: [unidecode(tag) for tag in tags]
        )

        # Rejoin the cleaned tags
        pruned_posts_df["tag_string_firstprune"] = pruned_posts_df["split_tags"].apply(
            lambda tags: ", ".join(tags)
        )

        # Drop the temporary split_tags column
        completed_pruned_posts_df = pruned_posts_df.drop(columns=["split_tags"])

        return completed_pruned_posts_df, task_number

    def has_meme_tag(self, tags, allowed_meme_tags):
        if tags is None:
            return False

        tags = set(tags)  # remove .split(", ") here
        return "meme" in tags and not any(tag in tags for tag in allowed_meme_tags)

    def strip_meme_tags(self, pruned_posts_df):
        task_number = 3
        with open("json/meme_tags.json", "r") as f:
            allowed_meme_tags = json.load(f)

        logging.info(f"Number of rows before filtering: {len(pruned_posts_df)}")

        # Split the tags in the DataFrame
        pruned_posts_df["split_tags"] = pruned_posts_df[
            "tag_string_firstprune"
        ].str.split(", ")

        # Apply the has_meme_tag function to the split_tags column
        mask = pruned_posts_df["split_tags"].apply(
            self.has_meme_tag, allowed_meme_tags=allowed_meme_tags
        )

        # Filter out posts with meme tags
        pruned_posts_df = pruned_posts_df[~mask]

        # Drop the temporary split_tags column and create a new DataFrame
        pruned_posts_df = pruned_posts_df.drop(columns=["split_tags"])

        # Update the original DataFrame
        completed_pruned_posts_df = pruned_posts_df

        logging.info(
            f"Number of rows after filtering: {len(completed_pruned_posts_df)}"
        )

        return completed_pruned_posts_df, task_number

    def create_implication_tag_index():
        # Read the parquets into DataFrames
        tag_index = pd.read_parquet("tag_index.parquet")
        implications = pd.read_parquet("implication.parquet")

        # Drop rows in 'implications' that don't have 'status' as 'active'
        implications = implications[implications["status"] == "active"]

        # Explode tag_index DataFrame
        exploded_tag_index = tag_index.explode("id")

        # Merge 'tag_index' and 'implications' based on the matching condition
        merged = exploded_tag_index.merge(
            implications, left_on="tag", right_on="antecedent_name_pruned"
        )

        # Get the DataFrame with post_ids and consequent_name_pruned
        consequent_post_ids = exploded_tag_index.merge(
            implications, left_on="tag", right_on="consequent_name_pruned"
        )

        # Filter rows in 'merged' where post_ids are not in 'consequent_post_ids'
        filtered = merged[~merged["id"].isin(consequent_post_ids["id"])]

        # Group by antecedent_name_pruned, consequent_name_pruned, and id_y (implication_id)
        grouped = (
            filtered.groupby(
                ["antecedent_name_pruned", "consequent_name_pruned", "id_y"]
            )["id"]
            .apply(list)
            .reset_index()
        )

        # Rename the columns
        grouped = grouped.rename(columns={"id_y": "implication_id", "id": "post_ids"})

        # Convert the 'post_ids' column values to strings and wrap each id in quotations
        grouped["post_ids"] = grouped["post_ids"].apply(
            lambda x: ", ".join(f"'{str(id)}'" for id in x)
        )

        # Save the final DataFrame to a parquet file
        grouped.to_parquet("implication_tag_index.parquet", index=False)

        # Save the final DataFrame as an SQLite database
        conn = sqlite3.connect("implication_tag_index.sqlite")
        grouped.to_sql("implication_tag_index", conn, if_exists="replace", index=False)
        conn.close()

    def append_e621_tag(self, pruned_posts_df):
        task_number = 4
        # Add 'e621' as the first tag if not already present
        mask = ~pruned_posts_df["tag_string_firstprune"].str.startswith("e621")
        pruned_posts_df.loc[mask, "tag_string_firstprune"] = (
            "e621, " + pruned_posts_df.loc[mask, "tag_string_firstprune"]
        )

        return pruned_posts_df, task_number

    def has_artist_under_10_posts(self, tags, artists_under_10_set):
        if tags is None:
            return False

        return any(artist in artists_under_10_set for artist in tags)

    def remove_artist_tags_under_10_posts(self, pruned_posts_df):
        task_number = 5
        # Read pruned_artists DataFrame
        pruned_artists_df = pd.read_parquet("pruned_artists.parquet")

        # Convert the artists_under_10 series to a set
        artists_under_10_set = set(pruned_artists_df["name_firstprune"])

        logging.info(f"Number of rows before filtering: {len(pruned_posts_df)}")

        # Split the tags in the DataFrame
        pruned_posts_df["split_tags"] = pruned_posts_df[
            "tag_string_firstprune"
        ].str.split(", ")

        # Remove artist tags with under 10 posts from the split_tags column
        pruned_posts_df["split_tags"] = pruned_posts_df["split_tags"].apply(
            lambda tags: [tag for tag in tags if tag not in artists_under_10_set]
        )

        # Rejoin the cleaned tags
        pruned_posts_df["tag_string_firstprune"] = pruned_posts_df["split_tags"].apply(
            lambda tags: ", ".join(tags)
        )

        # Drop the temporary split_tags column and create a new DataFrame
        completed_pruned_posts_df = pruned_posts_df.drop(columns=["split_tags"])

        logging.info(
            f"Number of rows after filtering: {len(completed_pruned_posts_df)}"
        )

        return completed_pruned_posts_df, task_number

    def add_by_to_artist(self, pruned_posts_df, usable_artists_df):
        task_number = 6
        # Read pruned_artists DataFrame
        pruned_artist_names = usable_artists_df["name_firstprune"].to_numpy()

        # Remove the '(artist)' part from the artist names and add 'by ' in front of them
        modified_artist_names = [
            "by " + name.replace(" (artist)", "") for name in pruned_artist_names
        ]

        # Create a dictionary mapping artist names to their modified version
        artist_mapping = dict(zip(pruned_artist_names, modified_artist_names))

        # Replace the artist names in the tag_string_firstprune column
        def replace_artist_name(tag_string):
            tag_list = tag_string.split(", ")
            new_tag_list = [
                artist_mapping.get(tag, tag) if tag in artist_mapping else tag
                for tag in tag_list
            ]
            return ", ".join(new_tag_list)

        completed_pruned_posts_df = pruned_posts_df.copy()
        completed_pruned_posts_df["tag_string_firstprune"] = pruned_posts_df[
            "tag_string_firstprune"
        ].apply(replace_artist_name)

        return completed_pruned_posts_df, task_number

    def strip_bad_tags(self, pruned_posts_df):
        task_number = 7
        # Read the tags from the JSON file
        with open("json/bad_tags.json", "r") as f:
            all_tags = json.load(f)

        # Fetch tags with keep = 'n'
        bad_tags_from_new_tags = {tag["name"] for tag in all_tags if tag["keep"] == "n"}

        # Split the tags in the DataFrame
        pruned_posts_df["split_tags"] = pruned_posts_df[
            "tag_string_firstprune"
        ].str.split(", ")

        # Remove bad tags from the split_tags column
        pruned_posts_df["split_tags"] = pruned_posts_df["split_tags"].apply(
            lambda tags: [tag for tag in tags if tag not in bad_tags_from_new_tags]
        )

        # Rejoin the cleaned tags
        pruned_posts_df["tag_string_firstprune"] = pruned_posts_df["split_tags"].apply(
            lambda tags: ", ".join(tags)
        )

        # Drop the temporary split_tags column
        completed_pruned_posts_df = pruned_posts_df.drop(columns=["split_tags"])

        return completed_pruned_posts_df, task_number

    def remove_rows_with_special_tags(self, pruned_posts_df):
        task_number = 8
        # Read the tags from the JSON file
        with open("json/bad_tags.json", "r") as f:
            all_tags = json.load(f)

        logging.info(f"Number of rows before filtering: {len(pruned_posts_df)}")

        # Fetch tags with keep = 's'
        special_tags = [tag["name"] for tag in all_tags if tag["keep"] == "s"]

        # Filter out rows containing special tags
        completed_pruned_posts_df = pruned_posts_df[
            ~pruned_posts_df["tag_string_firstprune"].str.contains(
                "|".join(special_tags), regex=True
            )
        ]

        logging.info(
            f"Number of rows after filtering: {len(completed_pruned_posts_df)}"
        )

        return completed_pruned_posts_df, task_number

    @staticmethod
    def is_aspect_ratio_or_year_tag(tag):
        aspect_ratio_pattern = re.compile(r"^\d+:\d+$")
        year_pattern = re.compile(r"^\d{4}$")
        return aspect_ratio_pattern.match(tag) or year_pattern.match(tag)

    def remove_aspect_ratio_and_year_tags(self, pruned_posts_df):
        task_number = 9

        # Define patterns for aspect ratio and year tags
        aspect_ratio_pattern = re.compile(r"^\d+:\d+$")
        year_pattern = re.compile(r"^\d{4}$")
        patterns_to_remove = [aspect_ratio_pattern, year_pattern]

        def remove_tags(tag_string):
            tag_list = tag_string.split(", ")
            new_tag_list = []

            for tag in tag_list:
                if not any(pattern.match(tag) for pattern in patterns_to_remove):
                    new_tag_list.append(tag)

            return ", ".join(new_tag_list)

        completed_pruned_posts_df = pruned_posts_df.copy()
        completed_pruned_posts_df["tag_string_firstprune"] = pruned_posts_df[
            "tag_string_firstprune"
        ].apply(remove_tags)

        return completed_pruned_posts_df, task_number

    def remove_low_frequency_tags(self, pruned_posts_df):
        task_number = 10

        # Read low_tags DataFrame
        low_tags_df = pd.read_parquet("low_tags.parquet")
        low_frequency_tags = low_tags_df["name_firstprune"].tolist()

        # Create a set of low-frequency tags for faster lookup
        low_frequency_tags_set = set(low_frequency_tags)

        # Define a function to remove low-frequency tags from a tag string
        def remove_tags_from_string(tag_string):
            tags = tag_string.split(", ")
            cleaned_tags = [tag for tag in tags if tag not in low_frequency_tags_set]
            return ", ".join(cleaned_tags)

        # Remove low-frequency tags from the tag_string_firstprune column
        completed_pruned_posts_df = pruned_posts_df.copy()
        completed_pruned_posts_df["tag_string_firstprune"] = pruned_posts_df[
            "tag_string_firstprune"
        ].apply(remove_tags_from_string)

        return completed_pruned_posts_df, task_number

    # TODO: Redo this entire function

    def is_gender_post(self, tag_string, gender):
        tags = set(tag_string.split(", "))
        target_tags = {
            "male": ["andromorph", "maleherm", "gynomorph", "herm", "ambiguous gender"],
            "female": [
                "andromorph",
                "maleherm",
                "gynomorph",
                "herm",
                "ambiguous gender",
            ],
        }

        return gender in tags and not any(tag in tags for tag in target_tags[gender])

    def balance_male_and_female(self, pruned_posts_df):
        task_number = 11

        male_posts = pruned_posts_df[
            pruned_posts_df["tag_string_firstprune"].apply(
                lambda x: self.is_gender_post(x, "male")
            )
        ]
        female_posts = pruned_posts_df[
            pruned_posts_df["tag_string_firstprune"].apply(
                lambda x: self.is_gender_post(x, "female")
            )
        ]

        male_count = len(male_posts)
        female_count = len(female_posts)

        if male_count > female_count:
            male_posts = male_posts.sample(
                frac=female_count / male_count, random_state=42
            )
        else:
            female_posts = female_posts.sample(
                frac=male_count / female_count, random_state=42
            )

        balanced_posts = pd.concat([male_posts, female_posts])
        completed_pruned_posts_df = balanced_posts

        return completed_pruned_posts_df, task_number
