import scripts.database_post_pruner as dp
import scripts.database_scorer as ds
import scripts.database_pruner_dep as dpd
from scripts.data_saver import DataSaver
import logging
from rich.logging import RichHandler
from rich.progress import Progress
import os
import sqlite3
from multiprocessing import Process
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read("config.ini")

posts_csv_path = config["Paths"]["posts_csv_path"]
tags_csv_path = config["Paths"]["tags_csv_path"]
current_dataset = config["Paths"]["current_dataset"]
implication_csv_path = config["Paths"]["implication_csv_path"]

debug = False
in_frequency = 50

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)
# Change rich log level to debug
logging.getLogger("rich").setLevel("INFO")

logging = logging.getLogger("rich")


def save_to_database(task_number, task_name, df, conn):
    table_name = str(task_number) + "_" + str(task_name)
    df.to_sql(table_name, conn, if_exists="replace", index=False)


def create_debug_database(db_name):
    if not os.path.exists(db_name):
        # Create a connection to a new SQLite3 database file
        conn = sqlite3.connect(db_name)
        # Close the connection, effectively creating the database file
        conn.close()
        print(f"Created a new SQLite3 database: {db_name}")
    else:
        print(f"Database '{db_name}' already exists.")


def run_deps():
    tags_df = pd.read_parquet("tags.parquet")
    pruned_posts_df = pd.read_parquet("pruned_posts.parquet")
    logging.info("Creating dependencies...")
    # Run the dependency checker
    # pruner_dep = dpd.DatabasePruneDep()
    dpd.DatabasePruneDep.remove_artist_tags_under_10_posts(tags_df)
    dpd.DatabasePruneDep.remove_low_frequency_tags(tags_df)
    dpd.DatabasePruneDep.create_tag_index(pruned_posts_df, tags_df)
    dpd.DatabasePruneDep.create_implication_tag_index()
    # dpd.DatabasePruneDep.create_tag_tree()


def initalise_parquets():
    logging.info("Initialising parquets...")
    # Initalise the parquets using Pandas
    pruned_posts_df = pd.read_parquet("pruned_posts.parquet")
    tags_df = pd.read_parquet("tags.parquet")
    pruned_artists_df = pd.read_parquet("pruned_artists.parquet")
    implication_df = pd.read_parquet("implication.parquet")
    usable_artists_df = pd.read_parquet("usable_artists.parquet")
    return (
        pruned_posts_df,
        tags_df,
        pruned_artists_df,
        implication_df,
        usable_artists_df,
    )


if __name__ == "__main__":
    logging.info("Running checks...")
    # Create the debug database
    debug_db_name = "tasks_debug.db"
    run_deps()
    create_debug_database(debug_db_name)
    (
        pruned_posts_df,
        tags_df,
        pruned_artists_df,
        implication_df,
        usable_artists_df,
    ) = initalise_parquets()

    # Start the run
    dataset_pruner = dp.DatasetPruner()
    completed_pruned_posts_df = pruned_posts_df

    if debug:
        conn = sqlite3.connect("tasks_debug.db")

    tasks = [
        (dataset_pruner.forbidden_tags, "remove posts with forbidden tags"),
        # (dataset_pruner.add_implication_tags, "add implication tags"),
        (dataset_pruner.convert_accent_characters, "convert accent characters"),
        (dataset_pruner.strip_meme_tags, "remove posts with meme tags"),
        (dataset_pruner.append_e621_tag, "add e621 tag to posts"),
        # (dataset_pruner.create_tag_index, "create tag index", tags_df),
        (
            dataset_pruner.remove_artist_tags_under_10_posts,
            "remove artist tag for artists under 10 posts",
        ),
        (
            dataset_pruner.add_by_to_artist,
            "add by to artist tags",
            usable_artists_df,
        ),
        (dataset_pruner.strip_bad_tags, "strip bad tags"),
        (dataset_pruner.remove_rows_with_special_tags, "remove rows with special tags"),
        (
            dataset_pruner.remove_aspect_ratio_and_year_tags,
            "remove aspect ratio and year tags",
        ),
        (
            dataset_pruner.remove_low_frequency_tags,
            "remove low frequency tags",
        ),
        (dataset_pruner.balance_male_and_female, "balance male and female"),
    ]

    with Progress() as progress:
        task_bar = progress.add_task("[cyan]Processing...", total=len(tasks))

        for task, task_description, *args in tasks:
            logging.info(f"Currently processing: {task_description}")
            previous_row = completed_pruned_posts_df.loc[
                completed_pruned_posts_df["id"] == "1631"
            ].iloc[0]
            previous_tag_string_firstprune = previous_row["tag_string_firstprune"]
            completed_pruned_posts_df, task_number = task(
                completed_pruned_posts_df, *args
            )
            # Print a row from the completed_pruned_posts_df for debugging
            current_row = completed_pruned_posts_df.loc[
                completed_pruned_posts_df["id"] == "1631"
            ].iloc[0]
            logging.info("Task number: " + str(task_number))

            # Check the difference between the previous row and the current row for debugging
            previous_tag_string_firstprune = set(
                previous_row["tag_string_firstprune"].split()
            )
            current_tag_string_firstprune = set(
                current_row["tag_string_firstprune"].split()
            )

            if previous_tag_string_firstprune != current_tag_string_firstprune:
                removed_tags = (
                    previous_tag_string_firstprune - current_tag_string_firstprune
                )
                added_tags = (
                    current_tag_string_firstprune - previous_tag_string_firstprune
                )
                logging.info(f"Removed tags: {', '.join(removed_tags)}")
                logging.info(f"Added tags: {', '.join(added_tags)}")
            else:
                logging.info("No difference in tag_string_firstprune")

            # Save the iteration to the database if debug is True
            if debug:
                save_to_database(
                    task_number, task_description, completed_pruned_posts_df, conn
                )

            progress.update(task_bar, advance=1)

        if debug:
            conn.close()

    # Save the pruned posts using the DataSaver
    data_saver = DataSaver(completed_pruned_posts_df)
    data_saver.save_pruned_posts()
