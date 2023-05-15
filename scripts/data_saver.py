import sqlite3
import pandas as pd
import os


class DataSaver:
    def __init__(self, pruned_posts_df: pd.DataFrame):
        self.pruned_posts_df = pruned_posts_df

    def save_pruned_posts(self):
        self.save_to_parquet()
        self.save_to_sqlite()

    def save_to_parquet(self):
        self.pruned_posts_df.to_parquet(
            "final_pruned_posts.parquet",
            compression="snappy",
        )

    def save_to_sqlite(self):
        with sqlite3.connect("final_pruned_posts.db") as conn:
            self.pruned_posts_df.to_sql("posts", conn, if_exists="replace", index=False)
