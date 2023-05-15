import pandas as pd
import sqlite3
import ast


class DatabasePruneDep:
    def remove_artist_tags_under_10_posts(tags_df):
        print(tags_df.columns)
        tags_df["category"] = tags_df["category"].astype(int)

        # Filter out artists with post_count < 10
        pruned_artists_df = tags_df[
            (tags_df["category"] == 1) & (tags_df["post_count"] < 10)
        ]

        # Save the pruned_artists DataFrame to a new Parquet file
        pruned_artists_df.to_parquet("pruned_artists.parquet", compression="snappy")

        # Filter artists with post_count >= 10
        usable_artists_df = tags_df[
            (tags_df["category"] == 1) & (tags_df["post_count"] >= 10)
        ]

        # Save the usable_artists DataFrame to a new Parquet file
        usable_artists_df.to_parquet("usable_artists.parquet", compression="snappy")

    def remove_low_frequency_tags(tags_df):
        # Filter out artists with post_count < 10
        low_tags = tags_df[(tags_df["post_count"] < 50)]

        # Save the pruned_artists DataFrame to a new Parquet file
        low_tags.to_parquet("low_tags.parquet", compression="snappy")

        # Filter artists with post_count >= 10
        ok_tags = tags_df[(tags_df["post_count"] >= 50)]

        # Save the usable_artists DataFrame to a new Parquet file
        ok_tags.to_parquet("ok_tags.parquet", compression="snappy")

    def create_tag_index(pruned_posts_df, tags_df):
        # Create a dictionary to store the index for each tag
        tag_index = {}

        # Iterate through each row of the dataset using itertuples()
        for row in pruned_posts_df.itertuples():
            id = row.id
            tags = row.tag_string_firstprune.split(", ")

            # Add id to the list of each tag in the dictionary
            for tag in tags:
                if tag in tag_index:
                    tag_index[tag].append(id)
                else:
                    tag_index[tag] = [id]

        # Convert the lists of IDs to comma-separated strings (without spaces)
        for tag in tag_index:
            tag_index[tag] = ",".join(map(str, tag_index[tag]))

        # Convert the tag_index dictionary to a DataFrame
        tag_index_df = pd.DataFrame(list(tag_index.items()), columns=["tag", "id"])

        # Save the DataFrame as a parquet file and CSV file
        tag_index_df.to_parquet("tag_index.parquet", index=False)
        tag_index_df.to_csv("tag_index.csv", index=False)

    def create_implication_tag_index():
        tag_index = pd.read_parquet("tag_index.parquet")
        implications = pd.read_parquet("implication.parquet")

        # Convert the string of IDs in tag_index to list of integers
        tag_post_ids = {
            row["tag"]: list(map(int, row["id"].split(",")))
            for _, row in tag_index.iterrows()
        }

        # Define the function to extract and compare the post_ids
        def extract_post_ids(row):
            antecedent_ids = set(tag_post_ids.get(row["antecedent_name_pruned"], []))
            consequent_ids = set(tag_post_ids.get(row["consequent_name_pruned"], []))

            # Debug lines
            # print(f"Implication: {row['id']}")
            # print(f"Antecedent name: {row['antecedent_name_pruned']}")
            # print(f"Antecedent ids from tag_post_ids: {antecedent_ids}")
            # print(f"Consequent name: {row['consequent_name_pruned']}")
            # print(f"Consequent ids from tag_post_ids: {consequent_ids}")

            result = list(antecedent_ids - consequent_ids)
            # if not result:  # if result is an empty list
            #     print(f"No post ids for implication: {row['id']}")

            return result

        # Apply the function to each row in the implications DataFrame
        implications["post_ids"] = implications.apply(extract_post_ids, axis=1)

        # Select only the columns you need
        implications = implications[
            ["id", "antecedent_name_pruned", "consequent_name_pruned", "post_ids"]
        ]

        # Convert post_ids back to string for SQL compatibility
        implications["post_ids"] = implications["post_ids"].apply(
            lambda x: ",".join(map(str, x))
        )

        # Save the DataFrame to parquet
        # Save the final DataFrame as an SQLite database
        conn = sqlite3.connect("implication_tag_index.sqlite")
        implications.to_sql(
            "implication_tag_index", conn, if_exists="replace", index=False
        )
        conn.close()

        implications.to_parquet("implication_index.parquet")
