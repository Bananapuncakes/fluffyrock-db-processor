import scripts.database_builder as db
import configparser

config = configparser.ConfigParser()
config.read("config.ini")

posts_csv_path = config["Paths"]["posts_csv_path"]
tags_csv_path = config["Paths"]["tags_csv_path"]
current_dataset = config["Paths"]["current_dataset"]
implication_csv_path = config["Paths"]["implication_csv_path"]


def build_db():
    database_builder = db.DatabaseBuilder(
        posts_csv_path, tags_csv_path, current_dataset, implication_csv_path
    )
    database_builder.build_database()
    database_builder.first_prune()


if __name__ == "__main__":
    build_db()
