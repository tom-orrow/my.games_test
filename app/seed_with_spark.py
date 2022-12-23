import os
import ast

from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)
from db import DB


data_schema = StructType(
    [
        StructField("ordering_x", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("region", StringType(), True),
        StructField("language", StringType(), True),
        StructField("types", StringType(), True),
        StructField("attributes", StringType(), True),
        StructField("is_original_title", BooleanType(), False),
        StructField("tconst", StringType(), False),
        StructField("title_type", StringType(), False),
        StructField("primary_title", StringType(), False),
        StructField("original_title", StringType(), False),
        StructField("runtime_minutes", IntegerType(), False),
        StructField("genres", StringType(), True),
        StructField("avg_rating", DoubleType(), False),
        StructField("num_votes", IntegerType(), False),
        StructField("directors", StringType(), True),
        StructField("writers", StringType(), True),
        StructField("ordering_y", IntegerType(), False),
        StructField("nconst", IntegerType(), False),
        StructField("category", StringType(), False),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True),
        StructField("primary_name", StringType(), False),
        StructField("birth_year", IntegerType(), False),
        StructField("death_year", IntegerType(), True),
        StructField("primary_professions", StringType(), False),
        StructField("known_for_titles", StringType(), False),
    ]
)


def get_most_popular_genres(spark: SparkSession) -> DataFrame:
    data_path = os.path.join(Path(__name__).parent, "./app/data", "complete_data.csv")
    print(data_path)

    df = (
        (
            spark.read.options(header=True)
            .schema(data_schema)
            .csv(data_path, nullValue="\\N")
        )
        .withColumn("types", f.split("types", ","))
        .withColumn("attributes", f.split("attributes", ","))
        .withColumn("genres", f.split("genres", ","))
        .withColumn("directors", f.split("directors", ","))
        .withColumn("writers", f.split("writers", ","))
        .withColumn(
            "characters",
            f.split(f.regexp_replace("characters", r"[\[\]\"]", ""), ","),
        )
        .withColumn("primary_professions", f.split("primary_professions", ","))
        .withColumn("known_for_titles", f.split("known_for_titles", ","))
    )

    exploded_genres_df = (
        df.where(f.col("genres").isNotNull())
        .withColumn("genres", f.explode("genres"))
        .groupBy("genres", "tconst")
        .agg(f.first("num_votes").alias("num_votes"))
        .select("tconst", f.col("genres").alias("genre"), "num_votes")
        .orderBy("tconst")
    )

    most_popular_genre_df = (
        exploded_genres_df.groupBy("genre")
        .agg(f.sum("num_votes").alias("num_votes"))
        .select("genre", "num_votes")
    )

    return most_popular_genre_df


def save_to_db(df: DataFrame):
    df.write.mode("overwrite").jdbc(
        url=f"jdbc:postgresql://db:5432/{os.environ['POSTGRES_DB']}",
        table="dm_most_popular_genres",
        properties={
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver",
        },
    )


def main(spark: SparkSession):
    datamart = get_most_popular_genres(spark)
    save_to_db(datamart)


if __name__ == "__main__":
    main(
        SparkSession.builder.config(
            "spark.jars.packages", "org.postgresql:postgresql:42.5.1"
        )
        .appName("Create datamart from data")
        .getOrCreate()
    )
