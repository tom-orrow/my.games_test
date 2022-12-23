import pandas as pd
import os

from db import DB


def main():
    chunksize = 100_000
    with pd.read_csv(
        f'{os.environ["DATA_FOLDER"]}/complete_data.csv', chunksize=chunksize
    ) as reader:
        for i, chunk in enumerate(reader):
            prepared_data = prepare(chunk)
            save_to_db(prepared_data)
            print(f"Completed {(i+1) * chunksize} rows")

            return  # too long for 20mil rows


def prepare(chunk: pd.DataFrame) -> dict:
    # Связь будет идти по tconst, убираем строки без tconst
    chunk = chunk.loc[chunk.tconst.str.startswith("tt")].replace("\\N", None)

    res = {
        "contents": {},
        "content_genre": {},
        "genres": chunk[pd.notnull(chunk["genres"])]["genres"]
        .str.split(",")
        .explode("genres")
        .unique(),
        "persons": {},
        "person_content": {},
        "professions": set(),
        "titles": [],
        "person_profession": {},
    }

    lst = chunk.to_dict(orient="records")

    for row in lst:
        # prepare contents
        res["contents"][row["tconst"]] = {
            "tconst": row["tconst"],
            "type": row["titleType"],
            "primary_title": row["primaryTitle"],
            "original_title": row["originalTitle"],
            "runtime_minutes": row["runtimeMinutes"],
            "average_rating": row["averageRating"],
            "num_votes": row["numVotes"],
        }

        # prepare link between content and genres
        res["content_genre"].setdefault(row["tconst"], set())
        if row["genres"] is not None:
            res["content_genre"][row["tconst"]].update(row["genres"].split(","))

        # titles
        res["titles"].append(
            {
                "tconst": row["tconst"],
                "name": row["title"],
                "type": row["types"],
                "region": row["region"],
                "language": row["language"],
                "attributes": row["attributes"],
                "is_original_title": row["isOriginalTitle"] == 1,
            }
        )

        # persons
        res["persons"][row["nconst"]] = {
            "nconst": row["nconst"],
            "name": row["primaryName"],
            "birth_year": row["birthYear"],
            "death_year": row["deathYear"],
        }

        # prepare link between profession and person
        res["person_profession"].setdefault(row["nconst"], set())
        if isinstance(row["primaryProfession"], str):
            res["person_profession"][row["nconst"]].update(
                row["primaryProfession"].split(",")
            )

        # professions
        if isinstance(row["primaryProfession"], str):
            res["professions"].update(row["primaryProfession"].split(","))
        if row["category"] is not None:
            res["professions"].add(row["category"])

        # person_content
        res["person_content"][f"{row['tconst']}_{row['nconst']}"] = {
            "tconst": row["tconst"],
            "nconst": row["nconst"],
            "is_known_for": row["tconst"] in row["knownForTitles"].split(",")
            if row["knownForTitles"] is not None
            else False,
            "person_job": row["job"],
            "person_profession": row["category"],
            "characters": row["characters"][2:-2].split('","')
            if row["characters"] is not None
            else None,
        }

    return res


def save_to_db(data: dict):
    db = DB()

    # contents
    db.execute_values(
        """
        INSERT INTO contents (tconst, type, primary_title, original_title, runtime_minutes, average_rating, num_votes)
        VALUES %s
        ON CONFLICT DO NOTHING
    """,
        data["contents"].values(),
        template="(%(tconst)s, %(type)s, %(primary_title)s, %(original_title)s, %(runtime_minutes)s, %(average_rating)s, %(num_votes)s)",
    )

    db.cur.execute(
        "SELECT tconst, content_id FROM contents WHERE tconst = ANY(%s)",
        (list(data["contents"].keys()),),
    )
    tconst_to_id = dict(db.cur.fetchall())

    # genres
    db.execute_values(
        """
        INSERT INTO genres (name)
        VALUES %s
        ON CONFLICT DO NOTHING
    """,
        [(x,) for x in data["genres"]],
        template="(%s)",
    )

    db.cur.execute("SELECT name, genre_id FROM genres")
    genre_to_id = dict(db.cur.fetchall())

    # content_genres
    content_genres = set()
    for tconst, genres in data["content_genre"].items():
        for genre in genres:
            content_genres.add((tconst_to_id[tconst], genre_to_id[genre]))

    db.execute_values(
        """
        INSERT INTO content_genre (content_id, genre_id)
        VALUES %s
        ON CONFLICT DO NOTHING
    """,
        list(content_genres),
        template="(%s, %s)",
    )

    # titles
    for title in data["titles"]:
        title["content_id"] = tconst_to_id[title["tconst"]]

    db.execute_values(
        """
        INSERT INTO titles (name, type, region, language, attributes, is_original_title, content_id)
        VALUES %s
        ON CONFLICT DO NOTHING
    """,
        data["titles"],
        template="(%(name)s, %(type)s, %(region)s, %(language)s, %(attributes)s, %(is_original_title)s, %(content_id)s)",
    )

    # persons
    db.execute_values(
        """
        INSERT INTO persons (nconst, name, birth_year, death_year)
        VALUES %s
        ON CONFLICT DO NOTHING
    """,
        data["persons"].values(),
        template="(%(nconst)s, %(name)s, %(birth_year)s, %(death_year)s)",
    )

    db.cur.execute(
        "SELECT nconst, person_id FROM persons WHERE nconst = ANY(%s)",
        (list(data["persons"].keys()),),
    )
    nconst_to_id = dict(db.cur.fetchall())

    # professions
    db.execute_values(
        """
        INSERT INTO professions (name)
        VALUES %s
        ON CONFLICT DO NOTHING
    """,
        [(x,) for x in data["professions"]],
        template="(%s)",
    )

    db.cur.execute("SELECT name, profession_id FROM professions")
    profession_to_id = dict(db.cur.fetchall())

    # person_profession
    person_professions = {}
    for nconst, professions in data["person_profession"].items():
        for profession in professions:
            person_professions[f"{nconst}_{profession}"] = {
                "person_id": nconst_to_id[nconst],
                "profession_id": profession_to_id[profession],
            }

    db.execute_values(
        """
        INSERT INTO person_profession (person_id, profession_id)
        VALUES %s
        ON CONFLICT DO NOTHING
    """,
        person_professions.values(),
        template="(%(person_id)s, %(profession_id)s)",
    )

    # person_content
    person_content = []
    for person in data["person_content"].values():
        person_content.append(
            {
                "person_id": nconst_to_id[person["nconst"]],
                "content_id": tconst_to_id[person["tconst"]],
                "person_profession_id": profession_to_id[person["person_profession"]],
                "person_job": person["person_job"],
                "characters": person["characters"],
                "is_known_for": person["is_known_for"],
            }
        )

    db.execute_values(
        """
        INSERT INTO person_content (person_id, content_id, person_profession_id, person_job, characters, is_known_for)
        VALUES %s
        ON CONFLICT DO NOTHING
    """,
        person_content,
        template="(%(person_id)s, %(content_id)s, %(person_profession_id)s, %(person_job)s, %(characters)s, %(is_known_for)s)",
    )

    db.conn.commit()


if __name__ == "__main__":
    main()
