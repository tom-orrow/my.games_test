import psycopg2
import os

from psycopg2.extras import DictCursor
from typing import List


class DB(object):
    def __init__(self):
        if not hasattr(self, "conn"):
            self.conn = psycopg2.connect(os.environ["DATABASE_URL"])
            self.cur = self.conn.cursor(cursor_factory=DictCursor)

    def execute_values(
        self, sql: str, values: List[tuple], template: str, page_size=10_000
    ):
        psycopg2.extras.execute_values(
            self.cur, sql, values, template=template, page_size=page_size
        )

    def get_top_worst_actors(self):
        self.cur.execute(
            """
            select
                p.name,
                avg(c.average_rating) as avg_rating
            from professions pf
            join person_content pc on pc.person_profession_id = pf.profession_id
            join persons p using (person_id)
            join contents c using (content_id)
            where
                pf.name in ('actor', 'actress')
            group by p.person_id
            order by avg(c.average_rating) asc 
            limit 10
            """
        )

        return dict(self.cur.fetchall())

    def get_top_producers_by_number_of_movies(self):
        self.cur.execute(
            """
            select
                p.name,
                count(*) as number_of_movies
            from professions pf
            join person_content pc on pc.person_profession_id = pf.profession_id
            join persons p using (person_id)
            join contents c using (content_id)
            where
                pf.name = 'director'
                and c.type = 'movie'
            group by p.person_id
            order by count(*) desc
            limit 10
            """
        )

        return dict(self.cur.fetchall())

    def get_most_popular_genres(self):
        self.cur.execute(
            """
            select
                g.name,
                count(*)
            from contents c 
            join content_genre cg using(content_id)
            join genres g using (genre_id)
            group by g.genre_id
            order by count(*) desc
            """
        )

        return dict(self.cur.fetchall())

    def get_movies_top250(self):
        self.cur.execute(
            """
            select
                primary_title,
                average_rating
            from contents
            where
                num_votes > 50000
                and type = 'movie'
            order by average_rating desc
            limit 250
            """
        )

        return dict(self.cur.fetchall())
