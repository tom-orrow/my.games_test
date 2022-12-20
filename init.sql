DROP TABLE IF EXISTS contents CASCADE;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS content_genre;
DROP TABLE IF EXISTS titles;
DROP TABLE IF EXISTS persons CASCADE;
DROP TABLE IF EXISTS person_profession;
DROP TABLE IF EXISTS person_content;
DROP TABLE IF EXISTS professions;

CREATE TABLE IF NOT EXISTS contents (
    content_id serial primary key,
    tconst varchar(128) not null,
    type varchar(128) not null,
    primary_title text not null,
    original_title text,
    runtime_minutes int,
    average_rating float,
    num_votes int not null default 0
);

CREATE UNIQUE INDEX contents_tconst_idx ON contents(tconst);

CREATE TABLE IF NOT EXISTS genres (
    genre_id serial primary key,
    name varchar(128) not null
);

CREATE UNIQUE INDEX genres_name_idx ON genres(name);

CREATE TABLE IF NOT EXISTS content_genre (
    content_id int not null,
    genre_id int not null,
    CONSTRAINT content_genre_pk PRIMARY KEY (content_id, genre_id)
);

CREATE TABLE IF NOT EXISTS titles (
    title_id serial primary key,
    name text not null,
    type varchar(128),
    region varchar(10),
    language varchar(10),
    attributes text,
    is_original_title boolean not null default False,
    content_id int not null,
    CONSTRAINT content_fk FOREIGN KEY(content_id) REFERENCES contents(content_id)
);

CREATE UNIQUE INDEX titles_combined_idx ON titles(content_id, name, coalesce(type, 'None'), coalesce(region, 'None'), coalesce(language, 'None'));

CREATE TABLE IF NOT EXISTS professions (
    profession_id serial primary key,
    name varchar(128) not null
);

CREATE UNIQUE INDEX professions_name_idx ON professions(name);

CREATE TABLE IF NOT EXISTS persons (
    person_id serial primary key,
    nconst varchar(128) not null,
    name varchar(128) not null,
    birth_year int,
    death_year int
);

CREATE UNIQUE INDEX persons_nconst_idx ON persons(nconst);

CREATE TABLE IF NOT EXISTS person_profession (
    person_id int not null,
    profession_id int not null,
    CONSTRAINT person_profession_pk PRIMARY KEY (person_id, profession_id)
);

CREATE TABLE IF NOT EXISTS person_content (
    person_id int not null,
    content_id int not null,
    person_profession_id int not null,
    person_job text,
    characters varchar[],
    is_known_for boolean not null default False,
    CONSTRAINT person_content_pk PRIMARY KEY (person_id, content_id)
);

CREATE INDEX person_content_idx ON person_content(content_id, person_id);