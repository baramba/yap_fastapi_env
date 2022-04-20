import os
import uuid
import json
import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

import logging

from dotenv import load_dotenv
from typing import List, Optional

from pydantic import BaseModel
from datetime import datetime


# Logger initialization
logging.basicConfig(format='[%(levelname)s]: %(message)s', level=logging.DEBUG)

class Film_work(BaseModel):
    id: uuid.UUID
    title: str
    description: Optional[str]
    creation_date: Optional[datetime]
    file_path: Optional[str]
    rating: Optional[float]
    type: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class Genre(BaseModel):
    id: uuid.UUID
    name: str
    description: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class Person(BaseModel):
    id: uuid.UUID
    full_name: str
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class Genre_film_work(BaseModel):
    id: uuid.UUID
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created_at: Optional[datetime]


class Person_film_work(BaseModel):
    id: uuid.UUID
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: str
    created_at: Optional[datetime]


class SQLiteLoader:
    def __init__(self, connect: sqlite3.Connection):
        logging.info('Connect to SQLite..')
        self.connect = connect
        self.connect.row_factory = SQLiteLoader.dict_factory
        self.cursor = self.connect.cursor()


    def dict_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d


    def get_movies(self) -> List[dict]:
        logging.info('Loading film_works..')
        sql = 'SELECT * FROM film_work'
        self.cursor.execute(sql)
        return self.cursor.fetchall()


    def get_persons(self) -> List[dict]:
        logging.info('Loading persons..')
        sql = 'SELECT * FROM person'
        self.cursor.execute(sql)
        return self.cursor.fetchall()


    def get_genres(self) -> List[dict]:
        logging.info('Loading genres..')
        sql = 'SELECT * FROM genre'
        self.cursor.execute(sql)
        return self.cursor.fetchall()


    def get_person_film_works(self) -> List[dict]:
        logging.info('Loading person_film_works..')
        sql = 'SELECT * FROM person_film_work'
        self.cursor.execute(sql)
        return self.cursor.fetchall()


    def get_genre_film_works(self) -> List[dict]:
        logging.info('Loading genre_film_works..')
        sql = 'SELECT * FROM genre_film_work'
        self.cursor.execute(sql)
        return self.cursor.fetchall()


    def load_movies(self) -> dict:
        logging.info('Start load from SQLite..')
        data = {
            'film_work': [],
            'genre': [],
            'person': [],
            'film_work_genre': [],
            'film_work_person': []
        }

        data['film_work'] = [Film_work(**movie_row) for movie_row in self.get_movies()]
        data['genre'] = [Genre(**genre_row) for genre_row in self.get_genres()]
        data['person'] = [Person(**person_row) for person_row in self.get_persons()]
        data['film_work_genre'] = [Genre_film_work(**genre_film_work_row) for genre_film_work_row in self.get_genre_film_works()]
        data['film_work_person'] = [Person_film_work(**person_film_work_row) for person_film_work_row in self.get_person_film_works()]

        return data


class PostgresSaver:

    def __init__(self, connect):
        logging.info('Connect to postgres..')
        self.connect = connect
        self.cursor = self.connect.cursor()


    def save_all_data(self, data: dict):
        logging.info('Saving to postgres..')
        # Save to film_work
        film_insert_data = [
            (str(film.id), film.title, film.description, str(film.rating if film.rating is not None else 0.0), film.type, str(film.created_at), str(film.updated_at))
            for film in data['film_work']
        ]
        args = ','.join(self.cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s)", item).decode() for item in film_insert_data)
        self.cursor.execute(f'''
            INSERT INTO content.film_work (id, title, description, rating, type, created_at, updated_at)
            VALUES {args}
        ''')

        # Save to genre
        genre_insert_data = [(str(genre.id), genre.name, str(genre.created_at), str(genre.updated_at)) for genre in data['genre']]
        args = ','.join(self.cursor.mogrify("(%s, %s, %s, %s)", item).decode() for item in genre_insert_data)
        self.cursor.execute(f'''
            INSERT INTO content.genre (id, name, created_at, updated_at)
            VALUES {args}
        ''')

        # Save to person
        person_insert_data = [(str(person.id), person.full_name, str(person.created_at), str(person.updated_at)) for person in data['person']]
        args = ','.join(self.cursor.mogrify("(%s, %s, %s, %s)", item).decode() for item in person_insert_data)
        self.cursor.execute(f'''
            INSERT INTO content.person (id, full_name, created_at, updated_at)
            VALUES {args}
        ''')

        # Save to genre_film_work
        genre_film_work_insert_data = [
            (str(film_work_genre.id), str(film_work_genre.film_work_id), str(film_work_genre.genre_id), str(film_work_genre.created_at))
            for film_work_genre in data['film_work_genre']
        ]
        args = ','.join(
            self.cursor.mogrify("(%s, %s, %s, %s)", item).decode() for item in genre_film_work_insert_data
        )
        self.cursor.execute(f'''
            INSERT INTO content.genre_film_work (id, film_work_id, genre_id, created_at)
            VALUES {args}
        ''')

        # Save to person_film_work
        film_work_person_insert_data = [
            (
                str(film_work_person.id),
                str(film_work_person.film_work_id),
                str(film_work_person.person_id),
                film_work_person.role,
                str(film_work_person.created_at)
            )
            for film_work_person in data['film_work_person']
        ]
        args = ','.join(
            self.cursor.mogrify("(%s, %s, %s, %s, %s)", item).decode() for item in film_work_person_insert_data
        )
        self.cursor.execute(f'''
            INSERT INTO content.person_film_work (id, film_work_id, person_id, role, created_at)
            VALUES {args}
        ''')


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    '''Основной метод загрузки данных из SQLite в Postgres'''
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    data = sqlite_loader.load_movies()
    postgres_saver.save_all_data(data)


if __name__ == '__main__':
    load_dotenv()
    dsl = {
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'host': os.getenv('POSTGRES_HOST'),
        'port': os.getenv('POSTGRES_PORT')
    }
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
