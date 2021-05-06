from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class DataBase:

    def __init__(self, user=None, password=None, host=None, port=None, db=None):
        self.conn = connect(
            user=user or 'postgres',
            password=password or '1111',
            host=host or 'localhost',
            port=port or "5432",
            database=db or "database"
        )
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.conn.cursor()

    def write_to_database(self, vacancy_id, salary_from,
                          salary_to, curr, areas, url,
                          description, company, title,
                          job_format, date_posted):
        with self.conn:
            self.create_table()
            self.cursor.execute("""INSERT INTO vacancies(
            vacancy_id, salary_from, salary_to, curr, areas, url, description, company, title, format, date)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(url) DO NOTHING""",
                                (vacancy_id, salary_from, salary_to,
                                 curr, areas, url, description, company,
                                 title, job_format, date_posted)
                                )

    def create_table(self):
        with self.conn:
            self.cursor.execute("""CREATE TABLE IF NOT EXISTS vacancies (
                                id SERIAL PRIMARY KEY,
                                vacancy_id INT,
                                salary_from INT,
                                salary_to INT,
                                curr TEXT,
                                areas TEXT,
                                url TEXT UNIQUE,
                                description TEXT,
                                company TEXT,
                                title TEXT,
                                format TEXT,
                                date TIMESTAMP);"""
                                )
