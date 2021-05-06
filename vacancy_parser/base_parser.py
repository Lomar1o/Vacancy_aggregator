import time
from datetime import datetime, timedelta

from functools import wraps
from abc import ABC, abstractmethod

from db.database import DataBase
from logger import write_logs


def duration(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        """
        Декоратор для измерения времени работы парсера.
        """
        start = time.time()
        result = func(*args, **kwargs)
        duration_program = time.time() - start
        print(f'Время работы составило {duration_program}')
        return result

    return wrapper


class Parser(ABC):
    """
    Абстрактный, базовый класс для парсинга вакансий.

    Принимает:
    количество дней либо часов, либо минут, за которые необходимо найти вакансии,
    логгер, куда записываются логи и ошибки,
    базу данных, куда пишется вся информация о вакансиях.

    Назначение:
    определить набор методов, который должны быть у дочерних классов.
    """
    search_interval = 30

    def __init__(self, days=None, hours=None, minutes=None, logger=None, db=None):
        self.logger = logger or write_logs(self.__class__)
        self.db = db or DataBase()
        self.time_from = datetime.now() - timedelta(minutes=self.search_interval)
        self.time_to = datetime.now()
        self.time_end = datetime.now() - timedelta(
            days=days or 0, hours=hours or 0, minutes=minutes or 0
        )

    @abstractmethod
    def start_parse(self):
        """
        Запускает работу парсера.
        """
        pass

    @abstractmethod
    def get_vacancy_id(self, vacancy):
        """
        Обрабатывает входящие данные и возвращает id вакансии.
        """
        pass

    @abstractmethod
    def get_salary(self, vacancy):
        """
        Обрабатывает входящие данные и возвращает зарплату (от, до и валюту).
        """
        pass

    @abstractmethod
    def get_vacancy_url(self, vacancy):
        """
        Обрабатывает входящие данные и возвращает url вакансии.
        """
        pass

    @abstractmethod
    def get_description(self, vacancy):
        """
        Обрабатывает входящие данные и возвращает краткое описание вакансии.
        """
        pass

    @abstractmethod
    def get_company_name(self, vacancy):
        """
        Обрабатывает входящие данные и возвращает наименование работодателя.
        """
        pass

    @abstractmethod
    def get_title(self, vacancy):
        """
        Обрабатывает входящие данные и возвращает название вакансии.
        """
        pass

    @abstractmethod
    def get_vacancy_format(self, vacancy):
        """
        Обрабатывает входящие данные и возвращает формат работы.
        """
        pass

    @abstractmethod
    def get_city_vacancy(self, vacancy):
        """
        Обрабатывает входящие данные и возвращает город, в которой расположена вакансия.
        """
        pass

    @abstractmethod
    def get_date_vacancy(self, vacancy):
        """
        Обрабатывает входящие данные и возвращает дату размещения вакансии.
        """
        pass
