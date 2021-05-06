import asyncio
import aiohttp

from random import uniform
from datetime import datetime, timedelta

import config
from vacancy_parser.base_parser import Parser, duration


class ParserSJ(Parser):
    """
    Класс для работы с API https://www.superjob.ru/

    Из-за ограничения количества получаемых результатов в 500,
    запросы отправляются с отрезком размещения вакансий 15 минут,
    что позволяет получить меньше максимального ограничения вакансий в ответе.

    Так же устанавливается ограничение на количество одновременных подключений.
    """

    __SECRET_KEY = config.SJ_KEY
    url = 'https://api.superjob.ru/2.0/vacancies/'
    sem = asyncio.Semaphore(100)
    search_interval = 15  # интервал поиска, в минутах

    async def get_response(self, time_from=None, time_to=None, page=0):
        """
        Принимает:
        время, с которого нужно начинать поиск вакансий,
        время, по которое нужно совершить поиск,
        номер страницы, на которой нужно производить поиск.

        Назначение:
        сделать запрос к API

        Возвращает:
        ответ в формате json

        Сначала формируются параметры, с которыми происходит запрос,
        в асинхронном режиме делает запрос, возвращает ответ.
        Обрабатывает ошибки, записывая их в файл логгирования.
        """
        params = {
            'date_published_from': time_from.timestamp(),
            'date_published_to': time_to.timestamp(),
            'count': 100,
            'page': page,  # количество результатов на странице
            'town': 4  # id города
        }
        headers = {'X-Api-App-Id': self.__SECRET_KEY}
        async with self.sem:
            await asyncio.sleep(uniform(10, 25))
            try:
                async with self.session.get(self.url, params=params, headers=headers) as response:
                    return await response.json()
            except (aiohttp.ServerDisconnectedError,
                    aiohttp.ContentTypeError,
                    asyncio.TimeoutError,
                    aiohttp.ClientPayloadError,
                    aiohttp.ClientOSError,
                    aiohttp.ClientConnectorError
                    ) as err:
                self.logger.error(f'Not parsed {err}')

    async def get_number_pages(self, time_from, time_to):
        """
        Принимает:
        время, с которого начинается поиск,
        время, по которое происходит поиск.

        Назначение:
        сделать запрос с отрезком времени поиска,
        если ответ не пустой, получает количество страниц
        с вакансиями в данном промежутке времени.
        Создает новый таск для нового запроса с отрезком времени
        и количеством страниц.
        """
        resp = await self.get_response(time_from, time_to)
        if resp:
            number_pages = resp['total'] // 100
            if number_pages == 0:
                number_pages = 1
            await asyncio.create_task(self.get_vacancies(time_from, time_to, number_pages))
        else:
            self.logger.error(f'Not found from {time_from} to {time_to}')

    async def get_vacancies(self, time_from, time_to, number_pages):
        """
        Принимает:
        время, с которого начинается поиск,
        время по которое происходит поиск,
        общее количество страниц с вакансиями в данный промежуток времени.

        Назначение:
        сделать запрос, обработать полученную информацию и записать в базу данных.

        Сначала генерируются номера страниц от 0 до number_pages,
        затем происходит запрос с промежутком времени и номером страницы.
        Если ответ не пустой, обрабатываем информацию, получая со страницы:
        id вакансии,
        размер заработной платы (от, до и валюту зарплаты),
        url данной вакансии,
        краткое описание вакансии,
        наименование компании, разместившей вакансию,
        наименование вакансии,
        формат работы (удаленка),
        город, где предполагается работа,
        дата размещения вакансии.
        Следом происходит запись полученной информации в базу данных.
        """
        for page in range(number_pages):
            resp = await self.get_response(time_from, time_to, page)
            if resp:
                for vacancy in resp['objects']:
                    vacancy_id = self.get_vacancy_id(vacancy)
                    salary_from, salary_to, curr = self.get_salary(vacancy)
                    url = self.get_vacancy_url(vacancy)
                    description = self.get_description(vacancy)
                    company = self.get_company_name(vacancy)
                    title = self.get_title(vacancy)
                    job_format = self.get_vacancy_format(vacancy)
                    areas = self.get_city_vacancy(vacancy)
                    date_published = self.get_date_vacancy(vacancy)
                    self.db.write_to_database(vacancy_id, salary_from, salary_to, curr, areas, url, description,
                                              company, title, job_format, date_published)
            else:
                self.logger.error(f'Not Found from {time_from} to {time_to}, page {page}')

    def get_vacancy_id(self, vacancy):
        return vacancy['id']

    def get_salary(self, vacancy):
        salary_from = vacancy['payment_from']
        salary_to = vacancy['payment_to']
        curr = vacancy['currency']
        return salary_from, salary_to, curr

    def get_vacancy_url(self, vacancy):
        return vacancy['link']

    def get_description(self, vacancy):
        return vacancy['candidat']

    def get_company_name(self, vacancy):
        return vacancy['firm_name']

    def get_title(self, vacancy):
        return vacancy['profession']

    def get_vacancy_format(self, vacancy):
        return vacancy['type_of_work']['title']

    def get_city_vacancy(self, vacancy):
        return vacancy['town']['title']

    def get_date_vacancy(self, vacancy):
        return datetime.fromtimestamp(vacancy['date_published'])

    async def start_parse(self):
        tasks = []
        async with aiohttp.ClientSession() as self.session:
            while self.time_end < self.time_from:
                tasks.append(asyncio.create_task(self.get_number_pages(self.time_from, self.time_to)))
                self.time_from -= timedelta(minutes=self.search_interval)
                self.time_to -= timedelta(minutes=self.search_interval)
            await asyncio.gather(*tasks)


@duration
def get_parse_sj():
    loop = asyncio.get_event_loop()
    hh = ParserSJ(days=1).start_parse()
    loop.run_until_complete(hh)


if __name__ == '__main__':
    get_parse_sj()
