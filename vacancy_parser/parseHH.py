import asyncio
import aiohttp

from datetime import datetime, timedelta

from vacancy_parser.base_parser import Parser, duration


class ParserHH(Parser):
    """
    Класс для работы с API hh.ru.

    Принимает количество дней, часов или минут, за которые будет проводиться поиск вакансий.

    Из-за ограничения количества получаемых результатов в 2000,
    запросы отправляются с отрезком размещения вакансий 45 минут,
    что позволяет получить меньше максимального ограничения вакансий в ответе.

    Так же устанавливается ограничение на количество одновременных подключений.
    """
    sem = asyncio.Semaphore(70)
    url = 'https://api.hh.ru/vacancies'

    async def get_response(self, time_from=None, time_to=None, page=0):
        """
        Принимает:
        время, с которого производится поиск вакансий,
        время, по которое производится поиск вакансий,
        номер страницы, на которой производится поиск.

        Назначение:
        Сделать запрос к api и вернуть json ответ.

        Формирует параметры, которые будут переданы при запросе,
        совершает запрос и получает ответ в json формате.
        Обрабатывает ошибки и записывает их в файл логгирования.

        Возвращает:
        json ответ.
        """
        params = {
            'date_from': time_from.isoformat(),
            'date_to': time_to.isoformat(),
            'per_page': 100,
            'page': page,  # количество результатов на странице
            'area': 1  # id города
        }
        async with self.sem:
            try:
                async with self.session.get(self.url, params=params) as response:
                    return await response.json()
            except (aiohttp.ServerDisconnectedError,
                    aiohttp.ContentTypeError,
                    asyncio.TimeoutError,
                    aiohttp.ClientPayloadError,
                    aiohttp.ClientOSError,
                    aiohttp.ClientConnectorError,
                    ConnectionAbortedError) as err:
                self.logger.error(f'Not parsed {err}')

    async def get_number_pages(self, time_from, time_to):
        """
        Принимает:
        отрезок времени, в котором будет происходить поиск вакансий.

        Назначение:
        сделать запрос, получить количество страниц и создать новые таски.

        Делает запрос с отрезком времени, если ответ не пустой,
        высчитывает количество страниц с вакансиями.
        """
        resp = await self.get_response(time_from, time_to)
        if resp:
            try:
                if resp['found'] >= 2000:
                    self.logger.error(
                        f'More than 2000 vacancies from{time_from} to{time_to}, {resp["found"]}'
                    )
            except Exception as e:
                self.logger.error(e, resp)
            total_pages = resp['found'] // 100
            if total_pages == 0:
                total_pages = 1
            await asyncio.create_task(self.get_vacancies(time_from, time_to, total_pages))

    async def get_vacancies(self, time_from, time_to, total_pages):
        """
        Принимает:
        время, с которого начинается поиск вакансий,
        время, по которое происходит поиск вакансий,
        общее количество страниц с вакансиями.

        Назначение:
        сделать запрос, обработать результат запроса и записать в базу данных

        Сначала генерируются номера страниц от 0 до последней страницы (total_pages),
        затем происходит новый запрос с отрезком времени и номером страницы.
        В случае, если ответ не пустой, получаем следующие данные из ответа:
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
        for page in range(total_pages):
            resp = await self.get_response(time_from, time_to, page)
            if resp:
                for vacancy in resp['items']:
                    vacancy_id = self.get_vacancy_id(vacancy)  # id вакансии
                    salary_from, salary_to, curr = self.get_salary(vacancy['salary'])  # предлагаемая зарплата
                    url = self.get_vacancy_url(vacancy)  # url вакансии
                    description = self.get_description(vacancy)  # описание вакансии
                    company = self.get_company_name(vacancy)  # наименование компании, разместившей вакансию
                    title = self.get_title(vacancy)  # наименование вакансии
                    job_format = self.get_vacancy_format(vacancy)  # формат работы (удаленно, в офисе)
                    areas = self.get_city_vacancy(vacancy)  # город, в котором размещена вакансия
                    date_posted = self.get_date_vacancy(vacancy)  # дата размещения вакансии
                    # запись всей полученной информации в базу данных
                    self.db.write_to_database(vacancy_id, salary_from, salary_to, curr, areas, url, description,
                                              company, title, job_format, date_posted)

    def get_vacancy_id(self, vacancy):
        return vacancy['id']

    def get_salary(self, salary):
        """
        Принимает на вход зарплату в произвольном формате.

        Назначение: приведение зарплаты к необходимому формату, для записи в бд.

        Возвращает зарплату в виде кортежа значений (минимальная граница зарплаты,
        максимальная граница зарплаты и валюта, в которой указана зарплата).
        """
        try:
            salary_from = salary['from']
        except TypeError:
            salary_from = 0
        try:
            salary_to = salary['to']
        except TypeError:
            salary_to = 0
        try:
            curr = salary['currency']
        except TypeError:
            curr = 'RUB'
        return salary_from, salary_to, curr

    def get_vacancy_url(self, vacancy):
        return vacancy['alternate_url']

    def get_description(self, vacancy):
        return vacancy['snippet']['requirement']

    def get_company_name(self, vacancy):
        return vacancy['employer']['name']

    def get_title(self, vacancy):
        return vacancy['name']

    def get_vacancy_format(self, vacancy):
        return vacancy['schedule']['name']

    def get_date_vacancy(self, vacancy):
        """
        Получает на вход дату.

        Назначение: привести дату к необходимому формату для записи в бд.

        Возвращает дату в формате гг-мм-дд.
        """
        vacancy_date = vacancy['published_at']
        date_strip = vacancy_date.split('+')
        format_date = datetime.fromisoformat(date_strip[0])
        return format_date

    def get_city_vacancy(self, vacancy):
        return vacancy['area']['name']

    async def start_parse(self):
        tasks = []
        async with aiohttp.ClientSession() as self.session:
            while self.time_end < self.time_from:
                tasks.append(asyncio.create_task(self.get_number_pages(self.time_from, self.time_to)))
                self.time_from -= timedelta(minutes=self.search_interval)
                self.time_to -= timedelta(minutes=self.search_interval)
            await asyncio.gather(*tasks)


@duration
def get_parse_hh():
    loop = asyncio.get_event_loop()
    hh = ParserHH(days=1).start_parse()
    loop.run_until_complete(hh)


if __name__ == '__main__':
    get_parse_hh()
