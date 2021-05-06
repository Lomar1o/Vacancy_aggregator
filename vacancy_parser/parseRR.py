import asyncio
import aiohttp

from lxml import etree
from datetime import datetime

from vacancy_parser.base_parser import duration, Parser


class ParserRR(Parser):
    """
    Класс для парсинга вакансий с сайта https://www.rabota.ru.

    Реализован в асинхронном режиме.

    start_url - стартовая страница для начала парсинга.
    Установлено ограничение на количество подключений, из-за ограничений сервиса.
    """

    start_url = 'https://www.rabota.ru/'
    sem = asyncio.Semaphore(24)  # ограничение количества одновременных подключений.

    async def get_response(self, page):
        """
        Принимает:
        номер страницы.

        Назначение:
        сделать запрос по url и вернуть результат запроса.

        Выполняется запрос по указанному url (self.start_url),
        обработка происходит через библиотеку lxml.
        Обрабатываются ошибки и записываются в файл логгирования.

        Возвращает:
        DOM-дерево.
        """
        params = {'page': page}
        try:
            async with self.sem:
                async with self.session.get(self.start_url, params=params) as response:  # создаем запрос
                    text = await response.text()  # читает полученный результат
                    parser = etree.HTML(text)  # парсит в вид HTML
                    if page == 1:
                        self.city = parser.find('.//svg[@class="icon md-icon md-r-location"]').getnext().text
                    return parser
        except (aiohttp.ServerDisconnectedError,
                aiohttp.ContentTypeError,
                asyncio.TimeoutError,
                aiohttp.ClientPayloadError,
                aiohttp.ClientOSError,
                aiohttp.ClientConnectorError,
                ConnectionAbortedError) as e:
            self.logger.error(e)
        except Exception as e:
            self.logger.error("uncaught exception: %s", e)

    async def get_number_pages(self):
        """
        Назначение:
        Получить общее количество страниц с вакансиями.

        Делает запрос страницы 1 для получения DOM-дерева.
        По заданному тегу находит количество страниц.
        Итерируется, генерируя и созадвая новый таск
        с номером страницы добавляя в список.
        Запускаем одновременно все таски функцией gather()
        """
        parser = await self.get_response(page=1)
        # Если возвращается пустой результат, то пропускает данную страницу.
        if parser is None:
            return
        test = []
        # По имени класса находит номера страниц, выбирает из списка последний элемент - номер последний страницы.
        number_pages = [par.text.strip() for par in parser.findall('.//li//a[@class="pagination-list__item"]')][-1]
        # Итерирация со 2 страницы до последней, создавая новый таск.
        for page in range(2, int(number_pages) + 1):
            test.append(asyncio.create_task(self.get_data(page=page)))
        await asyncio.gather(*test)  # запускает все таски

    async def get_data(self, page):
        """
        Принимает:
        номер страницы.

        Назначение:
        выполнить запрос и получить информацию о вакансиях.

        Выполняет запрос и получает данные для дальнейшей работы с DOM-деревом.
        Если результат не пустой, происходит итерация по результатам запроса.
        Со страницы получаем:
        id вакансии,
        размер заработной платы (от, до и валюту зарплаты),
        url данной вакансии,
        краткое описание вакансии,
        наименование компании, разместившей вакансию,
        наименование вакансии,
        формат работы (удаленка),
        город, где предполагается работа,
        дата размещения вакансии.

        Далее происходит запись полученной информации в базу данных.
        """
        parser = await self.get_response(page)  # Создается запрос страницы
        if parser is not None:
            vacancies = parser.findall('.//div[@class="vacancy-preview-card__top"]')
            for vacancy in vacancies:
                vacancy_id = self.get_vacancy_id(vacancy)
                salary_from, salary_to, curr = \
                    self.get_salary(vacancy.find('.//div[@class="vacancy-preview-card'
                                                 '__salary vacancy-preview-card__salary-blue"]//a//span').text)
                vacancy_url = self.get_vacancy_url(vacancy)
                description = self.get_description(vacancy)
                company = self.get_company_name(vacancy)
                title = self.get_title(vacancy)
                job_format = self.get_vacancy_format(vacancy)
                areas = self.city
                date_posted = self.get_date_vacancy(vacancy)
                self.db.write_to_database(vacancy_id, salary_from, salary_to, curr, areas, vacancy_url,
                                          description,
                                          company, title, job_format, date_posted)
        else:
            self.logger.error(f'Not parsing {page}')

    def get_vacancy_id(self, vacancy):
        parent = vacancy.getparent()
        return int(parent.attrib['data-key'].split(':')[0])

    def get_salary(self, vacancy):
        if 'договорная зарплата' in vacancy:
            return 0, 0, 'RUB'
        salary = vacancy.replace('\xa0', ' ')
        salary_split = salary.split('—')
        if len(salary_split) == 2:
            salary_from = int(salary_split[0].replace(' ', ''))
            salary_split = salary_split[1].split()
            salary_to = int(salary_split[0] + salary_split[1])
        else:
            if 'от' in str(salary):
                salary = salary.replace('руб.', '').split('от')
                salary_from = int(salary[1].replace(' ', ''))
                salary_to = 0
            elif 'до' in str(salary):
                salary = salary.replace('руб.', '').split('до')
                salary_to = int(salary[1].replace(' ', ''))
                salary_from = 0
            else:
                salary_to = salary.replace('руб.', '').replace(' ', '')
                salary_from = 0
        curr = 'RUB'
        return salary_from, salary_to, curr

    def get_vacancy_url(self, vacancy):
        url = 'https://www.rabota.ru'
        vacancy_url = url + vacancy.find('.//h3[@class="vacancy-preview-card__title"]//a').attrib['href']
        return vacancy_url

    def get_description(self, vacancy):
        try:
            return vacancy.find('.//div[@class="vacancy-preview-card__short-description"]').text
        except AttributeError:
            return None

    def get_company_name(self, vacancy):
        try:
            return vacancy.find('.//span[@class="vacancy-preview-card__company-name"]//a').text
        except AttributeError:
            return None

    def get_title(self, vacancy):
        return vacancy.find('.//h3[@class="vacancy-preview-card__title"]//a').text

    def get_vacancy_format(self, vacancy):
        return None

    def get_city_vacancy(self, vacancy):
        return vacancy.find('.//svg[@class="icon md-icon md-r-location"]').getnext().text

    def get_date_vacancy(self, vacancy):
        vacancy_date = vacancy.find('.//meta[@itemprop="datePosted"]').attrib['content']
        date_strip = vacancy_date.split('.')
        format_date = datetime.strptime(date_strip[0], '%Y-%m-%dT%H:%M:%S')
        return format_date

    async def start_parse(self):
        tasks = []
        async with aiohttp.ClientSession() as self.session:
            tasks.append(asyncio.create_task(self.get_number_pages()))
            await asyncio.gather(*tasks)


@duration
def get_parse_rr():
    loop = asyncio.get_event_loop()
    parser = ParserRR().start_parse()
    loop.run_until_complete(parser)


if __name__ == '__main__':
    get_parse_rr()
