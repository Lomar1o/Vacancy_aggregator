import asyncio

from celery.schedules import crontab

from vacancy_parser.parseHH import ParserHH
from vacancy_parser.parseRR import ParserRR
from vacancy_parser.parseSJ import ParserSJ
from task._celery import app


period = 60


@app.task
def parse_hh(period):
    loop = asyncio.get_event_loop()
    # hh = ParserHH(minutes=time_between_parse).start_parse()
    hh = ParserHH(days=period).start_parse()
    loop.run_until_complete(hh)


@app.task
def parse_sj(period):
    loop = asyncio.get_event_loop()
    # sj = ParserSJ(minutes=time_between_parse).start_parse()
    sj = ParserSJ(days=period).start_parse()
    loop.run_until_complete(sj)


@app.task
def parse_rr(period):
    loop = asyncio.get_event_loop()
    rr = ParserRR().start_parse()
    loop.run_until_complete(rr)


app.conf.beat_schedule = {
    'scrapping-_hh': {
        'task': 'task.tasks.parse_hh',
        'schedule': crontab(minute=f'*/{period}')
    },
    'scrapping-_sj': {
        'task': 'task.tasks.parse_sj',
        'schedule': crontab(minute=f'*/{period}')
    },
    'scrapping-_rr': {
        'task': 'task.tasks.parse_rr',
        'schedule': crontab(minute=f'*/{period}')
    },
}
