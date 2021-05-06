import argparse

from task.tasks import parse_hh, parse_sj, parse_rr


def start_parse_vacancy(period):
    parse_hh(period)
    parse_sj(period)
    parse_rr(period)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-parse', action='store_const', const=True)
    parser.add_argument('-period', type=int)
    args = parser.parse_args()

    if args.parse:
        print('parse start')
        start_parse_vacancy(args.period)
