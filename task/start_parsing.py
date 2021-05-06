from task.tasks import parse_hh, parse_sj, parse_rr


def main1():
    parse_hh.delay()
    parse_sj.delay()
    parse_rr.delay()


if __name__ == '__main__':
    main1()
