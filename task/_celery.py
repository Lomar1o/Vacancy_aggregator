from celery import Celery


app = Celery('task', include=['task.tasks'])


if __name__ == '__main__':
    app.start()
