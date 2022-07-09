import sched
import schedule
import time

def aprint(name):
    print('Hello', name)

if __name__ == '__main__':
    schedule.every().day.at('00:11:00').do(lambda: aprint('The Anh'))

    while True:
        schedule.run_pending()
        time.sleep(1)