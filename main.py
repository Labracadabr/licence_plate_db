

from datetime import datetime
import time
from scraping import scrape
from psql import insert_data, rm_dub, count_all
from sync_bot import post_msg
import schedule

def main():
    try:
        data = scrape()
        insert_data(data_list=data)

        if data:
            # сохранить первую найденную машину, чтобы в след раз на ней остановиться
            with open('stop.txt', 'w', encoding='utf-8') as f:
                stop_item = data[0]
                print(stop_item, file=f)
                print(f'{stop_item = }')

            post_msg(text=f'+ {len(data) if data else 0}')

    except Exception as e:
        post_msg(text=f'ОШИБКА:\n{e}')


def report():
    rm_dub()
    total = count_all()
    post_msg(text=str(total))


if __name__ == '__main__':
    # отчет каждый час
    schedule.every().hour.at(f':00').do(report())

    # поллинг каждые n секунд
    n = 10
    while 1:
        main()

        # проверить, не настало ли время
        schedule.run_pending()

        nxt = schedule.next_run()
        now = datetime.now()
        dif = nxt - now
        print('\rслед действие через', int(dif.total_seconds()), 'сек', end='')

        time.sleep(n)

# pip install environs schedule curl_cffi bs4 telebot psycopg2
