from datetime import datetime
import time
from scraping import scrape
from psql import insert_data
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


if __name__ == '__main__':
    schedule.every().hour.at(':00').do(main)
    schedule.every().hour.at(':20').do(main)
    schedule.every().hour.at(':40').do(main)

    # поллинг каждые n секунд
    n = 10
    while 1:
        # проверить, не настало ли время
        schedule.run_pending()

        nxt = schedule.next_run()
        now = datetime.now()
        dif = nxt - now
        print('\rслед действие через', int(dif.total_seconds()), 'сек', end='')

        time.sleep(n)

# pip install environs schedule curl_cffi bs4 telebot psycopg2