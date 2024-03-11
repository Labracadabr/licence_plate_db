from curl_cffi import requests
from bs4 import BeautifulSoup as Bs
import time
import random

base_url = 'https://platesmania.com/gallery'


def get_car_data(page_soup) -> dict:
    car_info = {}

    # модель авто
    car_name = page_soup.find('h4', class_='text-center').text.strip()
    car_info['model'] = car_name

    # ссылка на основное фото
    car_image_link = page_soup.find('img', class_='img-responsive')['src']
    car_info['image'] = car_image_link

    # дата поста
    upload_date = page_soup.find('small', class_='pull-right').text.strip()
    car_info['date'] = upload_date

    # опциональные детали
    car_details = page_soup.find('small')
    car_details = car_details.text.strip() if car_details else None
    car_info['details'] = None if car_details == upload_date else repr(car_details)

    # текст номера
    plate_text = page_soup.select('img.img-responsive')[-1]['alt']
    car_info['txt'] = plate_text

    # картинка номера
    plate_png = page_soup.select('img.img-responsive')[-1]['src']
    car_info['png'] = plate_png

    # tag - bus, cabriolet, etc
    tag = None
    tag = page_soup.find('i', class_='tooltips')
    if tag:
        tag = tag.get('data-original-title')
        if 'There are several license plates' in tag:
            tag = None
    car_info['tag'] = tag

    # страна номера
    country = page_soup.find('a', href=True).text.strip()
    car_info['loc'] = country

    # юзер
    user = page_soup.findAll('a', href=True)[-2]
    user_name = user.text.strip()
    uuid = user['href'].replace('/user', '')
    car_info['user'] = user_name
    car_info['uuid'] = uuid

    # car_data.append(car_info)
    print(f'{car_info = }')
    return car_info


def scrape() -> list:
    max_try = 5
    while True:
        try:
            # первая машина с прошлого парсинга - остановиться при нахождении такой
            with open('stop.txt', 'r', encoding='utf-8') as f:
                stop_item = f.read().strip()
                print(f'\n{stop_item = }')

            session = requests.Session()
            new_cars = []
            for i in range(0, 100):
                # запрос
                url = base_url if i == 0 else f'{base_url}-{i}'
                print(url, end='\n')
                while True:
                    resp = session.get(url, impersonate="chrome101")
                    if not resp.ok:
                        print(f'Error status_code {resp.status_code}, sleeping 10 sec')
                        max_try -= 1
                        if not max_try:
                            return None
                        time.sleep(10)
                    break

                # чтение html
                soup = Bs(resp.text, features='html.parser')

                # перебор до 10 машин с 1 страницы
                cards = soup.find_all('div', class_='col-sm-6')
                for j, card in enumerate(cards):
                    # все данные одной машины
                    try:
                        one_car = get_car_data(page_soup=card)
                    except Exception as e:
                        print('Error:', e)
                        continue

                    # если дошли до уже найденной машины
                    if stop_item and stop_item == repr(one_car):
                        print('\ndone, collected:', len(new_cars))
                        return new_cars
                    # сохранить новую машину
                    if one_car not in new_cars:
                        new_cars.append(one_car)

                print(len(new_cars), 'cars')

                # ждать минимум 0.6 сек, иначе code 429
                sec = random.randint(6, 10)/10
                print('sleep:', sec)
                time.sleep(sec)

            return new_cars

        except Exception as e:
            print('Scrapping Error:', e)
            max_try -= 1
            if not max_try:
                return None
            time.sleep(20)


if __name__ == '__main__':
    scrape()
