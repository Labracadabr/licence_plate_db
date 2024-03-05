import telebot
import time
from config import config

bot = telebot.TeleBot(config.BOT_TOKEN)  # тут токен
post_channel = '-1002130916129'


# простейший синхронный бот на одно действие
def post_msg(text) -> None:
    max_try = 5
    while True:
        try:
            bot.send_message(chat_id=post_channel, text=text)
            time.sleep(1)
            return
        # если слишком много запросов
        except Exception as e:
            print('\nTelegram Error', e)
            print('msg text:', text)
            max_try -= 1
            if not max_try:
                return
            time.sleep(20)
