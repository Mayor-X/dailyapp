import enum
import os
import time

import requests


class BotName(enum.Enum):
    CARGOBOARD = 'cargoboard'
    # EUROSENDER = 'eurosender'
    UPELA = 'upela'
    FREIGHTOS = 'freightos'
    EUROPACCO = 'europacco'
    JUMINGO = 'jumingo'
    QUICARGO = 'quicargo'
    GENTLOGISTIC = 'gentlogistics'


class BotAPI:
    def __init__(self):
        self.BASE_URL = 'https://konfidio-web-scrapers.herokuapp.com'
        self.API_KEY = os.getenv('BOT_API_KEY')
        assert self.API_KEY, 'env variable BOT_API_KEY must be set'

    def call(self, bot: BotName):
        bot_name = bot.value
        url1 = f'{self.BASE_URL}/quote/{bot_name}?key={self.API_KEY}'

        res1 = requests.post(url1)

        if res1.status_code == 200:
            req_id = res1.json()['id']
            print('request_id:', req_id)

            while True:
                url2 = f'{self.BASE_URL}/quote/{req_id}?key={self.API_KEY}'
                res2 = requests.get(url2)

                if res2.status_code == 200:
                    if res2.json()['status'] == 'failed':
                        raise Exception(f'bot {bot.value.upper()} is called but execution failed, {res2.text}')

                    elif res2.json()['status'] == 'pending':
                        print('wait 20 secs')
                        time.sleep(20)  # wait 5 seconds

                    elif res2.json()['status'] == 'done':
                        return res2.json()['result']
                else:
                    raise Exception(f'can not reach bot api {bot.value.upper()}, {res2}')
        else:
            raise Exception(f'can not get REQUEST_ID, {res1}')

