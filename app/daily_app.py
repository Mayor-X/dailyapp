import time
import traceback

from bot_api import BotAPI, BotName
from database.scrapers import ScraperDB

bot_api = BotAPI()
PRIORITY_BOTS = {BotName.CARGOBOARD: 6,
                 BotName.EUROPACCO: 2,
                 BotName.JUMINGO: 2,
                 # BotName.QUICARGO: 1
                 }

# ref: youtube/v=SH_KOHyU6Dg
while True:
    # go through all available bots 3 times
    for i in range(3):
        for bot in BotName:
            try:
                res = bot_api.call(bot)
                print(f'{bot.value} fetched {len(res)} prices')
                for row in res:
                    ScraperDB().insert(row)
            except:
                print(traceback.format_exc())

    # # for priority bots, get more data points

    for bot, times in PRIORITY_BOTS.items():
        for i in range(times):
            try:
                res = bot_api.call(bot)
                print(f'{bot.value} fetched {len(res)} prices')
                for row in res:
                    ScraperDB().insert(row)
                time.sleep(5)
            except:
                print(traceback.format_exc())

    print('sleeping 5 mins..')
    time.sleep(5 * 60)  # sleep 5 minute
