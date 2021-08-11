import urllib.request
import urllib.error
import time
import multiprocessing
from multiprocessing import Pool
import pymysql
from discord_hooks import Webhook
import pytz
import datetime
import signal


def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def checkurl(url):
    try:
        conn = urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        # Return code error (e.g. 404, 501, ...)
        #print('HTTPError: {}'.format(e.code) + ', ' + url)
        pass
    except urllib.error.URLError as e:
        # Not an HTTP-specific error (e.g. connection refused)
        print('URLError: {}'.format(e.reason) + ', ' + url)
    else:
        # 200
        sku = url[-8:-2]
        # print(sku)
        return sku


def getValidItems(url, pool):
    urls = []

    for i in range(300001):
        sku = format(i, '06d')
        urls.append(url.format(sku))

    results = pool.map(checkurl, urls)
    validSkus = [x for x in results if x != None]

    return validSkus


def getOldItems(conn, storeName):
    conn.ping(reconnect=True)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM `{}`'.format(storeName))
    oldItems = [item[0] for item in cursor.fetchall()]
    return oldItems


def sendDiscordUpdate(storeName, skuNumber, itemUrl, webHookUrl):
    embed = Webhook(webHookUrl, desc="New Image Found", color=0x1A1818)
    imageUrl = itemUrl.format(skuNumber)
    embed.set_image(url=imageUrl)
    embed.set_footer(text="Mesh Image Scraper | {0:%I:%M:%S %p} EST".format(datetime.datetime.now(tz=pytz.timezone(
        'US/Eastern'))), icon="BrandUrlIcon")
    embed.add_field(name="PID", value=skuNumber, inline=True)
    embed.add_field(name="Store", value=storeName, inline=True)
    embed.post()
    time.sleep(.1)


def sendDiscordUpdateTwo(storeName, skuNumber, itemUrl, webHookUrl):
    embed = Webhook(webHookUrl, desc="New Image Found", color=0x1132d8)
    imageUrl = itemUrl.format(skuNumber)
    embed.set_image(url=imageUrl)
    embed.set_author(name="Mesh Image Scraper",
                     icon="BrandIconUrl")
    embed.set_footer(text="{0:%H:%M:%S} EST".format(datetime.datetime.now(tz=pytz.timezone(
        'US/Eastern'))), icon="BrandUrlIcon")
    embed.add_field(name="PID", value=skuNumber, inline=True)
    embed.add_field(name="Store", value=storeName, inline=True)
    embed.post()
    time.sleep(.1)


if __name__ == "__main__":

    # FIRST DISCORD
    urls = {'Footpatrol': 'https://i1.adis.ws/i/jpl/fp_{}_a',
            'size?': 'https://i1.adis.ws/i/jpl/sz_{}_a',
            'Hip Store': 'https://i1.adis.ws/i/jpl/hp_{}_a'}

    webHookUrls = {
        'Footpatrol': 'https://discordapp.com/api/webhooks/436248183597694986/_vB4PsnTQ1iWDqcUHEejgVKl5edfe7KLlaUnBx5sdwI_Tnd9i8GYVUmrsIKZTaD6Waws',
        'size?': 'https://discordapp.com/api/webhooks/436248268469567489/dBAKLKOYxKHXHRxtlE9ZH2b7TpvDv4B20gxBRVWraMdOUjbw86iLjEPduJCi2SPVg37N',
        'Hip Store': 'https://discordapp.com/api/webhooks/436248345816858624/MoFgD0kO96cNF-RdoorHtySRnTaRkI8v_rTNTEMh6EszGlgne5sMOc4BuDqd3BvW42ku'
    }
    #

    # SECOND DISCORD
    secondWebHookUrls = {
        'Footpatrol': 'https://discordapp.com/api/webhooks/445723878652772352/qYWEbz4f_NPweYMr1i75crpD1zAzCzj36tYkXGDPXqo7vxAq9tVx261SIf0HDys_JKh1',
        'size?': 'https://discordapp.com/api/webhooks/445725003317641247/oxUIYIsbWNZfZRjVOdTm1lasmWxFDyI4FwrQFelj3RsB5qcj3xLuez0BcbwFrgexyDo6',
        'Hip Store': 'https://discordapp.com/api/webhooks/445725102404009984/NRTTBZxUVakmR1kb4TFWXw0meUCnsWGkevjbsmwKJ1OwVwc2ceyHyOYh1ccV8bvpQ5iS'
    }
    #

    pool = Pool(multiprocessing.cpu_count() * 2, init_worker)
    conn = pymysql.connect("tyduzbv3ggpf15sx.cbetxkdyhwsb.us-east-1.rds.amazonaws.com",
                           "id2wxviwiy7m0j5h", "ub7qpmsk3hgcbyh9", "l277hkx53tf3u9lv")

    try:
        while True:
            for storeName, url in urls.items():
                webHookUrl = webHookUrls[storeName]
                validItems = getValidItems(url, pool)
                oldItems = getOldItems(conn, storeName)

                newItems = list(set(validItems) - set(oldItems))
                for newItem in newItems:
                    conn.cursor().execute("INSERT INTO `{}` (`sku`) Values ('{}')".format(storeName, newItem))

                    # First Discord
                    sendDiscordUpdate(storeName, newItem, url,
                                      webHookUrls[storeName])

                    # Second Discord
                    sendDiscordUpdateTwo(
                        storeName, newItem, url, secondWebHookUrls[storeName])
                    #sendDiscordUpdateTwo(storeName, newItem, url, secondWebHookUrls2[storeName])
                conn.commit()
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, terminating workers")
        pool.terminate()
        pool.join()
    conn.close()

#start = time.time()
#print("Done in : ", time.time()-start)
