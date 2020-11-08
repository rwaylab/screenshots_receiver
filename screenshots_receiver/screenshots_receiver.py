from kafka import KafkaConsumer, KafkaProducer
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from PIL import Image, ImageFont, ImageDraw
from kafka_logger import KafkaLoggingHandler
from threading import Timer,Thread,Event
from io import BytesIO
import json
import base64
import os
import logging
from urllib.parse import unquote
import textwrap
import time
import random
import datetime
from multiprocessing import Process
import asyncio


##FOLDERS##
ROOTDIR = '/data/screenshots'
# ROOTDIR = 'screenshots'
# ROOTDIR = '/home/nkalt/PycharmProjects/sr_app/screenshots'

## APPLICATION NAME
APPLICATION = 'Screenshots Receiver'
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092').split()

## LOGGING SETUP
class AppFilter(logging.Filter):
    def filter(self, record):
        record.application = APPLICATION
        return True


## LOGGING SETUP
logger = logging.getLogger(__name__)
logger.addFilter(AppFilter())
logger.setLevel(logging.INFO)
log_producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)  #["10.199.13.36:9091", "10.199.13.37:9092", "10.199.13.38:9093"])
# log_producer = KafkaProducer(bootstrap_servers=["78.157.221.27:9091", "78.157.221.43:9092", "78.157.221.45:9093"])

logger.addHandler(KafkaLoggingHandler(log_producer, 'logs'))


class Counter():

    def __init__(self):
        self.rootdir = ROOTDIR
        self.counter = {}

    async def run (self):
        while True:
            await self.update_counter()
            await asyncio.sleep(60)

            # logger.info('CYCLED MESSAGE')

    async def update_counter(self):
        folders = ([name for name in os.listdir(self.rootdir) if os.path.isdir(os.path.join(self.rootdir, name))])
        for folder in folders:
            contents = os.listdir(os.path.join(self.rootdir, folder))
            for c in contents:
                self.counter_stats(task_id=folder)
        logger.info('Статистика по скриншотам: {}'.format(self.counter), extra={"addressee": "operator"})
        self.counter = {}

    def counter_stats(self, task_id=''):
        am = self.counter.get(task_id, 0)
        self.counter[task_id] = am + 1


class ScreensReceiver:

    def __init__(self, task_queue, app):
        self.rootdir = ROOTDIR
        self.path = ""
        self.task_queue = task_queue
        self.group_id = 'screenshots_recever_1'
        self.app = app
        asyncio.ensure_future(self.receive())

    def draw_text_img(self, font_size=None, max_size = None,  text=' '):
        font = ImageFont.truetype("OpenSans.ttf", font_size)
        lines = textwrap.wrap(text, width=65)
        height = font.getsize(lines[0])[1]
        y_text = 0
        text_img = Image.new('RGB', (max_size, height*len(lines)+height//2), "black")
        for line in lines:
            # print (line)
            width, height = font.getsize(line)
            text_img_t = ImageDraw.Draw(text_img)
            text_img_t.text((0, y_text), line, font=font, fill=(255, 255, 255))
            y_text += height

        return text_img

    def make_addition(self, base_img=object, text="", font_size = 18):
        max_size = base_img.size[0]
        font_size=font_size
        text_img = self.draw_text_img(font_size=font_size, text=text, max_size=max_size)
        new_image = Image.new("RGB", (max_size, base_img.size[1]+text_img.size[1]), (255,255,255))
        new_image.paste(text_img, (0,0))
        new_image.paste(base_img, (0,text_img.size[1]))
        return new_image

    def strTimeProp(self, start, end, format, prop):
        """Get a time at a proportion of a range of two formatted times.
        start and end should be strings specifying times formated in the
        given format (strftime-style), giving an interval [start, end].
        prop specifies how a proportion of the interval to be taken after
        start.  The returned time will be in the specified format.
        """
        stime = time.mktime(time.strptime(start, format))
        etime = time.mktime(time.strptime(end, format))

        ptime = stime + prop * (etime - stime)

        return time.strftime(format, time.localtime(ptime))

    def randomDate(self, start, end, prop, real_time):
        r = ' '
        try:
            r = self.strTimeProp(start, end, '%Y-%m-%d %H:%M:%S', prop)
            return r
        except:
            r = real_time
            return r

    async def receive(self):
        logger.info("Screenshots Receiver ожидает скриншоты")
        while self.app['running']:
            message = await self.task_queue.get()
            screen_id = message.get('screen_id', 'empty_id')
            task_id = message.get('task_id', '')
            try:
                real_time = message['real_time']
                logger.info("received {}".format(message.get('task_id', '')), extra={"task_id": message.get('task_id','')})

                url = message.get('url', '')

                if not os.path.exists(self.rootdir+'/'+task_id):
                    os.makedirs(self.rootdir+'/'+task_id)
                    logger.info("folder created", extra={"task_id": task_id})
                if message['value'].get('image'):
                    base_img = Image.open(BytesIO(base64.b64decode(message['value'].get('image',''))))
                    img = self.make_addition(base_img=base_img, text="URL: "+url+"  ", font_size=20)
                    img = self.make_addition(base_img=img, text='TIME: ' + str(real_time) + '  ', font_size=20)
                    img.save(self.rootdir+'/{}/{}.jpeg'.format(task_id, screen_id))
                    logger.info("screenshot saved {}".format(screen_id), extra={"task_id": task_id})
            except:
                logger.warning('ошибка получения скриншота {} {}'.format(task_id, screen_id))


async def consume_urls(app):
    urls_consumer = AIOKafkaConsumer(
        'screenshots',
        loop=app.get('loop'),
        bootstrap_servers=BOOTSTRAP_SERVERS,  #["10.199.13.36:9091", "10.199.13.37:9092", "10.199.13.38:9093"],
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf8')),
        consumer_timeout_ms=60000*60,
        group_id='screenshots_recever_1'
    )
    await urls_consumer.start()
    try:
        async for message in urls_consumer:
            real_time = '{0:%Y-%m-%d %H:%M:%S}'.format(
                datetime.datetime.fromtimestamp((message.timestamp // 1000) - 10)
            )
            task_id = message.value.get('task_id', '')
            url = unquote(message.value.get('url', ''))
            screen_id = message.value.get('screen_id', 'empty_id')
            await app['to_process'].put({
                'value': message.value,
                'task_id':task_id,
                'real_time':real_time,
                'url':url,
                'screen_id':screen_id,
                'timestamp': message.timestamp
            })
    except:
        app['logger'].debug('urls_consumer exception')
    finally:
        app['logger'].debug('urls_consumer stopped')
        await urls_consumer.stop()


async def start(loop):

    app = {
        'loop': loop,
        'running': True,
        'to_process': asyncio.Queue(maxsize=64)
    }
    app['receivers'] = [ScreensReceiver(app['to_process'], app) for i in range(64)]
    app['counter'] = Counter().run()
    tasks = asyncio.gather(consume_urls(app))
    await tasks

loop = asyncio.get_event_loop()
loop.run_until_complete(start(loop))
# loop = asyncio.get_running_loop()
loop.close()