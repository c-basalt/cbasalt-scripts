#!/usr/bin/env python3
import json
import time
import asyncio
import subprocess
import signal
import urllib.parse
import sys
import os
import argparse
from multiprocessing import Process
import traceback

import aiohttp
from aiohttp import web

ADDR = 'localhost'
PORT = 7000

class ProxyServer:
    def __init__(self, handler):
        self.handler = handler
        self.app = web.Application()
        self.app.add_routes([
            web.get('/m3u8', self.m3u8),
            web.get('/file', self.file),
        ])
        self.runner = web.AppRunner(self.app)

    async def start(self):
        await self.runner.setup()
        addr, port = ADDR, PORT
        for i in range(10000):
            try:
                site = web.TCPSite(self.runner, addr, port)
                await site.start()
                print('server started at %s:%s' % (addr, port))
                return addr, port
            except OSError:
                port += 1
        raise OSError('Failed to initiate proxy server')

    @staticmethod
    def process_m3u8(m3u8_str):
        lines = m3u8_str.split('\n')
        for i, line, in enumerate(lines):
            if line.startswith('http'):
                lines[i] = '/file?url=' + urllib.parse.quote(line)
        return '\n'.join(lines)

    async def m3u8(self, request):
        text = self.process_m3u8(await self.handler.get_m3u8())
        return web.Response(text=text)
    
    async def file(self, request):
        url = request.query['url']
        data = await self.handler.get(request.query['url'])
        if 'playlist?' in url:
            text = self.process_m3u8(data.decode())
            return web.Response(text=text)
        else:
            return web.Response(body=data)

def test_ffmpeg():
    try:
        p = subprocess.run(['ffmpeg'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return (p.returncode == 1)
    except FileNotFoundError:
        return False

class FC2Handler:
    def __init__(self, channel_id, out_dir):
        self.channel_id = channel_id
        self.server = ProxyServer(self)
        self.out_dir = out_dir
        self.basename = os.path.join(out_dir, '%s-%.0f' % (channel_id, time.time()))
        self.ffmpeg_future = None
        self.ffmpeg_proc = None
        self.log_queue = asyncio.Queue()
        self.chat_queue = asyncio.Queue()
        self._ws_count = 0

    @property
    def ws_count(self):
        self._ws_count += 1
        return self._ws_count

    async def logging_worker(self):
        while True:
            with open(self.basename + '.log', 'at', encoding='utf-8') as f:
                (msg, print_log) = await self.log_queue.get()
                log_str = '[%s] %s\n' % (time.asctime(), msg)
                if print_log:
                    print(log_str[:600], end='')
                f.write(log_str)

    async def chat_worker(self):
        while True:
            with open(self.basename + '.chat.json', 'at', encoding='utf-8') as f:
                data = await self.chat_queue.get()
                f.write('%s\n' % (json.dumps(data, ensure_ascii=False)))
    
    async def ffmpeg_worker(self):
        with open(self.basename + '.ts.log', 'at', encoding='utf-8') as f:
            cmd = ['ffmpeg', '-hide_banner', '-i', 'http://%s:%s/m3u8'% (self.addr,self.port), '-c', 'copy', '%s.ts'%self.basename]
            self.ffmpeg_proc = await asyncio.create_subprocess_exec(*cmd, stdout=f, stderr=f)
            await self.ffmpeg_proc.wait()

    def log(self, msg, print_log=False):
        self.log_queue.put_nowait((msg, print_log))

    async def post(self, url, payload):
        async with self.session.post(url, data=payload) as r:
            # print(url, r.status)
            data = json.loads(await r.text())
            return data

    async def get(self, url):
        async with self.session.get(url) as r:
            data = await r.read()
            if r.status == 200:
                self.log(url)
            else:
                self.log('%d %s\n%s' % (r.status, r.url, data), print_log=True)
                if self.websocket and r.status == 403:
                    await self.websocket.send_json({"name":"get_hls_information","arguments":{},"id":self.ws_count})
            return data

    async def get_m3u8(self):
        data = await self.get(self.m3u8_url)
        return data.decode()

    async def get_member_info(self):
        payload = {
            'channel': 1,
            # 'profile': 1,
            # 'user': 1,
            'streamid': self.channel_id,
        }
        data = await self.post('https://live.fc2.com/api/memberApi.php', payload)
        assert data['data']["channel_data"]["version"], 'channel is not on live'
        assert data['data']["channel_data"]["is_publish"], 'channel is not on live'
        self.log(json.dumps(data, ensure_ascii=False))
        return {
            'channel_version': data['data']["channel_data"]["version"]
        }
    async def get_control_server(self):
        member_info = await self.get_member_info()
        payload = {
            'channel_id': self.channel_id,
            # 'mode': 'play',
            # 'orz': '',
            'channel_version': member_info['channel_version'],
            # 'client_version': '2.1.1  [1]',
            'client_type': 'pc',
            'client_app': 'browser_hls',
            # 'ipv6': '',
            # 'comment': '2',
        }
        data = await self.post('https://live.fc2.com/api/getControlServer.php', payload)
        return '%s?control_token=%s' % (data['url'], data['control_token'])

    async def heartbeat(self, websocket):
        while True:
            await websocket.send_json({"name":"heartbeat","arguments":{},"id":self.ws_count})
            await asyncio.sleep(30)

    async def main(self):
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"}
        self.session = aiohttp.ClientSession(headers=headers)
        self.addr, self.port = await self.server.start()
        url = await self.get_control_server()
        os.makedirs(self.out_dir, exist_ok=True)
        async with self.session.ws_connect(url) as websocket:
            self.websocket = websocket
            await websocket.send_json({"name":"get_hls_information","arguments":{},"id":self.ws_count})
            self.heartbeat_future = asyncio.ensure_future(self.heartbeat(websocket))
            self.logger_future = asyncio.ensure_future(self.logging_worker())
            self.chat_future = asyncio.ensure_future(self.chat_worker())
            async for message in websocket:
                await self.handle_ws_message(message)
            self.websocket = None
            print('websocket disconnect')
            self.heartbeat_future.cancel()
            if self.ffmpeg_future:
                self.ffmpeg_proc.send_signal(signal.SIGTERM)
                try:
                    await asyncio.wait_for(self.ffmpeg_future, 3)
                except asyncio.TimeoutError:
                    pass
            await asyncio.sleep(0.1)

    async def handle_ws_message(self, message):
        if message.type == aiohttp.WSMsgType.TEXT:
            data = json.loads(message.data)
            if "playlists" in data.get("arguments", {}):
                max_mode = -1
                for i in data["arguments"]["playlists"]:
                    if i['mode'] > max_mode and i['mode'] < 90:
                        self.m3u8_url = i['url']
                        max_mode = i['mode']
                self.log('start recording %s: %s' % (max_mode, self.m3u8_url), print_log=True)
                if not self.ffmpeg_future:
                    self.ffmpeg_future = asyncio.ensure_future(self.ffmpeg_worker())
            elif data.get('name', None) in [
                'initial_connect',
                'connect_complete',
                'connect_data',
                'user_count',
                'video_information',
                'point_information',
                'ng_comment',
                '_response_', # url or heartbeat
            ]:
                pass
            elif data.get('name', None) == 'comment':
                self.chat_queue.put_nowait(data['arguments'])
            else:
                self.log('%s %s' % (data.keys(), str(data)), print_log=True)
            return data.get('name', None)

def wrapper(channel_id, out_dir):
    try:
        asyncio.get_event_loop().run_until_complete(FC2Handler(channel_id, out_dir).main())
    except Exception:
        traceback.print_exc()

def main():
    parser = argparse.ArgumentParser(description='Dump fc2 live stream with ffmpeg', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('channel_id', type=int, help='id number in url')
    parser.add_argument('--loop_interval', type=float, metavar='N',help='number of seconds between tries, exit after single run if not specified')
    parser.add_argument('--out_dir', type=str, default='fc2-record', help='dir for output')
    args = parser.parse_args()
    if not test_ffmpeg():
        print('Please make sure ffmpeg is installed and added to PATH')
        exit(1)
    os.makedirs(args.out_dir, exist_ok=True)
    if args.loop_interval:
        while True:
            p = Process(target=wrapper, args=(args.channel_id, args.out_dir,))
            p.start()
            p.join()
            time.sleep(args.loop_interval)
    else:
        asyncio.get_event_loop().run_until_complete(FC2Handler(args.channel_id, args.out_dir).main())


if __name__ == '__main__':
    main()
