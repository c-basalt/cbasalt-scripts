#!/usr/bin/env python3
import json
import time
import asyncio
import subprocess
import urllib.parse
import sys
import os

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

    async def start(self):
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, ADDR, PORT)
        await site.start()

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
    def __init__(self, channel_id):
        self.channel_id = channel_id
        self.server = ProxyServer(self)
        self.basename = 'fc2-record/%s-%.0f' % (channel_id, time.time())
        self.ffmpeg_future = None
        self.log_queue = asyncio.Queue()
        self.chat_queue = asyncio.Queue()

    async def logging_worker(self):
        while True:
            with open(self.basename + '.log', 'at', encoding='utf-8') as f:
                (msg, print_log) = await self.log_queue.get()
                log_str = '[%s] %s\n' % (time.asctime(), msg)
                if print_log:
                    print(log_str, end='')
                f.write(log_str)

    async def chat_worker(self):
        while True:
            with open(self.basename + '.chat.json', 'at', encoding='utf-8') as f:
                data = await self.chat_queue.get()
                f.write('%s\n' % (json.dumps(data, ensure_ascii=False)))
    
    async def ffmpeg_worker(self):
        with open(self.basename + '.ts.log', 'at', encoding='utf-8') as f:
            cmd = 'ffmpeg -hide_banner -i "http://%s:%s/m3u8" -c copy %s.ts' % (ADDR, PORT, self.basename)
            p = await asyncio.create_subprocess_shell(cmd, stdout=f, stderr=f)
            await p.wait()

    def log(self, msg, print_log=False):
        self.log_queue.put_nowait((msg, print_log))

    async def post(self, url, payload):
        async with self.session.post(url, data=payload) as r:
            # print(url, r.status)
            data = json.loads(await r.text())
            return data

    async def get(self, url):
        async with self.session.get(url) as r:
            # print(url, r.status)
            if r.status != 200:
                self.log('%d %s\n%s' % (r.status, r.url, (await r.data())), print_log=True)
                if r.status == 403:
                    await websocket.send_json({"name":"get_hls_information","arguments":{},"id":1})
            data = await r.read()
            self.log(url)
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
        count = 2
        while True:
            await websocket.send_json({"name":"heartbeat","arguments":{},"id":count})
            await asyncio.sleep(30)
            count += 1

    async def main(self):
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"}
        self.session = aiohttp.ClientSession(headers=headers)
        await self.server.start()
        url = await self.get_control_server()
        async with self.session.ws_connect(url) as websocket:
            self.websocket = websocket
            await websocket.send_json({"name":"get_hls_information","arguments":{},"id":1})
            self.heartbeat_future = asyncio.ensure_future(self.heartbeat(websocket))
            self.logger_future = asyncio.ensure_future(self.logging_worker())
            self.chat_future = asyncio.ensure_future(self.chat_worker())
            async for message in websocket:
                try:
                    await self.handle_ws_message(message)
                except KeyboardInterrupt:
                    break
            self.heartbeat_future.cancel()
            self.ffmpeg_future.cancel()
        self.logger_future.cancel()
        self.chat_future.cancel()
        await self.ffmpeg_future
        await self.logger_future
        await self.chat_future

    async def handle_ws_message(self, message):
        if message.type == aiohttp.WSMsgType.TEXT:
            data = json.loads(message.data)
            if "playlists" in data.get("arguments", {}):
                max_mode = -1
                for i in data["arguments"]["playlists"]:
                    if i['mode'] > max_mode and i['mode'] < 90:
                        self.m3u8_url = i['url']
                        max_mode = i['mode']
                self.log('start recording: %s' % self.m3u8_url, print_log=True)
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
                self.log('%s %s' % (data.keys(), str(data)[:300]), print_log=True)
            return data.get('name', None)

if __name__ == '__main__':
    if not test_ffmpeg():
        print('Please make sure ffmpeg is installed and added to PATH')
        exit(1)
    if len(sys.argv) != 2:
        print(sys.argv[0], '<channel_id>')
    else:
        os.makedirs('fc2-record', exist_ok=True)
        asyncio.get_event_loop().run_until_complete(FC2Handler(sys.argv[1]).main())
