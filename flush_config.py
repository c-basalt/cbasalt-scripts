#!/usr/bin/env python3

import os

basedir = os.path.dirname(__file__)


def load_cookies(basename):
    with open(os.path.join(basedir, basename), 'rt', encoding='utf-8') as f:
        lines = f.readlines()

    cookies = {}
    for line in lines:
        if line.startswith('#'):
            continue
        entry = line.strip().split('\t')
        if len(entry) < 7:
            continue
        key, value = entry[5:]
        cookies[key] = value
    return cookies
    

def flush_ytb_cookies():
    cookies = load_cookies('youtube-cookies.txt')
    lines = []
    for key, value in cookies.items():
        if key in ['SID', 'HSID', 'SSID', 'APISID', 'SAPISID'] or True:
            lines.append('http-cookie=%s=%s' % (key, value))
    with open(os.path.join(basedir, 'youtube-config.txt'), 'wt', encoding='utf-8') as f:
        f.write('\n'.join(lines)+'\n')


def flush_twitch_cookies():
    cookies = load_cookies('twitch-cookies.txt')
    lines = []
    for key, value in cookies.items():
        lines.append('http-cookie=%s=%s' % (key, value))
    with open(os.path.join(basedir, 'twitch-config.txt'), 'wt', encoding='utf-8') as f:
        f.write('\n'.join(lines)+'\n')

flush_ytb_cookies()
flush_twitch_cookies()
