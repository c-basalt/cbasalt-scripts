#!/usr/bin/env python3

import logging
import hashlib
import traceback
import os
import sys
import time
import multiprocessing as mp
import argparse
import traceback
import re

logger = logging.getLogger()

def big_endian_to_int(big_end_bytes):
    value = 0
    for byte in big_end_bytes:
        value *= 256
        value += byte
    return value

def get_md5_hex(data):
    md5_hash = hashlib.md5(data)
    return md5_hash.hexdigest()

def get_sha1_hex(data):
    sha1_hash = hashlib.sha1(data)
    return sha1_hash.hexdigest()

class FlvReader(object):
    def __init__(self, file, args):
        if isinstance(file, str):
            file = open(file, 'rb')
        self._file = file
        self.args = args
        self.parse_header()
        assert self.read_ui32() == 0
    
    @property
    def filename(self):
        return self._file.name
    
    def tell(self):
        return self._file.tell()
    def seek(self, *args):
        return self._file.seek(*args)
    def close(self):
        self._file.close()

    def read_bytes(self, size):
        data = self._file.read(size)
        if len(data) < size:
            raise IOError("End of file")
        else:
            return data

    def read_int(self, size):
        return self.to_int(self.read_bytes(size))
        
    def read_ui8(self):
        return self.read_int(1)

    def read_ui24(self):
        return self.read_int(3)

    def read_ui32(self):
        return self.read_int(4)

    def to_int(self, value_bytes):
        return big_endian_to_int(value_bytes)

    def parse_header(self):
        self.seek(0)
        header_bytes = self.read_bytes(9)
        if not header_bytes[:3] == b'FLV':
            raise ValueError('Incorrect header signature: %s' % header_bytes[:3])
        if header_bytes[3] != 1:
            raise ValueError('Unexpected flv version: %d' % header_bytes[3])
        if header_bytes[4] & 0b11111010 != 0:
            raise ValueError('Reserved bytes not zero: %s' % bin(header_bytes[4]))
        self.has_video = (header_bytes[4] & 0x01)==0x01
        self.has_audio = (header_bytes[4] & 0x04)==0x04
        if self.to_int(header_bytes[5:]) != 9:
            raise ValueError('Unexpected header size')
    
    def get_next_tag(self):
        fh_pos = self.tell()
        tag_type = self.read_ui8()
        data_size = self.read_ui24()
        timestamp = self.read_ui24()
        timestamp += self.read_ui8() * (2**24)
        stream_id = self.read_ui24()
        if stream_id != 0:
            logger.warn('Unexpected stream_id %d' % stream_id)
        data = self.read_bytes(data_size)
        tag_size = self.read_ui32()
        if tag_size != data_size + 11:
            logger.warn('Tag size %d does not match data size %d' % (tag_size, data_size))
        flv_tag = FlvTag(tag_type, timestamp, data, fh_pos, self.args)
        return flv_tag

    def iter_tag(self):
        while True:
            try:
                yield self.get_next_tag()
            except IOError:
                break

    def get_next_video_tag(self):
        flv_tag = self.get_next_tag()
        while flv_tag.type != 'video':
            flv_tag = self.get_next_tag()
        return flv_tag

    def get_next_audio_tag(self):
        flv_tag = self.get_next_tag()
        while flv_tag.type != 'audio':
            flv_tag = self.get_next_tag()
        return flv_tag
    
    def dump_hash(self):
        basename = os.path.splitext(os.path.basename(self.filename))[0]
        if self.args.skip_existing:
            if os.path.exists('%s-hash_v.txt' % basename):
                return
            if os.path.exists('%s-hash_a.txt' % basename):
                return
            if os.path.exists('%s-hash_misc.txt' % basename):
                return
        print('Begin hashing for %s\tfile size: %.3f MiB' % (self.filename, os.stat(self.filename).st_size/(1024**2)), flush=True)
        start_time = time.time()
        first_tags = set()
        with open('%s-hash_v.txt' % basename, 'wt', encoding='utf-8') as vhash_f:
            with open('%s-hash_a.txt' % basename, 'wt', encoding='utf-8') as ahash_f:
                with open('%s-hash_misc.txt' % basename, 'wt', encoding='utf-8') as mhash_f:
                    for tag in self.iter_tag():
                        if tag.type == 'video':
                            if self.args.print_first and 'video' not in first_tags:
                                first_tags.add('video')
                                print('video', tag.pos, len(tag.data), tag.timecode, sep='\t')
                            vhash_f.write('%s\t%s\t%s\t%10d\t%8d\t%s\n' % (tag.hexdigest, tag.timecode, tag.frame_type, tag.pos, tag.size, tag.packet_type))
                        elif tag.type == 'audio':
                            if self.args.print_first and 'audio' not in first_tags:
                                first_tags.add('audio')
                                print('audio', tag.pos, len(tag.data), tag.timecode, sep='\t')
                            ahash_f.write('%s\t%s\t%s\t%10d\t%8d\t%s\n' % (tag.hexdigest, tag.timecode, tag.sound_rate, tag.pos, tag.size, tag.sound_format))
                        else:
                            if self.args.print_first and 'script' not in first_tags:
                                first_tags.add('script')
                                print('script', tag.pos, len(tag.data), tag.timecode, sep='\t')
                            mhash_f.write('%s\t%s\t%s\t%d\n' % (tag.hexdigest, tag.timecode, tag.type, len(tag.data)))
                            if self.args.dump_script:
                                script_fn = '%s-script-%s.bin' % (basename, tag.timecode.replace(':', ''))
                                with open(script_fn, 'wb') as f:
                                    f.write(tag.data)
        print('done %.2fs' % (time.time() - start_time, ))

class FlvTag(object):
    def __init__(self, type_id, timestamp, data, pos, args):
        self.args = args
        self.type_id = type_id
        self.timestamp = timestamp
        self._data = data
        self.pos = pos
        self.md5, self.sha1 = get_md5_hex(self.data), get_sha1_hex(self.data)
        self.parse_meta()
    
    def filter_hls(self, data):
        if data[5:10].startswith(b'\x00\x00\x00\x02'):
            if data[0] == 0x17:
                assert data[5:11] == b"\x00\x00\x00\x02\t\xf0", str(data[0:5]) + str(data[5:20]) + self.type
                if data[11:15] == b"\x00\x00\x00\x0e":
                    assert data[40-11:44-11] == b"\x00\x00\x00\x04", data[40-11:44-11]
                    return data[0:5] + data[48-11:]
                elif data[11:15] == b"\x00\x00\x00\x27":
                    assert data[54:58] == b"\x00\x00\x00\x04"
                    return data[0:5] + data[62:]
            else:
                assert data[5:11] == b"\x00\x00\x00\x02\x09\xf0", str(data[0:5]) + str(data[5:20]) + self.type
                return data[0:5] + data[11:]
        else:
            return data

    header_re = re.compile(b'\x00\x00\x00\x3e\x06\x05:BVCLIVETIMESTAMP{"author":"nginx","curr_ms":\\d{13}}\x80')
    def filter_vheader(self, data):
        if data[5:10].startswith(b'\x00\x00\x00\x02\t'):
            data = data[:5] + data[11:]
        if b'BVCLIVETIMESTAMP' in data[:300]:
            if self.header_re.match(data[5:5+66]):
                return data[:5] + data[5+66:]
            elif b'\x00\x00\x00t\x06\x05pBVCLIVETIMESTAMP{"author":"pc_link","author_ver":"4.38.1.4464"' in data[:300]:
                header_index = data[:390].index(b'\x00\x00\x00t\x06\x05pBVCLIVETIMESTAMP')
                pre_header = data[5:header_index]
                while pre_header:
                    assert pre_header[:3] == b'\x00\x00\x00'
                    body_length = pre_header[3]
                    assert body_length+4 <= len(pre_header)
                    pre_header = pre_header[body_length+4:]
                header_end = header_index + len(b'\x00\x00\x00t\x06\x05pBVCLIVETIMESTAMP{"author":"pc_link","author_ver":"4.38.1.4464","clock_max_error_ms":235,"curr_ms":1678201398976}\x80')
                return data[:5] + data[header_end:]
            elif b'BILIAVC.1.4.2 - H.264/AVC codec - Copyright 2019-2021' in data[:300]:
                header_index = data[:390].index(b'\x00\x00\x00\x98\x06\x05')
                pre_header = data[5:header_index]
                while pre_header:
                    assert pre_header[:3] == b'\x00\x00\x00'
                    body_length = pre_header[3]
                    assert body_length+4 <= len(pre_header)
                    pre_header = pre_header[body_length+4:]
                header_end = header_index + len(b'\x00\x00\x00\x98\x06\x05X\xb3\xe1c0\x8c<\x9eO\xc29\x81\t~\xaa\xa5. BILIAVC.1.4.2 - H.264/AVC codec - Copyright 2019-2021 (c) Bilibili Inc\x00\x05:BVCLIVETIMESTAMP{"author":"nginx","curr_ms":1679663515323}\x80')
                assert re.match(b':BVCLIVETIMESTAMP{"author":"nginx","curr_ms":\\d{13}}\x80', data[header_end-len(b':BVCLIVETIMESTAMP{"author":"nginx","curr_ms":1679663515323}\x80'):header_end])
                return data[:5] + data[header_end:] 
            else:
                raise NotImplementedError('unexpected data %s' % data[5:300])
        else:
            assert data[:3] != b'\x00\x00\x00'
            return data
    @property
    def data(self):
        data = self._data
        if self.args.hls_fix:
            data = self.filter_hls(data)
        if not self.args.vheader_ignore:
            data = self.filter_vheader(data)
        return data

    def parse_meta(self):
        self.meta = {}
        if not self.data:
            return
        if self.type == 'video':
            self.meta['frame_type'] = self.data[0] // 16
            self.meta['codec_id'] = self.data[0] % 16
            if self.codec_id == 'AVC':
                self.meta['avc_packet_type'] = self.data[1]
        elif self.type == 'audio':
            self.meta['sound_format'] = self.data[0] // 16
            self.meta['sound_rate'] = (self.data[0] % 16) // 4            
    def get_meta_desc(self, name, def_dict):
        value = self.meta.get(name, None)
        return def_dict.get(value, value)
    @property
    def hexdigest(self):
        return self.md5 + self.sha1[:12]
    @property
    def frame_type(self):
        return self.get_meta_desc('frame_type', {
            1: 'I',
            2: ' P',
        })
    @property
    def codec_id(self):
        return self.get_meta_desc('codec_id', {
            7: 'AVC',
        })
    @property
    def packet_type(self):
        if self.codec_id == 'AVC':
            return self.get_meta_desc('avc_packet_type', {
                0: 'header',
                1: 'NALU',
                2: 'end',
            })
    @property
    def sound_rate(self):
        return self.get_meta_desc('sound_rate', {
            3: '44kHz',
        })
    @property
    def sound_format(self):
        return self.get_meta_desc('sound_format', {
            10: 'AAC',
        })
    @property
    def timecode(self):
        s = self.timestamp/1000
        m, s = s // 60, s % 60
        h, m = m // 60, m % 60
        return '%02d:%02d:%06.3f' % (h, m, s)
    @property
    def type(self):
        return {
            8: 'audio',
            9: 'video',
            18: 'script',
        }.get(self.type_id, self.type_id)
    @property
    def size(self):
        return len(self.data)

    def __repr__(self):
        return '<%s type=%s ts=%d md5=%s>' % (self.__class__.__name__, self.type, self.timestamp, self.md5)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('files', type=str, nargs='+')
    parser.add_argument('--dump_script', action='store_true')
    parser.add_argument('--print_first', action='store_true')
    parser.add_argument('--skip_existing', action='store_true', help='skip hashing if a hash text file already exists')
    parser.add_argument('--vheader_ignore', action='store_true', help='skip removal of BVCLIVETIMESTAMP header from video data')
    parser.add_argument('--hls_fix', action='store_true', help='remove 2-byte padding(?) from ts data')
    parser.add_argument('--get_tag', type=int, metavar='<start>', help='extract the data of a tag using its start_pos instead of calc hashes of tags')
    parser.add_argument('--get_raw', type=int, metavar='<start>', help='extract the raw bytes of a tag using its start_pos instead of calc hashes of tags')
    args = parser.parse_args()
    if args.get_tag or args.get_raw:
        try:
            with open(args.files[0], 'rb') as f:
                reader = FlvReader(f, args)
                pos = args.get_tag or args.get_raw
                f.seek(pos)
                tag = reader.get_next_tag()
                if args.get_tag:
                    with open(tag.hexdigest+'.data', 'wb') as f_tag:
                        f_tag.write(tag.data)
                print(tag)
                f.seek(pos)
                if args.get_raw:
                    with open(tag.hexdigest+'.raw', 'wb') as f_tag:
                        f_tag.write(f.read(len(tag.data)+15))
        except Exception:
            traceback.print_exc()
    else:
        for fn in args.files:
            try:
                with open(fn, 'rb') as f:
                    reader = FlvReader(f, args)
                    reader.dump_hash()
            except Exception:
                traceback.print_exc()

if __name__ == '__main__':
    main()
