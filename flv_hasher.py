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
        flv_tag = FlvTag(tag_type, timestamp, data, fh_pos)
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
    def __init__(self, type_id, timestamp, data, pos):
        self.type_id = type_id
        self.timestamp = timestamp
        self.data = data
        self.pos = pos
        self.md5, self.sha1 = get_md5_hex(data), get_sha1_hex(data)
        self.parse_meta()
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
    parser.add_argument('--skip_existing', action='store_true')
    args = parser.parse_args()
    for fn in args.files:
        try:
            with open(fn, 'rb') as f:
                reader = FlvReader(f, args)
                reader.dump_hash()
        except Exception:
            traceback.print_exc()

if __name__ == '__main__':
    main()
