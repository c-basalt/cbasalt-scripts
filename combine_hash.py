#!/usr/bin/env python3

import traceback
import os
import sys
import difflib
import shutil

def load_hashfile(fn):
    with open(fn, 'rt') as f:
        lines = f.readlines()
    hashes = []
    for line in lines:
        line = line.strip()
        if line:
            fields = line.split('\t')
            md5, ts = fields[:2]
            fields.append(fn)
            hashes.append( tuple(fields) )
    start, end = 0, len(hashes)
    if hashes[0][0] in ["385582ab16cda6cd4679a885595934d8", "bfe0f881f9171d081de1b75dd22036c1", "3b39b1eba5581f61885ba84348d6022b", "890bbdf2f5c958a93a873a0aac693b5e"]:
        start = 1
    if hashes[-1][0] == "4190e69bc46ee05ab77499152204f527":
        end = -1
    return hashes[start:end]

def print_c(hashes, offset, up=3, down=3):
    if offset < 0:
        offset = offset+len(hashes)
    for i in hashes[max(0, offset-up):min(offset+down, len(hashes))]:
        print(i)
    print()

def match(lead, follow, match_num=3600):
    lead_hashes = [i[0] for i in lead[-match_num:]]
    follow_hashes = [i[0] for i in follow[:match_num]]
    s = difflib.SequenceMatcher(None, lead_hashes, follow_hashes)
    a, b, size = s.find_longest_match(0, len(lead_hashes), 0, len(follow_hashes))
    if size > 0 and a > 0:
        if b <= 1 and a+size == min(match_num, len(lead_hashes)):
            return len(lead)+a-match_num, b
        elif b == 0 and size == len(follow_hashes) < match_num:
            return len(lead), len(follow_hashes)
        elif size == 1:
            # print(a, b, size, len(lead_hashes), len(follow_hashes))
            # print(follow[b])
            # print('Likely random match (size==1), assuming no overlap', lead[-1][-1], follow[0][-1])
            print('No overlap between', lead[-1][-1], follow[0][-1])
            return len(lead), 0
        else:
            if lead[a] != follow[b] and lead[a+size-1] != follow[b+size-1]:
                # print('False match, assuming no overlap', lead[a][0], follow[b][0])
                print('No overlap between', lead[-1][-1], follow[0][-1])
                return len(lead), 0 
            print(a, b, size, len(lead_hashes), len(follow_hashes))
            print(*lead[a:a+size], *follow[b:b+size], sep='\n')
            raise AssertionError
    else:
        print('No overlap between', lead[-1][-1], follow[0][-1])
        return len(lead), 0
    print(a, b, size, a+size)
    print_c(lead, a-match_num)
    print_c(follow, b)
    print(b==0, (a+size)%match_num==0)

def iter_neibor(iterable):
    last = None
    for i in iterable:
        if last is None:
            last = i
        else:
            yield last, i
            last = i

def match_and_join(lead, follow):
    while lead[-1][5] in ['end']:
        lead = lead[:-1]
    while follow[0][5] in ['header']:
        follow = follow[1:]
    a_end, b_start = match(lead, follow)
    return lead[:a_end] + follow[b_start:]

def join_hashes(hashes):
    if not hashes:
        return []
    lead = hashes[0]
    for follow in hashes[1:]:
        lead = match_and_join(lead, follow)
    return lead

def common_name(files):
    files = [f.split('-hash_')[0] for f in files]
    s = difflib.SequenceMatcher(None, files[0], files[-1])
    a, b, size = s.find_longest_match(0, len(files[0]), 0, len(files[-1]))
    if a == b == 0 and size:
        return files[0][:size].split('.fix')[0]
    else:
        return files[0].split('.fix')[0]

def main():
    files = sorted(sys.argv[1:])
    if len(files) == 1 and not os.path.exists(files[0]):
        print(files[0], 'not exists')
        return
    outname = common_name(files) + '.combine-hash.txt'
    v_hashes = [load_hashfile(f) for f in files if f.endswith('v.txt')]
    a_hashes = [load_hashfile(f) for f in files if f.endswith('a.txt')]
    v_hash = join_hashes(v_hashes)
    a_hash = join_hashes(a_hashes)
    lines = ['\t'.join(i) for i in v_hash+a_hash]
    if lines:
        with open(outname, 'wt') as f:
            f.write('\n'.join(lines))
            f.write('\n')
    os.makedirs("split-hashes", exist_ok=True)
    for file in files:
        shutil.move(file, "split-hashes")

if __name__ == '__main__':
    main()
