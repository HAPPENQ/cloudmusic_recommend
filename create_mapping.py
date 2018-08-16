#coding:utf-8

import pymongo as pm
from tqdm import tqdm
import json
import csv
import traceback, sys


client = pm.MongoClient('192.168.1.109', 27017)

def load_jmake_songs(in_name='data/tb_media.csv'):
    jmake_song_name_id = {}
    with open(in_name, encoding='utf8') as f:
        reader = csv.reader(f)
        for row in reader:
            _id = row[0]
            name = row[1]
            if name.endswith('(HD)'):
                name = name[:-4]
            jmake_song_name_id[name] = _id
    return jmake_song_name_id


def load_jmake_actors(in_name='data/tb_actor.csv'):
    jmake_actor_name_id = {}
    with open(in_name, encoding='utf8') as f:
        reader = csv.reader(f)
        for row in reader:
            _id = row[0]
            name = row[1]
            jmake_actor_name_id[name] = _id
    return jmake_actor_name_id


def load_jmake_actor_songs(in_name='data/tb_actor_on_media.csv'):
    jmake_actor_song = {}
    with open(in_name, encoding='utf8') as f:
        reader = csv.reader(f)
        for row in reader:
            actor = row[0]
            song = row[1]
            if actor not in jmake_actor_song:
                jmake_actor_song[actor] = set()
            jmake_actor_song[actor].add(song)
    return jmake_actor_song


jmake_song_name_id = load_jmake_songs()
jmake_actor_name_id = load_jmake_actors()
jmake_actor_song = load_jmake_actor_songs()


def mapping(new_track, name, actor):
    # 歌曲名和歌手都在曲库中
    if name in jmake_song_name_id and actor in jmake_actor_name_id:
        jmake_song_id = jmake_song_name_id[name]
        jmake_actor_id = jmake_actor_name_id[actor]
        if jmake_song_id in jmake_actor_song[jmake_actor_id]:
            new_track['jmake_song_id'] = jmake_song_id
            new_track['jmake_actor_id'] = jmake_actor_id
            new_track['song_name'] = name
            new_track['actor_name'] = actor
            new_track['emotion'] = -1
            # print(new_track)
            return new_track
    return False


# with open('mapping_300M.txt', 'a') as out_file:
#     coll = client.cloudmusic.toplists
#     song_set = set([])
#     for tracks in tqdm(coll.find()):
#         for track in tracks['result']['tracks']:
#             try:
#                 new_track = {}
#                 new_track['_id'] = track['id']
#                 if track['id'] in song_set:
#                     continue
#                 song_set.add(track['id'])
#                 name = track['name']
#                 actor = track['artists'][0]['name']
#                 new_track = mapping(new_track, name, actor)
#                 if new_track:
#                     out_file.write(json.dumps(new_track, ensure_ascii=False) + '\n')
#             except:
#                 traceback.print_exc(file=sys.stdout)

# coll = client.music.song
# with open('mapping_300M.txt', 'a', encoding='utf8') as out_file:
#     for track in tqdm(coll.find()):
#         try:
#             new_track = {}
#             new_track['_id'] = track['id']
#             name = track['name']
#             actor = track['artists'][0]['name']
#             new_track = mapping(new_track, name, actor)
#             if new_track:
#                 out_file.write(json.dumps(new_track, ensure_ascii=False) + '\n')
#         except:
#             traceback.print_exc(file=sys.stdout)


def into_mongo(in_name):
    coll = client.jmake.mapping_netease_jmake
    for line in open(in_name, encoding='utf8'):
        coll.save(json.loads(line.strip()))

into_mongo('mapping.txt')
into_mongo('mapping_300M.txt')