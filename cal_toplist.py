#coding:utf8

import langid
import Levenshtein
import json
import pymongo as pm
import sys
import time
import send_email

reload(sys)
sys.setdefaultencoding( "utf-8" )

client =pm.MongoClient('192.168.1.109',27017)
db=client.cloudmusic


coll = db.day_new
coll_tp = db.toplists
coll_web = db.webdata

coll_jm = db.jmake_songs

db_old=client.music
coll_sf = db_old.song_feature


#records = json.load(open('ktvsongs.json'))['RECORDS']
ktvdict = json.load(open('/home/wengyutao/data_collect/webdata/ktvdict.json'))

def transfer_ktvsongs():
    _temp = {}
    for record in records:
        _name = record['name_norm']
        _artist = record['actors_name']
        _temp[_name] = _artist
    #print _temp
    json.dump(_temp,open('ktvdict.json','w'),ensure_ascii=False)


def match_song(song):
    '''
    ifmatch = False
    for record in records:
        sname = record['name_norm']
        sartist = record['actors_name']
        if Levenshtein.jaro(song['name'],sname)>=0.9 and sartist==song['ar'][0]['name']:
            ifmatch = True
    '''
    
    if ktvdict.has_key(song['name']):
        if ktvdict[song['name']]==song['ar'][0]['name']:
            ifmatch = True
            #print song['name']
        else:
            ifmatch = False
    else:
        ifmatch= False
    return ifmatch,song
    

def get_jmake(song):
    #coll_jm = db.jmake_songs
    _r = coll_jm.find({'name_norm':{'$regex':song['name']}})
    for item in _r:
        #print item['actor']
        if item['actor']!=-1:
            if item['actor']['name_norm'] == song['artist']:
                song['actor_no'] = item['actor']['_id']
                song['serial_no'] = item['_id']
                break
    return song


def filter_jmake(_list):
    _temp = []
    for song in _list:
        song = get_jmake(song)
        #print song
        _temp.append(song)
    return _temp


def find_song(_list,_num,_temp):
    hasit = False
    for item in _list:
    	if item['id']==_temp['id']:
        	item['times']+=1
        	hasit = True
        	break
    if not hasit:
    	_num +=1
    	_list.append(_temp)

    return _list,_num



def detect_lan(lrc):
        lrc_text=' '.join(lrc)
        lang,conf=langid.classify(lrc_text)
        if lang=='zh':
                return True
        else:
                return False


def filter_zh(_list):
    for item in _list:
        if detect_lan(item['name']):
            item['proba'] *=1.5

    return _list


def get_toplist(number):
    _tpsongs = []
    r = coll_tp.find({'_id':{'$regex':'-'+str(number)}}).sort('_id',pm.DESCENDING).limit(1)
    for _toplist in r:
        for song in _toplist['result']['tracks']:
            _tpsongs.append(song)
    return _tpsongs


def cal_top():
    print 'cal the toplists of four emotions...'
    '''
    f_date = open('day_in_daynew.txt')
    _dates = []
    for item in f_date.readlines():
        item = item.strip()
        _dates.append(item)
    '''
    
    _date = time.strftime('%Y%m%d',time.localtime(time.time()))
    _dates = [_date]
    sad_num = 0
    happy_num = 0
    peaceful_num = 0
    excited_num = 0
    sad_toplist = []
    happy_toplist = []
    peaceful_toplist = []
    excited_toplist = []
    
    for _date in _dates[0:1]:
        print 'current date is:',_date
        '''
        if coll_web.find_one({'_id':_date}):
            print "already!!!!"
            continue
        '''
        usersOfday = coll.find({'_id':{'$regex':_date}},timeout=False)
        for user in usersOfday:
            #print user['_id']
            for song in user['songs']:
                _id = song['id']
                r = coll_sf.find_one({'_id':int(_id)})
                if r:
                    if len(r['type'])<=0:
                        continue

                    ifmatch ,song = match_song(song)
                    if not ifmatch:
                    	continue

                    _temp = {'id':_id,'name':song['name'],'artist':song['ar'][0]['name'],'times':1}

                    if r['type'][0]=='HAPPY':
                        _temp['proba'] = r['proba'][0][2]
                        happy_toplist ,happy_num= find_song(happy_toplist,happy_num,_temp)
                        
                    elif r['type'][0]=='SAD' :
                        _temp['proba'] = r['proba'][0][0]
                    	sad_toplist ,sad_num= find_song(sad_toplist,sad_num,_temp)

                    elif r['type'][0]=='EXCITED' :
                        _temp['proba'] = r['proba'][0][1]
                    	excited_toplist ,excited_num= find_song(excited_toplist,excited_num,_temp)

                    elif r['type'][0]=='PEACEFUL' :
                        _temp['proba'] = r['proba'][0][3]
                    	peaceful_toplist ,peaceful_num= find_song(peaceful_toplist,peaceful_num,_temp)
                       
        
    
    print "整合网易榜单..."
    _topsongs = get_toplist(0)
    for song in _topsongs:
        _id = song['id']
        r = coll_sf.find_one({'_id':int(_id)})
        if r:
            if len(r['type'])<=0:
                continue

            ifmatch ,song = match_song(song)
            if not ifmatch:
                continue
            _temp = {'id':_id,'name':song['name'],'artist':song['ar'][0]['name'],'times':1}

            if r['type'][0]=='HAPPY':
                _temp['proba'] = r['proba'][0][2]
                happy_toplist ,happy_num= find_song(happy_toplist,happy_num,_temp)
                        
            elif r['type'][0]=='SAD' :
                _temp['proba'] = r['proba'][0][0]
                sad_toplist ,sad_num= find_song(sad_toplist,sad_num,_temp)

            elif r['type'][0]=='EXCITED' :
                _temp['proba'] = r['proba'][0][1]
                excited_toplist ,excited_num= find_song(excited_toplist,excited_num,_temp)

            elif r['type'][0]=='PEACEFUL' :
                _temp['proba'] = r['proba'][0][3]
                peaceful_toplist ,peaceful_num= find_song(peaceful_toplist,peaceful_num,_temp)
        
    print "提高中文歌权重filter_ch..."
    happy_toplist = filter_zh(happy_toplist)
    sad_toplist = filter_zh(sad_toplist)
    excited_toplist = filter_zh(excited_toplist)
    peaceful_toplist = filter_zh(peaceful_toplist)
        

    print "排序..."
    happy_toplist.sort(key=lambda x:(-x['times'],x['id'])) 
    sad_toplist.sort(key=lambda x:(-x['times'],x['id']))
    excited_toplist.sort(key=lambda x:(-x['times'],x['id']))
    peaceful_toplist.sort(key=lambda x:(-x['times'],x['id']))

    print "生成榜单..."
    if len(happy_toplist)>15:
        happy_toplist = happy_toplist[0:15]
    if len(sad_toplist)>15:
        sad_toplist = sad_toplist[0:15]
    if len(excited_toplist)>15:
        excited_toplist = excited_toplist[0:15]
    if len(peaceful_toplist)>15:
        peaceful_toplist = peaceful_toplist[0:15]
    
    print "加上id"

    happy_toplist = filter_jmake(happy_toplist)
    sad_toplist = filter_jmake(sad_toplist)
    excited_toplist = filter_jmake(excited_toplist)
    peaceful_toplist = filter_jmake(peaceful_toplist)

    dataOfday = {'_id':_date,'sad_num':sad_num,'happy_num':happy_num,'peaceful_num':peaceful_num,'excited_num':excited_num,'sad_toplist':sad_toplist,'happy_toplist':happy_toplist,'peaceful_toplist':peaceful_toplist,'excited_toplist':excited_toplist}
    #print dataOfday
    coll_web.save(dataOfday)

    dataOfday2 = dataOfday['_id']+'\n' + 'happy:' + '\n'
    for item in dataOfday['happy_toplist']:
        dataOfday2 += item['name'].replace('‘','').replace("'",'')  + '-' + item['artist'] + '-' + str(item['times']) + '\n'

    dataOfday2 +=  'sad:' + '\n'
    for item in dataOfday['sad_toplist']:
        dataOfday2 += item['name'].replace('‘','').replace("'",'')   + '-' + item['artist'] + '-' + str(item['times']) +'\n'

    dataOfday2 +=  'excited:' + '\n'
    for item in dataOfday['excited_toplist']:
        dataOfday2 += item['name'].replace('‘','').replace("'",'')   + '-' + item['artist'] + '-' + str(item['times']) +'\n'

    dataOfday2 +=  'peaceful:' + '\n'
    for item in dataOfday['peaceful_toplist']:
        dataOfday2 += item['name'].replace('‘','').replace("'",'')   + '-' + item['artist'] + '-' + str(item['times']) +'\n'

    #_str = json.dumps(dataOfday2,ensure_ascii=False)
    #print dataOfday2
    send_email.alert('今日榜单推荐已生成:' + dataOfday2)
    print "保存今日推荐"


if __name__ =='__main__':
    cal_top()
    #transfer_ktvsongs()


