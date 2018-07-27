#-*- coding: utf-8 -*-
import warnings
from snownlp import SnowNLP
from snownlp import sentiment
import os
import json
import pandas as pd
import numpy as np
import time
import multiprocessing
import time

warnings.filterwarnings('ignore')

# 遍历指定目录，显示目录下的所有文件名
def eachFile(filepath):
    files = []
    pathDir =  os.listdir(filepath)
    for allDir in pathDir:
        child = os.path.join('%s%s%s' % (filepath, '\\', allDir))
        files.append(child)
    return files

# 读取文件内容并打印
def readFile(filenames):
    lst = []
    for filename in filenames:
        with open(filename, encoding='utf-8') as f:  # 将json文件转化为字典
            for line in f.readlines():
                dic=json.loads(line)
                lst.append(dic)
    return lst

def weibo_lst_to_df(lst):
    lst_commentCount, lst_praiseCount, lst_reportCount, lst_source, lst_userId, lst_weiboId, lst_weiboUrl, lst_content \
        = [], [], [], [], [], [], [], []
    df = pd.DataFrame()
    for line in lst:
        line = dict(line[0])
        commentCount, praiseCount, reportCount, source, userId, weiboId, weiboUrl, content = line['commentCount'], \
                line['praiseCount'], line['reportCount'], line['source'], line['userId'], line['weiboId'], \
                line['weiboUrl'], line['content']
        lst_praiseCount.append(praiseCount)
        lst_commentCount.append(commentCount)
        lst_reportCount.append(reportCount)
        lst_source.append(source)
        lst_userId.append(userId)
        lst_weiboId.append(weiboId)
        lst_weiboUrl.append(weiboUrl)
        lst_content.append(content)
    dic = {'commentCount': lst_commentCount, 'reportCount': lst_reportCount, 'praiseCount': lst_praiseCount, \
           'source': lst_source, 'userId': lst_userId, 'weiboId': lst_weiboId, 'weiboUrl': lst_weiboUrl, \
           'content': lst_content}
    df = pd.DataFrame(dic)
    return df

def _sentiment_dealing(args):
    df, i = args
    sen_score = []
    print ('dealing: ' + str(i))
    try:
        df.loc[i,'score'] = (SnowNLP(df.loc[i, 'content'])).sentiments
    except:
        pass
    sen_score = df.loc[i, 'score']
    return sen_score

def sentiment_dealing(df):
    aList = [(df, i) for i in range(len(df))]
    p = multiprocessing.Pool(PROC_NUM)
    bList = p.map(_sentiment_dealing, aList)
    print (bList)
    #df['score'] = bList
    #return df
    return bList

if __name__ == '__main__':
    time_start = time.time()
    filepathC = r"C:\Users\destefano\PycharmProjects\WeiboRelationship\weibo2"
    filenames = eachFile(filepathC)
    weibo_list = readFile(filenames)
    df = weibo_lst_to_df(weibo_list)
    print ('dealing line numbers: '+str(len(df)))
    time.sleep(3)
    lst = []
    PROC_NUM = 4
    num = 500
    div = int(len(df)/num)
    dffir = df[:len(df)-div*num]
    lst = sentiment_dealing(dffir)
    dffir['score'] = lst
    for j in range(div):
        dfchunk = df[len(df)-div*num+j*num:len(df)-div*num+(j+1)*num]
        lst = sentiment_dealing(dfchunk)
        dfchunk['score'] = lst
        dffir = pd.concat([dffir, dfchunk])
    #df = sentiment_dealing(df)
    df = dffir
    df['tag'] = ['中性'] * len(df)
    df.loc[(df['score'] > 0.66), ['tag']] = ['积极'] * len(df[(df['score'] > 0.66)])
    df.loc[(df['score'] < 0.33), ['tag']] = ['消极'] * len(df[(df['score'] < 0.33)])
    time_end = time.time()
    df.to_csv('WeiboEmotion.csv', encoding='utf-8')
    print('totally cost', time_end - time_start)




