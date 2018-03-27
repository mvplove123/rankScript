#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2017/7/8 10:50
# @Author  : taoyongbo
# @Site    : 
# @File    : rankPrediction.py
# @desc    :
import csv
import multiprocessing
import os
import time

import numpy as np
import pandas as pd
from sklearn.externals import joblib

import utils

logger = utils.get_logger('rankPrediction')
local_weight_path = "/search/odin/taoyongbo/rank/result/poi-weight.txt"
local_featurePoiRank_path = 'D:\structure\\featurePoi\globalrank\\'

local_model_path = '/search/odin/taoyongbo/rank/rank_model/'

model_map = {
    "binguanfandian.m": "宾馆饭店",
    "jinrongyinxing.m": "金融银行",
    "gouwuchangsuo.m": "购物场所",
    "zhengfujiguan.m": "政府机关",
    "youzhengdianxin.m": "邮政电信",
    "yiliaoweisheng.m": "医疗卫生",
    "xuexiaokeyan.m": "学校科研",
    "xiuxianyule.m": "休闲娱乐",
    "xinwenmeiti.m": "新闻媒体",
    "tiyuchangguan.m": "体育场馆",
    "qichefuwu.m": "汽车服务",
    "jiaotongchuxing.m": "交通出行",
    "gongsiqiye.m": "公司企业",
    "diming.m": "地名",
    "canyinfuwu.m": "餐饮服务",
    "changguanhuisuo.m": "场馆会所",
    "fangdichan.m": "房地产",
    "qita.m": "其它",
    "lvyoujingdian.m": "旅游景点"
}


def get_model_map(file_name):
    if file_name in model_map:
        return model_map.get(file_name)
    return None


def convert_level(y):
    """
    format rank level
    :param y: 
    :return: 
    """
    if y < 1.5:
        y = 1
    elif y < 2.5:
        y = 2
    elif y < 3.5:
        y = 3
    elif y < 4.5:
        y = 4
    else:
        y = 5
    return y


weight = pd.read_csv(local_weight_path, sep='\t',
                     names=['parentCategory', 'subcategory', 'subCategoryScore', 'tagScore', 'matchCountScore',
                            'gradeScore',
                            'commentScore', 'priceScore', 'areaScore', 'leafCountScore', 'doorCountScore',
                            'parkCountScore', 'innerCountScore',
                            'buildCountScore'], quoting=csv.QUOTE_NONE, encoding='gb18030')


def load_models(model_file_list):
    model_dict = {}
    for model_file in model_file_list:
        file_name = os.path.basename(model_file)

        current_model = joblib.load(model_file)
        if file_name in model_map:
            key = model_map.get(file_name)
            model_dict.__setitem__(key, current_model)
    return model_dict


def files_rank_cluster(local_featurePoi_path, output_path=None, weight_path=None):
    global local_featurePoiRank_path
    global local_weight_path
    if output_path:
        local_featurePoiRank_path = output_path
    if weight_path:
        local_weight_path = weight_path

    logger.info(local_featurePoiRank_path)
    begin_time = time.time()
    logger.info("all fileList handle process")

    model_file_list, model_dirList = utils.get_files(local_model_path)

    # load model
    model_dict = load_models(model_file_list)

    # 读取文件
    fileList, dirList = utils.get_files(local_featurePoi_path)

    logger.info('total file:{num}'.format(num=len(fileList)))
    pool = multiprocessing.Pool(processes=5)

    for file in fileList:
        # rank_prediction(file, model_dict)
        pool.apply_async(rank_prediction, (file, model_dict,))  #
    pool.close()
    pool.join()

    end_time = time.time()

    logger.info("total time:" + str(end_time - begin_time) + "s")
    logger.info("all fileList handle finished")


def rank_prediction(file, model_dict):
    file_name = os.path.basename(file)

    try:
        # global local_featurePoiRank_path
        logger.info("{file_name} predict".format(file_name=file_name))
        tp = pd.read_csv(file, sep='\t', encoding='gb18030',
                                 names=['name', 'dataId', 'city', 'parentCategory', 'subCategory', 'brand',
                                        'categoryScore', 'tagScore', 'matchCountScore', 'gradeScore',
                                        'commentScore', 'priceScore', 'areaScore', 'leafCountScore', 'doorCountScore',
                                        'parkCountScore', 'innerCountScore', 'buildCountScore',
                                        'point', 'keyword', 'matchCount', 'grade', 'comment', 'price', 'area',
                                        'leafCount', 'doorCount', 'parkCount', 'innerCount', 'buildCount',
                                        'hotCount', 'hitcount', 'viewcount', 'citySize'],
                                 quoting=csv.QUOTE_NONE, iterator=True, chunksize=1000)

        featurePoi = pd.concat(tp, ignore_index=True)


        parentCategory = featurePoi['parentCategory'][0]

        if not parentCategory in model_dict:
            return

        model = model_dict.get(parentCategory)
        begin_time = time.time()
        W = weight.loc[weight.parentCategory == parentCategory, 'subCategoryScore':]
        X = featurePoi.iloc[:, 6:18]

        featureValues = np.multiply(X, W)

        featurePoi.loc[:, ('sumWeight1')] = featureValues.sum(axis=1)

        y_train_pred = model.predict(featurePoi.loc[:, ['sumWeight1', 'citySize']])

        featurePoi['y_train_pred'] = y_train_pred
        featurePoi['pre_rank_level'] = featurePoi['y_train_pred'].apply(func=convert_level)

        path = local_featurePoiRank_path + file_name + "-rank"
        featurePoi.to_csv(path, encoding='gb18030', sep='\t', index=False, header=False,quoting=csv.QUOTE_NONE)

        logger.info("{file_name} rank predict finished,used time:{use_time}".format(
            file_name=file_name, use_time=time.time() - begin_time))


    except Exception as e:
        logger.info('{file_name} exception,info:{info}'.format(file_name=file_name,info=utils.print_exception()))

if __name__ == '__main__':
    files_rank_cluster('/search/odin/taoyongbo/rank/tempfeaturePoi/', '/search/odin/taoyongbo/rank/guiyang_rank/')

