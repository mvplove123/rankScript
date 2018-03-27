#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2017/7/8 10:50
# @Author  : taoyongbo
# @Site    : 
# @File    : brandRankPrediction.py
# @desc    :
import csv
import multiprocessing
import os
import time

import numpy as np
import pandas as pd
from sklearn.externals import joblib

import utils

logger = utils.get_logger('brandRankPrediction')
local_featurePoiRank_path = 'D:\structure\\featurePoi\globalrank\\'

local_model_path = '/search/odin/taoyongbo/rank/brandRank_model'

model_map = {
    "lr_binguanfandian.m": "宾馆饭店",
    "lr_jinrongyinhang.m": "金融银行",
    "lr_gouwuchangsuo.m": "购物场所",
    "lr_yiliaoweisheng.m": "医疗卫生",
    "lr_xuexiaokeyan.m": "学校科研",
    "lr_xiuxianyule.m": "休闲娱乐",
    "lr_tiyuchangguan.m": "体育场馆",
    "lr_qichefuwu.m": "汽车服务",
    "lr_gongsiqiye.m": "公司企业",
    "lr_canyinfuwu.m": "餐饮服务",
    "lr_qita.m": "其它",
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
    # if y < 1.5:
    #     y = 1
    # elif y < 2.5:
    #     y = 2
    # elif y < 3.5:
    #     y = 3
    # elif y < 4.5:
    #     y = 4
    # else:
    #     y = 5


    return '%.2f' % y


def load_models(model_file_list):
    model_dict = {}
    for model_file in model_file_list:
        file_name = os.path.basename(model_file)

        current_model = joblib.load(model_file)
        if file_name in model_map:
            key = model_map.get(file_name)
            model_dict.__setitem__(key, current_model)
    return model_dict


def files_rank_cluster(local_featurePoi_path, output_path=None):
    global local_featurePoiRank_path
    if output_path:
        local_featurePoiRank_path = output_path

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
        # brand_rank_prediction(file, model_dict)
        pool.apply_async(brand_rank_prediction, (file, model_dict,))  #
    pool.close()
    pool.join()

    end_time = time.time()

    logger.info("all fileList handle finished,total time:{total_time} s".format(total_time=(end_time - begin_time)))


def convert(value):
    value_log = np.log10(value + 1)
    return value_log


def is_atm(str):
    if 'ATM' in str or '24小时自助银行' in str:
        return 0
    else:
        return 1


def feature_covert(tp):
    tp['cityCount_log'] = tp['cityCount'].apply(func=convert)
    tp['averageGrade_log'] = tp['averageGrade']  # 保持不变
    tp['averageCommentCount_log'] = tp['averageCommentCount'].apply(func=convert)
    tp['averageprice_log'] = tp['averageprice'].apply(func=convert)
    tp['categoryTotalCount_log'] = tp['categoryTotalCount'].apply(func=convert)
    tp['is_atm_sign'] = tp['brandName'].apply(func=is_atm)
    return tp


def brand_rank_prediction(file, model_dict):
    file_name = os.path.basename(file)

    try:
        logger.info("{file_name} predict".format(file_name=file_name))
        predict_begin_time = time.time()

        tp = pd.read_csv(file, sep='\t', encoding='gb18030',
                         names=['brandName', 'category', 'cityCount', 'averageGrade', 'midgrade', 'averageCommentCount',
                                'midCommentCount', 'averageprice', 'midPrice', 'categoryTotalCount', 'fiveRank',
                                'tenRank', 'twentyRank', 'fortyRank', 'fivePercentRank', 'tenPercentRank',
                                'twentyPercentRank', 'fortyPercentRank', 'hundredPercentRank'],
                         quoting=csv.QUOTE_NONE)

        parent_category = tp['category'][0]

        if not parent_category in model_dict:
            brand_rank_result = tp.loc[:, ['brandName', 'hundredPercentRank', 'categoryTotalCount']]
        else:
            tp = feature_covert(tp)
            if file_name == 'binguanfandian-feature':
                features = tp.loc[:, ['averageCommentCount_log', 'averageprice_log']]
            elif file_name == 'jinrongyinxing-feature':
                features = tp.loc[:,
                           ['cityCount_log', 'averageGrade_log', 'averageCommentCount_log', 'averageprice_log',
                            'categoryTotalCount_log', 'is_atm_sign']]
            else:
                features = tp.loc[:,
                           ['cityCount_log', 'averageGrade_log', 'averageCommentCount_log', 'averageprice_log',
                            'categoryTotalCount_log']]

            model = model_dict.get(parent_category)

            y_train_pred = model.predict(features)

            tp['y_train_pred'] = y_train_pred
            tp['pre_rank_level'] = tp['y_train_pred'].apply(func=convert_level)
            brand_rank_result = tp.loc[:, ['brandName', 'pre_rank_level', 'categoryTotalCount']]

        path = local_featurePoiRank_path + file_name + "-brandRank"
        brand_rank_result.to_csv(path, encoding='gb18030', sep='\t', index=False, header=False, quoting=csv.QUOTE_NONE)

        logger.info("{file_name} rank predict finished,used time:{use_time}".format(
            file_name=file_name, use_time=time.time() - predict_begin_time))


    except Exception as e:
        logger.info('{file_name} exception,info:{info}'.format(file_name=file_name, info=utils.print_exception()))


if __name__ == '__main__':
    files_rank_cluster('D:\sogou\\brand\\feature\\', 'D:\sogou\\brand\\output\\')
