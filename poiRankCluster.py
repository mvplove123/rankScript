#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2016/8/10 14:44
# @Author  : taoyongbo
# @Site    : 
# @File    : poiRankCluster.py
# @desc    : poiRank 聚类 用python 实现
import csv
import multiprocessing
import os
import sys
import time

import numpy as np
import pandas as pd
from pandas import DataFrame
from scipy.spatial.distance import cdist
from sklearn import metrics
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.utils import shuffle

import constant
import utils

logger = utils.get_logger('poiRankCluster')

# local_featurePoi_path = "D:\structure\\featurePoi\\"
# local_weight_path = "D:\structure\\poi-weight.txt"
local_featurePoiRank_path = "D:\structure\\rank\\"

local_weight_path = "/search/odin/taoyongbo/rank/result/poi-weight.txt"
# local_featurePoiRank_path = "/search/odin/taoyongbo/rank/poiRank_hotcountrank/"

weight = pd.read_csv(local_weight_path, sep='\t',
                     names=['parentCategory', 'subcategory', 'subCategoryScore', 'tagScore', 'matchCountScore',
                            'gradeScore',
                            'commentScore', 'priceScore', 'areaScore', 'leafCountScore', 'doorCountScore',
                            'parkCountScore', 'innerCountScore',
                            'buildCountScore'], quoting=csv.QUOTE_NONE, encoding='gb18030')


def files_rank_cluster(local_featurePoi_path, output_path=None, cluster_type="multi", column=None):
    global local_featurePoiRank_path

    if not output_path == "":
        local_featurePoiRank_path = output_path

    begin_time = time.time()
    logger.info("all fileList handle process")
    # 读取文件
    fileList, dirList = utils.get_files(local_featurePoi_path)

    logger.info('total file:{num}'.format(num=len(fileList)))
    pool = multiprocessing.Pool(processes=10)

    for file in fileList:
        if cluster_type == "multi":
            pool.apply_async(rank_cluster, (file,))  # 多维度聚类
        else:
            pool.apply_async(rank_cluster_singleColumn, (file, column))  # 单字段聚类
    pool.close()
    pool.join()

    end_time = time.time()

    logger.info(u"完成用时" + str(end_time - begin_time) + "s")
    logger.info("all fileList handle finished")


# 多维度特征值聚类
def rank_cluster(file):
    file_name = os.path.basename(file)

    try:

        featurePoi = pd.read_csv(file, sep='\t',
                                 names=['name', 'dataId', 'city', 'parentCategory', 'subCategory', 'brand',
                                        'categoryScore', 'tagScore', 'matchCountScore', 'gradeScore',
                                        'commentScore', 'priceScore', 'areaScore', 'leafCountScore', 'doorCountScore',
                                        'parkCountScore', 'innerCountScore', 'buildCountScore',
                                        'point', 'keyword', 'matchCount', 'grade', 'comment', 'price', 'area',
                                        'leafCount', 'doorCount', 'parkCount', 'innerCount', 'buildCount',
                                        'hotCount', 'hitcount', 'viewcount', 'sumViewOrder'], quoting=csv.QUOTE_NONE,
                                 encoding='gb18030')

        featurePoi = featurePoi.sample(frac=1).reset_index(drop=True)
        parentCategory = featurePoi['parentCategory'][0]
        begin_time = time.time()
        W = weight.ix[weight.parentCategory == parentCategory, 2:]
        X = featurePoi.ix[:, 6:18]

        featureValues = np.multiply(X, W)

        featurePoi.loc[:, ('sumWeight')] = featureValues.sum(axis=1)

        cluster_k, rank_k = get_k(file_name, featureValues)
        kmeans_model = KMeans(n_clusters=cluster_k, init='k-means++',n_jobs=1,n_init=100,max_iter=500,tol=1e-5).fit(featureValues)
        featurePoi.loc[:, ('label')] = kmeans_model.labels_
        df = DataFrame(kmeans_model.cluster_centers_, columns=['subCategoryScore', 'realKeyword', 'commentNum', 'price',
                                                               'grade', 'matchCount', 'area', 'childNum', 'doorNum',
                                                               'parkNum', 'internalSceneryNum', 'buildNum'])
        df['sum'] = df.sum(axis=1)
        if rank_k == 1:
            kmeans_model1 = KMeans(n_clusters=rank_k, init='k-means++').fit(np.array(df.ix[:, ['sum']]))
        else:
            kmeans_model1 = AgglomerativeClustering(n_clusters=rank_k, linkage='complete').fit(
                np.array(df.ix[:, ['sum']]))

        df['memLabel'] = kmeans_model1.labels_

        sort_df = df.sort_values(by=['sum'], axis=0, ascending=False)

        rank = rank_k + 1
        sort_df['rank'] = 0
        rank_map = {}
        tmp_label = 100

        for index, row in sort_df.iterrows():
            label = sort_df.loc[index, ('memLabel')]
            if rank_map != 100 and tmp_label != label:
                rank = rank - 1
            tmp_label = label
            sort_df.loc[index, ('rank')] = rank

        sort_df.loc[:, ('label')] = sort_df.index

        df_merged = pd.merge(featurePoi, sort_df.ix[:, 14:], on='label')

        end_time = time.time()

        logger.info('{file_name}用时:{use_time}s'.format(file_name=file_name, use_time=end_time - begin_time))
        sort_df.to_csv(constant.local_featurePoi_center_path+file_name+'-rank-center', encoding='gb18030', sep='\t', index=False, header=True)
        path = local_featurePoiRank_path + file_name + "-rank"
        df_merged.to_csv(path, encoding='gb18030', sep='\t', index=False, header=False)
    except Exception as e:
        logger.info(file_name + "exception1:")
        logger.info(e)


# 单字段聚类
def rank_cluster_singleColumn(file, column):
    try:
        file_name = os.path.basename(file)
        begin_time = time.time()

        featurePoi = pd.read_csv(file, sep='\t',
                                 names=['name', 'dataId', 'city', 'parentCategory', 'subCategory', 'brand',
                                        'categoryScore', 'tagScore', 'matchCountScore', 'gradeScore',
                                        'commentScore', 'priceScore', 'areaScore', 'leafCountScore', 'doorCountScore',
                                        'parkCountScore', 'innerCountScore', 'buildCountScore',
                                        'point', 'keyword', 'matchCount', 'grade', 'comment', 'price', 'area',
                                        'leafCount', 'doorCount', 'parkCount', 'innerCount', 'buildCount',
                                        'hotCount', 'hitCount', 'viewCount', 'sumViewOrder'], quoting=csv.QUOTE_NONE,
                                 encoding='gb18030')

        column_label = column + '_label'
        column_rank = column + '_rank'

        featurePoi[column] = featurePoi[column].replace(0, 1)

        feature_column = np.array(featurePoi.ix[:, [column]])

        featureSumWeight = np.log10(feature_column)

        # 获取聚类k，及rank k
        cluster_k, rank_k = get_single_comlun_k(file_name, featureSumWeight)

        if cluster_k <= rank_k:
            rank_k = cluster_k

        kmeans_model = KMeans(n_clusters=cluster_k, init='k-means++', random_state=0).fit(featureSumWeight)
        featurePoi.loc[:, (column_label)] = kmeans_model.labels_

        df = DataFrame(kmeans_model.cluster_centers_, columns=['center_mean_weight'])

        if rank_k == 1:
            kmeans_model1 = KMeans(n_clusters=rank_k, init='k-means++').fit(
                np.array(df.ix[:, df.columns == ['center_mean_weight']]))
        else:
            kmeans_model1 = AgglomerativeClustering(n_clusters=rank_k, linkage='complete').fit(
                np.array(df.ix[:, df.columns == ['center_mean_weight']]))

        df['mean_memLabel'] = kmeans_model1.labels_

        sort_df = df.sort_values(by=['center_mean_weight'], axis=0, ascending=False)

        rank = rank_k + 1
        sort_df[column_rank] = 0
        rank_map = {}
        tmp_label = 100

        for index, row in sort_df.iterrows():
            label = sort_df.loc[index, ('mean_memLabel')]
            if rank_map != 100 and tmp_label != label:
                rank = rank - 1
            tmp_label = label
            sort_df.loc[index, (column_rank)] = rank

        sort_df.loc[:, (column_label)] = sort_df.index

        feature_info = featurePoi.loc[:, ['dataId', column_label]]

        df_merged = pd.merge(feature_info, sort_df.ix[:, 2:], on=column_label)

        end_time = time.time()

        logger.info('{file_name}用时:{use_time}s'.format(file_name=file_name, use_time=end_time - begin_time))

        path = local_featurePoiRank_path + file_name + "-" + column_rank
        df_merged.to_csv(path, encoding='gb18030', sep='\t', index=False, header=False)
    except Exception as e:
        logger.info(e)

        logger.info(file_name + "singleException2:")


# 获取聚类k数，及rank k数
def get_k(file_name, featureValues):
    try:
        # 筛选样本个数
        size = len(featureValues)
        rank_k = 5

        if size > 2000:
            shuffle_sample = shuffle(featureValues, random_state=0, n_samples=2000)
            cluster_k = 20  # 指定

        elif rank_k < size <= 2000:
            shuffle_sample = featureValues
            cluster_k = autoGetK(file_name, shuffle_sample)

        elif 0 < size <= rank_k:
            cluster_k = size
            rank_k = size
        else:
            return

        logger.info('{file_name} cluster_k:{cluster_k}'.format(file_name=file_name, cluster_k=cluster_k))

        return cluster_k, rank_k
    except Exception as e:
        logger.info(e)

        logger.info(file_name + "exception3:")


# 获取聚类k数，及rank k数
def get_single_comlun_k(file_name, featureValues):
    try:
        # 筛选样本个数
        size = np.unique(featureValues).size
        rank_k = 10  # 确定业务上rank最大值

        if size > 2000:
            # shuffle_sample = shuffle(featureValues, random_state=0, n_samples=2000)
            cluster_k = 20  # 指定

        elif rank_k <= size <= 2000:
            shuffle_sample = featureValues
            cluster_k = 10

        elif 0 < size < rank_k:
            cluster_k = size
            rank_k = size
        else:
            return

        logger.info(file_name + " cluster_k:" + str(cluster_k))
        return cluster_k, rank_k
    except Exception as e:
        logger.info(e)

        logger.info(file_name + "singleException3:")


# 获取k
def autoGetK(file_name, shuffle_sample):
    try:
        K = [5, 6, 8, 10, 12, 14, 16, 18, 20]

        lkxs = {}
        meandistortions = []

        for t in K:
            if len(shuffle_sample) > t:
                kmeans_model = KMeans(n_clusters=t).fit(shuffle_sample)
                meandistortions.append(
                    sum(np.min(cdist(shuffle_sample, kmeans_model.cluster_centers_, 'euclidean'), axis=1)) / len(
                        shuffle_sample))

                # print 'K = %s, 轮廓系数 = %.03f' % (t, metrics.silhouette_score(shuffle_sample, kmeans_model.labels_, metric='euclidean'))
                lkxs[t] = metrics.silhouette_score(shuffle_sample, kmeans_model.labels_, metric='euclidean')

        sort_lkxs = sorted(lkxs.items(), key=lambda d: d[1], reverse=True)
        cluster_k = sort_lkxs[0][0]
        return cluster_k
    except Exception as e:
        return 5
        logger.info(file_name + "exception4:autoGetK")
        logger.info(e)


def main():
    # 多线程分类聚类
    # files_rank_cluster(constant.local_city_featurePoi_path, local_featurePoiRank_path, cluster_type="single",
    #                    column="hotCount")

    files_rank_cluster('D:\structure\\featurePoi', 'D:\\structure\\rank')

    sys.exit()


if __name__ == '__main__':
    main()
