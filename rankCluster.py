#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2016/11/8 14:58
# @Author  : taoyongbo
# @Site    : 
# @File    : diaoyong.py
# @desc    :
import multiprocessing
import time

import constant
import utils

input_data = "taoyongbo/output/cityFeatureValue/"
output = "/user/go2data_rank/taoyongbo/output/"
from constant import jar_path

fileList, dirList = utils.get_files(constant.lib_path)
libjars = ",".join(fileList)

logger = utils.get_logger('rankCluster')


def multi_rank():
    pool = multiprocessing.Pool(processes=10)
    for city in constant.global_city_list:
        pool.apply_async(runMultiRank, (city,))
    pool.close()
    pool.join()


def single_rank():
    pool = multiprocessing.Pool(processes=2)

    for i in range(1, 3):
        if i == 1:
            pool.apply_async(runHotRank)
        if i == 2:
            pool.apply_async(runHitRank)
    pool.close()
    pool.join()


def runMultiRank(city):
    process_time = time.time()
    input = city + "-feature"
    commond = "spark-submit --master yarn --deploy-mode cluster --name " + city + "-PoiRankTask --class cluster.task.PoiRankTask --jars " + libjars + " --executor-memory 5G --num-executors 2 --executor-cores 5 --driver-memory 1G --driver-cores 1 --conf spark.default.parallelism=1 --conf spark.storage.memoryFraction=0.3 --conf spark.shuffle.memoryFraction=0.5  --conf spark.shuffle.consolidateFiles=true " + jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    end_time = time.time()
    logger.info("%s poirank_task finished, used time:%s s", city, str(end_time - process_time))


def runHotRank():
    process_time = time.time()
    commond = "spark-submit --master yarn --deploy-mode cluster --name HotCountRankTask --class cluster.task.HotCountRankTask --jars " + libjars + " --executor-memory 11G --num-executors 10 --executor-cores 5 --driver-memory 1G --driver-cores 1 --conf spark.default.parallelism=1 --conf spark.storage.memoryFraction=0.3 --conf spark.shuffle.memoryFraction=0.5 " + jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input_data + " " + output
    utils.execute_command(commond, shell=True)
    end_time = time.time()
    logger.info("poirank_task finished, used time:%s s", str(end_time - process_time))


def runHitRank():
    process_time = time.time()
    commond = "spark-submit --master yarn --deploy-mode cluster --name HitCountRankTask --class cluster.task.HitCountRankTask --jars " + libjars + " --executor-memory 11G --num-executors 10 --executor-cores 5 --driver-memory 1G --driver-cores 1 --conf spark.default.parallelism=1 --conf spark.storage.memoryFraction=0.3 --conf spark.shuffle.memoryFraction=0.5 " + jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input_data + " " + output
    utils.execute_command(commond, shell=True)
    end_time = time.time()
    logger.info("poirank_task finished, used time:%s s", str(end_time - process_time))


if __name__ == '__main__':
    multi_rank_time = time.time()

    logger.info("spark multi_rank begin:")

    rm_multiRank_commond = "hadoop fs -rmr  taoyongbo/output/multiRank/*"
    utils.execute_command(rm_multiRank_commond, shell=True)
    multi_rank()
    logger.info("spark multi_rank finished,used time:%s s", str(time.time() - multi_rank_time))

    single_rank_time = time.time()
    logger.info("spark single_rank begin:")
    single_rank()
    logger.info("spark single_rank finished,used time:%s s", str(time.time() - single_rank_time))
