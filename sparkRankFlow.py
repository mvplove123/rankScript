#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2016/9/20 17:59
# @Author  : taoyongbo
# @Site    : 
# @File    : rankWorkFlow.py
# @desc    :
import datetime
import os
import shutil
import sys
import time
import re
import constant
import poiRankCluster
import sparkTask
import utils
import filecmp
from rankCluster import multi_rank, single_rank

logger = utils.get_logger('sparkRankFlow')


def parse_excel_upload():
    """
    解析特征阀值并上传
    :return:
    """

    # 解析excel 生成特征阈值及权重配置文件
    parse_commond = "java  -Xms800M -Xmx2g -jar " + constant.java_jar_path + "excelparse.jar"
    utils.execute_command(parse_commond, shell=True)
    parse_excel_time = time.time()
    logger.info("parse_excel finished,used time:%s s", str(time.time() - parse_excel_time))

    upload_config_time = time.time()

    # 特征阈值文件上传
    rm_threshold_commond = "hadoop fs -rm /user/go2data_rank/taoyongbo/input/featureThreshold/*"
    utils.execute_command(rm_threshold_commond, shell=True)
    upload_threshold_commond = "hadoop fs -put /search/odin/taoyongbo/rank/result/poi-threshold.txt /user/go2data_rank/taoyongbo/input/featureThreshold/"
    utils.execute_command(upload_threshold_commond, shell=True)

    # 权重文件上传
    rm_weight_commond = "hadoop fs -rm /user/go2data_rank/taoyongbo/input/poiWeight/*"
    utils.execute_command(rm_weight_commond, shell=True)
    upload_weight_commond = "hadoop fs -put /search/odin/taoyongbo/rank/result/poi-weight.txt /user/go2data_rank/taoyongbo/input/poiWeight/"
    utils.execute_command(upload_weight_commond, shell=True)

    logger.info("upload_threshold finished,used time:%s s", str(time.time() - upload_config_time))


def feature_poi_create(environment='beta'):
    """
    特征值生产
    :return:
    """
    global zeus_poi_path
    global zeus_buspoi_path
    global zeus_myself_path
    global zeus_structure_path
    global zeus_polygon_path

    logger.info("zeus_poi_path:" + zeus_poi_path)
    logger.info("zeus_buspoi_path:" + zeus_buspoi_path)
    logger.info("zeus_myself_path:" + zeus_myself_path)
    logger.info("zeus_structure_path:" + zeus_structure_path)
    logger.info("zeus_polygon_path:" + zeus_polygon_path)

    # 特征值数据生产
    feature_poi_create_time = time.time()
    sparkTask.xml_distcp(zeus_poi_path, zeus_buspoi_path, zeus_myself_path, zeus_structure_path, zeus_polygon_path)
    logger.info("xml_distcp finished,used time:%s s", str(time.time() - feature_poi_create_time))

    sparkTask.poi_task(environment)
    # gps discp
    if not diff_version(constant.rsync_version_path + 'remote_gps_version',
                        constant.rsync_version_path + 'spark_gps_version'):
        sparkTask.gps_distcp()
        sparkTask.gpsPopularity_task(environment)

        update_version_commond = "cat " + constant.rsync_version_path + 'remote_gps_version > ' + constant.rsync_version_path + 'spark_gps_version'
        utils.execute_command(update_version_commond, shell=True)

    # hitcount upload
    if not diff_version(constant.rsync_version_path + 'remote_hitcount_version',
                        constant.rsync_version_path + 'spark_hitcount_version'):
        upload_hit_counts()
        update_version_commond = "cat " + constant.rsync_version_path + 'remote_hitcount_version > ' + constant.rsync_version_path + 'spark_hitcount_version'
        utils.execute_command(update_version_commond, shell=True)

    # match_count sync
    if not diff_version(constant.rsync_version_path + 'remote_match_count_version',
                        constant.rsync_version_path + 'spark_match_count_version'):
        sparkTask.matchCount_distcp()
        update_version_commond = "cat " + constant.rsync_version_path + 'remote_match_count_version > ' + constant.rsync_version_path + 'spark_match_count_version'
        utils.execute_command(update_version_commond, shell=True)

    sparkTask.featureConvert_task(environment)
    logger.info("featurePoi create finished,used time:%s s", str(time.time() - feature_poi_create_time))


def download_feature_poi():
    """
    feature poi download in two ways (bycategory,by city)
    :return:
    """
    # 特征值数据下载
    download_begin_time = time.time()
    commond = "rm -rf " + constant.local_featurePoi_path + "*"
    utils.execute_command(commond, shell=True)
    commond = "hadoop fs -get /user/go2data_rank/taoyongbo/output/featureValue/*-feature " + constant.local_featurePoi_path
    utils.execute_command(commond, shell=True)
    logger.info("featurePoi download finished,used time:%s s", str(time.time() - download_begin_time))

    # 城市特征值数据下载
    download_begin_time = time.time()
    commond = "rm -rf " + constant.local_city_featurePoi_path + "*"
    utils.execute_command(commond, shell=True)
    commond = "hadoop fs -get /user/go2data_rank/taoyongbo/output/cityFeatureValue/*-feature " + constant.local_city_featurePoi_path
    utils.execute_command(commond, shell=True)
    logger.info("cityfeaturePoi download finished,used time:%s s", str(time.time() - download_begin_time))


def diff_version(src_file, target_file):
    if filecmp.cmp(src_file, target_file):
        return True
    else:
        return False


def upload_hit_counts():
    rm_hit_count_commond = "hadoop fs -rm -r taoyongbo/input/searchCount/*"
    utils.execute_command(rm_hit_count_commond, shell=True)

    upload_hit_count_commond = "hadoop fs -put " + constant.upload_local_path + 'hitcounts' + " " + constant.yarn_searchCount_input_path
    utils.execute_command(upload_hit_count_commond, shell=True)


def rank_create():
    """
    rank 生产并备份
    :return:
    """

    # 本地rank数据清空
    commond = "rm -rf " + constant.rank_path + "*"
    utils.execute_command(commond, shell=True)
    rm_rank_time = time.time()
    logger.info("rank rm finished,used time:%s s", str(time.time() - rm_rank_time))
    #
    # 创建城市文件夹

    multi_rank_path = constant.rank_path + "multi/"
    single_rank_path = constant.rank_path + "single/"

    if os.path.exists(multi_rank_path):
        shutil.rmtree(multi_rank_path)
    os.mkdir(multi_rank_path)
    if os.path.exists(single_rank_path):
        shutil.rmtree(single_rank_path)
    os.mkdir(single_rank_path)

    hotCount_single_rank_path = single_rank_path + "hotCount/"
    hitCount_single_rank_path = single_rank_path + "hitCount/"
    os.mkdir(hotCount_single_rank_path)
    os.mkdir(hitCount_single_rank_path)

    # 多维度特征值文件聚类
    cluster_begin_time = time.time()
    poiRankCluster.files_rank_cluster(constant.local_featurePoi_path, multi_rank_path, "multi")
    logger.info("multi featurePoi cluster finished,used time:%s s", str(time.time() - cluster_begin_time))

    hotcount_cluster_begin_time = time.time()
    # 单字段特征值文件聚类
    poiRankCluster.files_rank_cluster(constant.local_city_featurePoi_path, hotCount_single_rank_path, "single",
                                      "hotCount")
    logger.info("hotCount featurePoi cluster finished,used time:%s s", str(time.time() - hotcount_cluster_begin_time))

    hitcount_cluster_begin_time = time.time()
    poiRankCluster.files_rank_cluster(constant.local_city_featurePoi_path, hitCount_single_rank_path, "single",
                                      "hitCount")
    logger.info("hitCount featurePoi cluster finished,used time:%s s", str(time.time() - hitcount_cluster_begin_time))

    multiRankcommond = "cat " + multi_rank_path + "*-rank > " + constant.rank_path + "multiRank"
    utils.execute_command(multiRankcommond, shell=True)
    hotCountRankcommond = "cat " + hotCount_single_rank_path + "*_rank > " + constant.rank_path + "hotCountRank"
    utils.execute_command(hotCountRankcommond, shell=True)
    hitCountRankcommond = "cat " + hitCount_single_rank_path + "*_rank > " + constant.rank_path + "hitCountRank"
    utils.execute_command(hitCountRankcommond, shell=True)


def rank_combine_upload(environment):
    """
    rank数据合并上传
    :return:
    """
    rank_combine_upload_time = time.time()

    rm_rank_commond = "hadoop fs -rm -r taoyongbo/output/hitCountRank/* taoyongbo/output/hotCountRank/* taoyongbo/output/multiRank/*"
    utils.execute_command(rm_rank_commond, shell=True)

    upload_multiRank_commond = "hadoop fs -put " + constant.rank_path + "multiRank taoyongbo/output/multiRank/"
    utils.execute_command(upload_multiRank_commond, shell=True)

    upload_hotCountRank_commond = "hadoop fs -put " + constant.rank_path + "hotCountRank taoyongbo/output/hotCountRank/"
    utils.execute_command(upload_hotCountRank_commond, shell=True)

    upload_hitCountRank_commond = "hadoop fs -put " + constant.rank_path + "hitCountRank taoyongbo/output/hitCountRank/"
    utils.execute_command(upload_hitCountRank_commond, shell=True)

    # rank数据整合
    sparkTask.rankCombine_task(environment)

    logger.info("rank rank_combine_upload finished,used time:%s s", str(time.time() - rank_combine_upload_time))


def rank_cluster():
    multi_rank_time = time.time()

    logger.info("spark multi_rank begin:")

    rm_multiRank_commond = "hadoop fs -rmr  taoyongbo/output/multiRank/*"
    utils.execute_command(rm_multiRank_commond, shell=True)
    multi_rank()
    logger.info("spark multi_rank finished,used time:%s s", str(time.time() - multi_rank_time))

    single_rank_time = time.time()
    logger.info("sp ark single_rank begin:")
    single_rank()
    logger.info("spark single_rank finished,used time:%s s", str(time.time() - single_rank_time))


def brand_rank_create(environment):
    brand_rank_create_time = time.time()
    sparkTask.brandRank_task(environment)
    logger.info("spark brand_rank_create finished,used time:%s s", str(time.time() - brand_rank_create_time))


def rank_optimization(environment):
    rank_optimization_time = time.time()
    sparkTask.rank_optimize_task(environment)
    logger.info("spark rank_optimization finished,used time:%s s", str(time.time() - rank_optimization_time))


def rank_task_finish_sign():
    rank_task_finish_sign_time = time.time()
    # 上传时间戳
    rank_combine_status_commond = "echo  `date` > " + constant.rank_path + 'rankCombine'
    utils.execute_command(rank_combine_status_commond, shell=True)
    rm_rank_combine_status_commond = "hadoop fs -rm -r taoyongbo/output/result/rankCombine"
    utils.execute_command(rm_rank_combine_status_commond, shell=True)
    upload_rank_combine_status_commond = "hadoop fs -put " + constant.rank_path + "rankCombine taoyongbo/output/result"
    utils.execute_command(upload_rank_combine_status_commond, shell=True)
    logger.info("spark rank_task_finish_sign finished,used time:%s s", str(time.time() - rank_task_finish_sign_time))


def back_rank(environment):
    download_begin_time = time.time()
    now = environment+'_'+datetime.datetime.now().strftime('%Y%m%d_%H:%M')
    current_back_rank_path = constant.back_rank_path + now
    if os.path.exists(current_back_rank_path):
        shutil.rmtree(current_back_rank_path)
    os.mkdir(current_back_rank_path)

    commond = "hadoop fs -get /user/go2data_rank/taoyongbo/output/multiOptimizeRank/*-rank " + current_back_rank_path
    utils.execute_command(commond, shell=True)
    logger.info("rank backup finished,used time:%s s", str(time.time() - download_begin_time))


def main(environment='beta'):
    rank_begin_time = time.time()
    logger.info("rank work flow begin")
    parse_excel_upload()
    feature_poi_create(environment)
    download_feature_poi()
    rank_create()
    rank_combine_upload(environment)
    brand_rank_create(environment)
    rank_optimization(environment)
    rank_task_finish_sign()
    back_rank(environment)
    logger.info("rank work flow finished,total time:{time}s,environment:{environment}".format(time=str(time.time() - rank_begin_time),environment=environment) )


if __name__ == '__main__':

    pattern = '.*/rank/(.*)/python_spark.*'

    path_str = ','.join(sys.path)
    flag = re.match(pattern=pattern, string=path_str)
    if flag:
        environment = flag.group(1)
    else:
        environment = 'beta'

    if len(sys.argv) < 1:
        logger.warning('No action specified.')
        sys.exit(1)

    input = sys.argv[1]
    input_paths = input.split(",")
    zeus_poi_path = input_paths[0]
    zeus_buspoi_path = input_paths[1]
    zeus_myself_path = input_paths[2]
    zeus_structure_path = input_paths[3]
    zeus_polygon_path = input_paths[4]
    main(environment=environment)
    sys.exit(0)
