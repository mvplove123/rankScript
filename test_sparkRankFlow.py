#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2016/9/20 17:59
# @Author  : taoyongbo
# @Site    : 
# @File    : rankWorkFlow.py
# @desc    :
import datetime
import filecmp
import os
import re
import sys
import time

import brandRankPrediction
import constant
import poiRankCluster
import rankPrediction
import sparkTask
import splitFeatureFile
import utils

logger = utils.get_logger('test_sparkRankFlow')


def parse_excel_upload():
    """
    解析特征阀值并上传
    :return:
    """

    download_rank_config_commond = 'wget "http://svn.sogou-inc.com/svn/go2map/data/poi/edit/trunk/rank/poi-rank.xlsx"  --user=svnsogoumap --password="Helloworld2012" -O /search/odin/taoyongbo/rank/input/poi-rank.xlsx'
    utils.execute_command(download_rank_config_commond, shell=True)

    # 解析excel 生成特征阈值及权重配置文件
    parse_commond = "java  -Xms800M -Xmx2g -jar " + constant.java_jar_path + "excelparse.jar"
    utils.execute_command(parse_commond, shell=True)
    parse_excel_time = time.time()
    logger.info("parse_excel finished,used time:%s s", str(time.time() - parse_excel_time))

    utils.rm_mkdir(current_rank_version + "/config/")
    mv_config_rank_commond = "cp /search/odin/taoyongbo/rank/result/poi-threshold.txt /search/odin/taoyongbo/rank/result/poi-weight.txt " + current_rank_version + "/config/"
    utils.execute_command(mv_config_rank_commond, shell=True)

    upload_config_time = time.time()
    utils.rm_mkdir(rank_output_path, constant.cluster_sign)
    utils.rm_mkdir(rank_output_path + "/config/", constant.cluster_sign)

    # 特征阈值文件上传
    upload_threshold_commond = "hadoop fs -put " + current_rank_version + constant.poi_threshold_path + " " + rank_output_path + "/config/poi-threshold.txt"

    utils.execute_command(upload_threshold_commond, shell=True)

    # 权重文件上传
    upload_weight_commond = "hadoop fs -put " + current_rank_version + constant.weight_path + " " + rank_output_path + "/config/poiWeight.txt"
    utils.execute_command(upload_weight_commond, shell=True)

    logger.info("upload_threshold finished,used  time:%s s", str(time.time() - upload_config_time))


def feature_poi_create(environment='beta'):
    """
    特征值生产
    :return:
    """

    # 特征值数据生产
    feature_poi_create_time = time.time()
    sparkTask.xml_distcp(zeus_poi_path, zeus_myself_path, zeus_structure_path, zeus_polygon_path, rank_output_path)
    logger.info("xml_distcp finished,used time:%s s", str(time.time() - feature_poi_create_time))

    sparkTask.poi_task(environment, rank_output_path)

    sparkTask.gps_distcp(rank_output_path)
    sparkTask.gpsPopularity_task(environment, rank_output_path)

    sparkTask.featureConvert_task(environment, rank_output_path)
    logger.info("featurePoi create finished,used time:%s s", str(time.time() - feature_poi_create_time))


def download_feature_poi():
    """
    feature poi download in two ways (bycategory,by city)
    :return:
    """
    # 特征值数据下载
    download_begin_time = time.time()

    utils.rm_mkdir(current_rank_version + constant.local_featurePoi_path, constant.local_sign)
    commond = "hadoop fs -get " + rank_output_path + "/featureValue/*-feature " + current_rank_version + constant.local_featurePoi_path
    utils.execute_command(commond, shell=True)
    logger.info("featurePoi download finished,used time:%s s", str(time.time() - download_begin_time))

    # 城市特征值数据下载
    download_begin_time = time.time()
    utils.rm_mkdir(current_rank_version + constant.local_city_featurePoi_path, constant.local_sign)

    commond = "hadoop fs -get " + rank_output_path + "/cityFeatureValue/*-feature " + current_rank_version + constant.local_city_featurePoi_path
    utils.execute_command(commond, shell=True)
    logger.info("cityfeaturePoi download finished,used time:%s s", str(time.time() - download_begin_time))


def diff_version(src_file, target_file):
    if filecmp.cmp(src_file, target_file):
        return True
    else:
        return False


def rank_create():
    """
    rank 生产并备份
    :return:
    """
    # 切割文件
    splitFeatureFile.split_file(current_rank_version)

    utils.rm_mkdir(current_rank_version + constant.local_multi_path, constant.local_sign)
    utils.rm_mkdir(current_rank_version + constant.hotCount_single_rank_path, constant.local_sign)
    utils.rm_mkdir(current_rank_version + constant.hitCount_single_rank_path, constant.local_sign)

    # 多维度特征值文件聚类
    cluster_begin_time = time.time()

    rankPrediction.files_rank_cluster(current_rank_version + constant.local_split_featurePoi_path,
                                      current_rank_version + constant.local_multi_path,
                                      current_rank_version + constant.weight_path)

    logger.info("multi featurePoi cluster finished,used time:%s s", str(time.time() - cluster_begin_time))

    hotcount_cluster_begin_time = time.time()
    # 单字段特征值文件聚类
    poiRankCluster.files_rank_cluster(current_rank_version + constant.local_city_featurePoi_path,
                                      current_rank_version + constant.hotCount_single_rank_path, "single",
                                      "hotCount")
    logger.info("hotCount featurePoi cluster finished,used time:%s s", str(time.time() - hotcount_cluster_begin_time))

    hitcount_cluster_begin_time = time.time()
    poiRankCluster.files_rank_cluster(current_rank_version + constant.local_city_featurePoi_path,
                                      current_rank_version + constant.hitCount_single_rank_path, "single",
                                      "hitCount")
    logger.info("hitCount featurePoi cluster finished,used time:%s s", str(time.time() - hitcount_cluster_begin_time))

    multiRankcommond = "cat " + current_rank_version + constant.local_multi_path + "*-rank > " + current_rank_version + "/multiRank"
    utils.execute_command(multiRankcommond, shell=True)
    hotCountRankcommond = "cat " + current_rank_version + constant.hotCount_single_rank_path + "*_rank > " + current_rank_version + "/hotCountRank"
    utils.execute_command(hotCountRankcommond, shell=True)
    hitCountRankcommond = "cat " + current_rank_version + constant.hitCount_single_rank_path + "*_rank > " + current_rank_version + "/hitCountRank"
    utils.execute_command(hitCountRankcommond, shell=True)


def rank_combine_upload(environment):
    """
    rank数据合并上传
    :return:
    """
    rank_combine_upload_time = time.time()

    rm_rank_commond = "hadoop fs -mkdir " + rank_output_path + "/hitCountRank/ " + rank_output_path + "/hotCountRank/ " + rank_output_path + "/multiRank/"
    utils.execute_command(rm_rank_commond, shell=True)

    upload_multiRank_commond = "hadoop fs -put " + current_rank_version + "/multiRank " + rank_output_path + "/multiRank/"
    utils.execute_command(upload_multiRank_commond, shell=True)

    upload_hotCountRank_commond = "hadoop fs -put " + current_rank_version + "/hotCountRank " + rank_output_path + "/hotCountRank/"
    utils.execute_command(upload_hotCountRank_commond, shell=True)

    upload_hitCountRank_commond = "hadoop fs -put " + current_rank_version + "/hitCountRank " + rank_output_path + "/hitCountRank/"
    utils.execute_command(upload_hitCountRank_commond, shell=True)

    # rank数据整合
    sparkTask.rankCombine_task(environment, rank_output_path)

    logger.info("rank rank_combine_upload finished,used time:%s s", str(time.time() - rank_combine_upload_time))


def brand_rank_create(environment):
    brand_rank_create_time = time.time()
    sparkTask.brandFeature_task(environment, rank_output_path)
    utils.rm_mkdir(current_rank_version + constant.local_brandfeaturePoi_path, constant.local_sign)
    commond = "hadoop fs -get " + rank_output_path + "/brandFeatureValue/*-feature " + current_rank_version + constant.local_brandfeaturePoi_path
    utils.execute_command(commond, shell=True)

    utils.rm_mkdir(current_rank_version + constant.brand_rank_path, constant.local_sign)
    # brand predict
    brandRankPrediction.files_rank_cluster(current_rank_version + constant.local_brandfeaturePoi_path,
                                           current_rank_version + constant.brand_rank_path)

    upload_brand_rank_commond = "hadoop fs -put " + current_rank_version + constant.brand_rank_path + " " + rank_output_path
    utils.execute_command(upload_brand_rank_commond, shell=True)

    logger.info("brand rank create and upload finished,used time:%s s", str(time.time() - brand_rank_create_time))


def rank_optimization(environment):
    rank_optimization_time = time.time()
    sparkTask.rank_optimize_task(environment, rank_output_path)
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


def structure_rank_create(environment):
    """
    结构化rank创建映射，并优化rank
    :param environment: 
    :return: 
    """
    structure_rank_create_time = time.time()
    sparkTask.structureMapRank_task(environment, rank_output_path)
    utils.rm_mkdir(current_rank_version + "/result", constant.local_sign)

    # 结构化rank下载
    commond = "hadoop fs -text " + rank_output_path + "/structureMapRank/part* > " + current_rank_version + constant.local_structure_rank_path
    utils.execute_command(commond, shell=True)

    # 结构化rank 优化
    parse_commond = "java -Xms4096M -Xmx7096M -jar " + constant.java_jar_path + "structure-optimize-1.0-SNAPSHOT.jar " + current_rank_version + constant.local_structure_rank_path + " " + current_rank_version + constant.local_structure_optimize_path
    utils.execute_command(parse_commond, shell=True)

    # 结构化rank上传
    utils.rm_mkdir(rank_output_path + "/structureOptimizeRank/", constant.cluster_sign)

    upload_rank_structure_status_commond = "hadoop fs -put " + current_rank_version + constant.local_structure_optimize_path + " " + rank_output_path + "/structureOptimizeRank/"
    utils.execute_command(upload_rank_structure_status_commond, shell=True)

    logger.info("spark structure_rank_create finished,used time:%s s", str(time.time() - structure_rank_create_time))


def main(environment='beta'):
    rank_begin_time = time.time()
    logger.info("rank work flow begin")
    time_version = datetime.datetime.now().strftime('%Y%m%d_%H:%M')
    global zeus_poi_path
    global zeus_myself_path
    global zeus_structure_path
    global zeus_polygon_path
    global rank_output_path
    global current_rank_version

    logger.info("current environment:" + environment)
    logger.info("zeus_poi_path:" + zeus_poi_path)
    logger.info("zeus_myself_path:" + zeus_myself_path)
    logger.info("zeus_structure_path:" + zeus_structure_path)
    logger.info("zeus_polygon_path:" + zeus_polygon_path)
    logger.info("rank_output_path:" + rank_output_path)

    current_rank_version = constant.rank_version_path +time_version #"20171218_16:57/"
    # utils.rm_mkdir(current_rank_version,constant.local_sign)
    # parse_excel_upload()
    feature_poi_create(environment)
    # download_feature_poi()
    # rank_create()
    # rank_combine_upload(environment)
    # brand_rank_create(environment)
    # structure_rank_create(environment)
    # rank_optimization(environment)
    # rank_task_finish_sign()
    # back_rank(environment)
    logger.info("rank work flow finished,total time:{time}s,environment:{environment}".format(
        time=str(time.time() - rank_begin_time), environment=environment))


def kill_app():
    cmd = "yarn application -list | awk -F ' ' '{print $1,$4}'"
    output = utils.get_shell_output(cmd)
    kill_cmd = "yarn application -kill "

    for line in output:
        field = line.strip().split(" ")
        appId = field[0]
        user = field[1]
        if user == "go2data_rank":
            kill_app = kill_cmd + appId
            utils.execute_command(kill_app)
            logger.info("application:{app} kill done".format(app=appId))


if __name__ == '__main__':
    utils.del_dir(constant.rank_version_path)
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

    logger.info("rank work flow input all filepath{paths}".format(paths=input))

    input_paths = input.split(",")
    zeus_poi_path = '/user/go2data/base_data/pub_data/milestone/20171206/nochange/POI'
    zeus_myself_path = '/user/go2data/base_data/pub_data/milestone/20171206/temp/extends/surrounding/result'
    zeus_structure_path = '/user/go2data/base_data/pub_data/milestone/20171206/temp/extends/struct/structure/name_prefix_structure_release'
    zeus_polygon_path = '/user/go2data/base_data/pub_data/milestone/20171206/nochange/POLYGON'
    rank_output_path = input_paths[0]
    main(environment=environment)
    sys.exit(0)
