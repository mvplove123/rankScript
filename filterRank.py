#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2017/9/28 17:28
# @Author  : taoyongbo
# @Site    : 
# @File    : async.py
# @desc    :
import re
import sys

import utils
from constant import root_path
import sparkTask

input = "/user/go2data_rank/taoyongbo/input/"
output = "/user/go2data_rank/taoyongbo/output/"
logger = utils.get_logger('filterRank')


def GpsCustomStatisticTask(environment='beta'):
    logger.info("spark GpsCustomStatisticTask process:{environment}".format(environment=environment))
    scala_jar_path = root_path + environment + '/scala_spark/'
    scala_libjars_path = scala_jar_path + 'lib'
    fileList, dirList = utils.get_files(scala_libjars_path)
    libjars = ",".join(fileList)
    commond = "spark-submit --master yarn --deploy-mode cluster --name GpsCustomStatisticTask --class cluster.tempTask.GpsCustomStatisticTask --jars " + libjars + " --executor-memory 4G --num-executors 19 --executor-cores 5 --conf spark.default.parallelism=5000 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input + " " + output
    utils.execute_command(commond, shell=True)
    logger.info("spark GpsCustomStatisticTask finished")
    sys.exit(0)


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
    filter_poi_input_path = input_paths[0]
    zeus_similarQueryCount_path = input_paths[1]
    filter_poi_output_path = input_paths[2]

    logger.info("filterRank work flow begin")
    logger.info("filter_poi_input_path:" + ','.join(input_paths[:-1]))
    logger.info("filter_poi_output_path:" + filter_poi_output_path)
    sparkTask.filter_source_distcp(zeus_filterPoi_input_path=filter_poi_input_path,
                                   zeus_similarQueryCount_path=zeus_similarQueryCount_path,
                                   rank_output_path=filter_poi_output_path)
    sparkTask.gpsCustomStatistic_task(rank_output_path=filter_poi_output_path, environment=environment)
    logger.info("filterRank work flow end")
