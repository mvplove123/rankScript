#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2017/4/1 14:46
# @Author  : taoyongbo
# @Site    : 
# @File    : spark_crontab_task.py
# @desc    :
import datetime
import os
import time

import constant
import utils
from utils import get_logger

logger = get_logger('/search/odin/taoyongbo/rank/prod/python_spark/spark_crontab_task')

hadoop = '/usr/local/hadoop2.0/bin/hadoop'


def rsync_hit_count():
    hitcount_path = '/search/odin/taoyongbo/rank/rsync_data/'

    start_time = time.time()
    logger.info('rsync_hit_count start')

    del_hit_count_commond = "rm -rf /search/odin/taoyongbo/rank/rsync_data/*"
    utils.execute_command(del_hit_count_commond, shell=True)

    rsync_hit_count_commond = "scp -r root@10.153.57.134:/search/hitcount/*.txt /search/odin/taoyongbo/rank/rsync_data/"
    utils.execute_command(rsync_hit_count_commond, shell=True)
    dataid_count = {}
    fileList, dirList = utils.get_files(hitcount_path)

    filename_list = []
    for file in fileList:
        with open(file, mode='r', encoding='utf8') as hit_counts_lines:
            file_name = os.path.basename(file)
            filename_list.append(file_name)
            for line in hit_counts_lines:
                fields = line.strip().split('\t')
                if len(fields) == 3 and fields[2].isdigit():
                    dataid = fields[0]
                    count = int(fields[2])
                    if "&" not in dataid and "NULL" not in dataid:
                        dataid_count[dataid] = dataid_count[dataid] + count if dataid in dataid_count else count
        local_output_hit_count_file_path = constant.upload_local_path + '/hitcount/hitcount_' + file_name
        with open(local_output_hit_count_file_path, mode='w', encoding='gb18030') as valid_hit_counts_lines:
            for dataid, count in dataid_count.items():
                valid_hit_counts_lines.write('\t'.join((dataid, str(count))) + '\n')

        upload_hit_counts(local_output_hit_count_file_path)

    logger.info(
        'rsync_hit_count finished,files:{files_names},used_time{used_time}'.format(files_names=','.join(filename_list),
                                                                                   used_time=time.time() - start_time))


def match_count_calculate():
    start_time = time.time()
    today = datetime.date.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    previous_month = last_month.strftime("%Y_%m")
    logger.info('match_count_calculate start')

    logger.info("spark matchcount_task process:{environment}".format(environment='prod'))
    input_path = "/user/go2data_rank/taoyongbo/input/"
    output_path = "/user/go2data_rank/taoyongbo/output/"
    scala_jar_path = '/search/odin/taoyongbo/rank/beta/scala_spark/'

    commond = "/opt/spark/bin/spark-submit --master yarn --deploy-mode cluster --name MatchCountTask --class cluster.task.MatchCountTask --executor-memory 4G --num-executors 19 --executor-cores 5  --conf spark.default.parallelism=5000 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input_path + " " + output_path
    utils.execute_command(commond, shell=True)

    temp_matchCount_Filepath = constant.upload_local_path+'matchCountFile'

    check_commond = hadoop + " fs -count taoyongbo/output/newMatchCount | awk  '{print $2}' > "+temp_matchCount_Filepath
    utils.execute_command(check_commond, shell=True)

    sign = 0
    with open(temp_matchCount_Filepath, mode='r', encoding='gb18030') as match_count_line:
        for line in match_count_line:
            sign = int(line)

    if sign > 1000:
        rm_backup_matchcount_commond = hadoop + ' fs -rm -r taoyongbo/output/back_matchCount'
        utils.execute_command(rm_backup_matchcount_commond, shell=True)

        mv_backup_matchcount_commond = hadoop + ' fs -mv taoyongbo/output/matchCount taoyongbo/output/back_matchCount'
        utils.execute_command(mv_backup_matchcount_commond, shell=True)

        rename_matchcount_commond = hadoop + ' fs -mv taoyongbo/output/newMatchCount taoyongbo/output/matchCount'
        utils.execute_command(rename_matchcount_commond, shell=True)

        # check_commond = "rm -rf " + temp_matchCount_Filepath
        # utils.execute_command(check_commond, shell=True)

        logger.info('match_count_calculate success finished,used_time{used_time}'.format(
            previous_month=previous_month, used_time=time.time() - start_time))
    else:
        logger.info("spark matchcount_task failed")




def upload_hit_counts(hit_count_file):
    upload_hit_count_commond = hadoop + " fs -mkdir taoyongbo/input/newHitCount"
    utils.execute_command(upload_hit_count_commond, shell=True)

    upload_hit_count_commond = hadoop + " fs -put " + hit_count_file + " " + 'taoyongbo/input/newHitCount'
    utils.execute_command(upload_hit_count_commond, shell=True)

    rm_backup_hitcount_commond = hadoop + ' fs -rm -r taoyongbo/input/back_searchCount'
    utils.execute_command(rm_backup_hitcount_commond, shell=True)

    mv_backup_hitcount_commond = hadoop + ' fs -mv taoyongbo/input/searchCount taoyongbo/input/back_searchCount'
    utils.execute_command(mv_backup_hitcount_commond, shell=True)

    rename_matchcount_commond = hadoop + ' fs -mv taoyongbo/input/newHitCount taoyongbo/input/searchCount'
    utils.execute_command(rename_matchcount_commond, shell=True)
    logger.info("spark hitcount_task finished")


if __name__ == '__main__':
    rsync_hit_count()
    match_count_calculate()
