#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2017/4/1 14:46
# @Author  : taoyongbo
# @Site    : 
# @File    : spark_crontab_task.py
# @desc    :
import datetime
import time

import constant
import utils
from utils import get_logger

logger = get_logger('/search/odin/taoyongbo/rank/beta/python_spark/spark_crontab_task')

hadoop = '/usr/local/hadoop2.0/bin/hadoop'

def rsync_hit_count():
    start_time = time.time()
    today = datetime.date.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    previous_month = last_month.strftime("%Y_%m")
    logger.info('previous_month {previous_month}:rsync_hit_count start'.format(previous_month=previous_month))

    rsync_hit_count_commond = "rsync -vzrtopg --progress -e ssh --delete root@10.153.57.134:/search/hitcount/* /search/odin/taoyongbo/rank/rsync_data/hitcounts"
    utils.execute_command(rsync_hit_count_commond, shell=True)
    dataid_count = {}

    with open('/search/odin/taoyongbo/rank/rsync_data/hitcounts', mode='r', encoding='utf8') as hit_counts_lines:
        for line in hit_counts_lines:
            fields = line.strip().split('\t')
            if len(fields) == 3 and fields[2].isdigit():
                dataid = fields[0]
                count = int(fields[2])
                if "&" not in dataid and "NULL" not in dataid:
                    dataid_count[dataid] = dataid_count[dataid] + count if dataid in dataid_count else count
    with open(constant.upload_local_path + 'hitcounts', mode='w',
              encoding='gb18030') as valid_hit_counts_lines:
        for dataid, count in dataid_count.items():
            valid_hit_counts_lines.write('\t'.join((dataid, str(count))) + '\n')

    hit_count_version_commond = 'echo ' + previous_month + ' > ' + constant.rsync_version_path + 'remote_hitcount_version'
    utils.execute_command(hit_count_version_commond, shell=True)

    logger.info('previous_month {previous_month}:rsync_hit_count finished,used_time{used_time}'.format(
        previous_month=previous_month, used_time=time.time() - start_time))


def match_count_calculate():
    start_time = time.time()
    today = datetime.date.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    previous_month = last_month.strftime("%Y_%m")
    logger.info('previous_month {previous_month}:match_count_calculate start'.format(previous_month=previous_month))

    logger.info("spark matchcount_task process:{environment}".format(environment='beta'))
    input_path = "/user/go2data_rank/taoyongbo/input/"
    output_path = "/user/go2data_rank/taoyongbo/output/"
    scala_jar_path = '/search/odin/taoyongbo/rank/beta/scala_spark/'

    commond = "/opt/spark/bin/spark-submit --master yarn --deploy-mode cluster --name MatchCountTask --class cluster.task.MatchCountTask --executor-memory 4G --num-executors 19 --executor-cores 5  --conf spark.default.parallelism=3000 " + scala_jar_path + "poi-rank-1.0-SNAPSHOT.jar  " + input_path + " " + output_path
    utils.execute_command(commond, shell=True)

    rm_backup_matchcount_commond = hadoop+' fs -rm -r taoyongbo/output/back_matchCount'
    utils.execute_command(rm_backup_matchcount_commond, shell=True)

    mv_backup_matchcount_commond = hadoop+' fs -mv taoyongbo/output/matchCount taoyongbo/output/back_matchCount'
    utils.execute_command(mv_backup_matchcount_commond, shell=True)

    rename_matchcount_commond = hadoop+' fs -mv taoyongbo/output/newMatchCount taoyongbo/output/matchCount'
    utils.execute_command(rename_matchcount_commond, shell=True)
    logger.info("spark matchcount_task finished")

    logger.info('previous_month {previous_month}:match_count_calculate finished,used_time{used_time}'.format(
        previous_month=previous_month, used_time=time.time() - start_time))


match_count_calculate()
rsync_hit_count()
