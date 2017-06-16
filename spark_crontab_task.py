#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2017/4/1 14:46
# @Author  : taoyongbo
# @Site    : 
# @File    : spark_crontab_task.py
# @desc    :
import datetime
import time
from collections import Counter

import constant
import utils
from sparkTask import matchcount_task
from utils import get_logger

logger = get_logger('/search/odin/taoyongbo/rank/beta/python_spark/spark_crontab_task')


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
    flag =False
    try:
        matchcount_task('beta')
    except Exception as e:
        print(e)
    match_count_calculate_commond = 'echo ' + previous_month + ' > ' + constant.rsync_version_path + 'remote_match_count_version'
    utils.execute_command(match_count_calculate_commond, shell=True)

    logger.info('previous_month {previous_month}:match_count_calculate finished,used_time{used_time}'.format(
        previous_month=previous_month, used_time=time.time() - start_time))


if __name__ == '__main__':
    # rsync_hit_count()
    match_count_calculate()
