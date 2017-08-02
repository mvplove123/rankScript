#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2017/7/11 18:20
# @Author  : taoyongbo
# @Site    : 
# @File    : splitFeatureFile.py
# @desc    :
import os

import shutil

import constant
import utils

local_model_path = ' /search/odin/taoyongbo/rank/tempfeaturePoi/'

logger = utils.get_logger('split_file')


def split_file():
    """
    切分文件至临时文件夹
    :return: 
    """
    if os.path.exists(constant.local_split_featurePoi_path):
        shutil.rmtree(constant.local_split_featurePoi_path)
    os.mkdir(constant.local_split_featurePoi_path)

    feature_file_list, feature_dirList = utils.get_files(constant.local_featurePoi_path)
    for file in feature_file_list:
        file_name = os.path.basename(file)
        commond = 'split -l 500000 ' + file + ' ' + constant.local_split_featurePoi_path + file_name + '_'
        utils.execute_command(commond)
        logger.info('{file_name} split finished'.format(file_name=file_name))
    logger.info('all files split finished,output:{output}'.format(output=constant.local_split_featurePoi_path))


if __name__ == '__main__':
    split_file()
