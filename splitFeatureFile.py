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


logger = utils.get_logger('split_file')


def split_file(current_rank_version):
    """
    切分文件至临时文件夹
    :return: 
    """
    os.mkdir(current_rank_version+constant.local_split_featurePoi_path)

    feature_file_list, feature_dirList = utils.get_files(current_rank_version+constant.local_featurePoi_path)
    for file in feature_file_list:
        file_name = os.path.basename(file)
        commond = 'split -l 500000 ' + file + ' ' + current_rank_version+constant.local_split_featurePoi_path + file_name + '_'
        utils.execute_command(commond)
        logger.info('{file_name} split finished'.format(file_name=file_name))
    logger.info('all files split finished,output:{output}'.format(output=current_rank_version+constant.local_split_featurePoi_path))


if __name__ == '__main__':
    split_file()
