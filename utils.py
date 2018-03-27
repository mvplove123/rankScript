# encoding=gb18030
import datetime
import linecache
import logging
import os
import shlex
import subprocess
import time

# ��ȡĿ¼�µ������ļ��к��ļ�
import sys

import shutil

import constant


def get_files(path):
    global allFileNum

    # �����ļ��У���һ���ֶ��Ǵ�Ŀ¼�ļ���
    dirList = []
    # �����ļ�
    fileList = []
    # ����һ���б����а�����Ŀ¼��Ŀ������(google����)
    files = os.listdir(path)
    for f in files:
        if (os.path.isdir(path + '/' + f)):
            # �ų������ļ��С���Ϊ�����ļ��й���
            if (f[0] == '.'):
                pass
            else:
                # ��ӷ������ļ���
                dirList.append(f)
        if (os.path.isfile(path + '/' + f)):
            # ����ļ�
            fileList.append(path + '/' + f)
    # ��һ����־ʹ�ã��ļ����б��һ�����𲻴�ӡ
    i_dl = 0
    for dl in dirList:
        if (i_dl == 0):
            i_dl = i_dl + 1
    return fileList, dirList
    # for fl in fileList:
    #     # ��ӡ�ļ�
    #     print '-' * (int(dirList[0])), fl
    #     # ������һ���ж��ٸ��ļ�
    #     allFileNum = allFileNum + 1


# ����logger
def get_logger(log_file_name):
    # ����һ��logger
    logger = logging.getLogger(log_file_name)
    logger.setLevel(logging.DEBUG)
    # ����һ��handler������д����־�ļ�
    fh = logging.FileHandler(log_file_name + ".log")
    fh.setLevel(logging.DEBUG)
    # �ٴ���һ��handler���������������̨
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # ����handler�������ʽ
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # ��logger���handler
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ִ�нű�����
def execute_command(cmdstring, cwd=None, timeout=None, shell=False):
    """ִ��һ��SHELL����
            ��װ��subprocess��Popen����, ֧�ֳ�ʱ�жϣ�֧�ֶ�ȡstdout��stderr
           ����:
        cwd: ��������ʱ����·����������趨���ӽ��̻�ֱ���ȸ��ĵ�ǰ·����cwd
        timeout: ��ʱʱ�䣬�룬֧��С��������0.1��
        shell: �Ƿ�ͨ��shell����
    Returns: return_code
    Raises:  Exception: ִ�г�ʱ
    """
    if shell:
        cmdstring_list = cmdstring
    else:
        cmdstring_list = shlex.split(cmdstring)
    if timeout:
        end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)

    # û��ָ����׼����ʹ�������Ĺܵ�����˻��ӡ����Ļ�ϣ�
    sub = subprocess.Popen(cmdstring_list, cwd=cwd, stdout=None, shell=shell, bufsize=1)

    # subprocess.poll()����������ӽ����Ƿ�����ˣ���������ˣ��趨�������룬����subprocess.returncode������
    while sub.poll() is None:
        time.sleep(0.1)
        if timeout:
            if end_time <= datetime.datetime.now():
                raise Exception("Timeout��%s" % cmdstring)
    return str(sub.returncode)


def print_exception():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)

    exception = 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)
    print(exception)
    return exception


def get_shell_output(cmd):
    process = os.popen(cmd)  # return file
    output = process.readlines()
    return output


def del_dir(path):
    cmd = "ls -lt " + path + " | awk '{print $9}'"
    infos = get_shell_output(cmd)
    dir_names = []
    for info in infos:
        if info and '_' in info:
            fields = info.split('_')
            dir_names.append(int(fields[0]))
    dir_names.sort(reverse=True)
    dir_paths = [path + str(name) + "*" for name in dir_names[3:]]
    rm_dir = 'rm -rf ' + ' '.join(dir_paths)
    execute_command(rm_dir, shell=True)


def rm_mkdir(path, source):
    if source == constant.local_sign:
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path)
    else:
        rm_commond = "hadoop fs -rm -r " + path
        execute_command(rm_commond, shell=True)
        mkdir_commond = "hadoop fs -mkdir " + path
        execute_command(mkdir_commond, shell=True)
