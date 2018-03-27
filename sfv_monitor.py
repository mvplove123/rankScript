#!/usr/bin/env python
# -*- coding: gb18030 -*-
# @Time    : 2017/12/21 17:40
# @Author  : taoyongbo
# @Site    : 
# @File    : sendEmail.py
# @desc    :
import linecache
import logging
import smtplib
import sys
import time
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr
from urllib import request
import pyscreenshot as ImageGrab
import requests.packages.urllib3.util.ssl_
from bs4 import BeautifulSoup
from retrying import retry
from selenium import webdriver

requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL'


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


execute_logger = get_logger('sfv_monitor')
my_user = 'mvplove123@163.com'  # �ռ��������˺ţ�����߷��͸��Լ�
my_sender = 'mvplove123@163.com'  # �����������˺�
my_pass = 'tyb278867066'  # ��������������
has_info = '<b><br> <a href="https://onlineservices.immigration.govt.nz/?SF">��½����</a></p><br><a href="https://www.immigration.govt.nz/new-zealand-visas/apply-for-a-visa/visa-factsheet/silver-fern-job-search-work-visa">����������</a><br><img src="cid:image1">'
no_info = '����������'
sfv_title = '������sfvǩ֤����'
monitor_title = '������ǩ֤���'
has_info_img = ''
no_info_img = ''
last_time = time.time()
current_count = 1
error_count = 0


def image_screen_shot():
    im = ImageGrab.grab()

    # save image file
    im.save('screenshot.png')

    # show image in a window
    im.show()


    # driver = webdriver.Chrome()
    # driver.maximize_window()
    # driver.implicitly_wait(6)
    # driver.get("https://www.baidu.com")
    # time.sleep(1)
    #
    # driver.get_screenshot_as_file("D:\\sogou\\sfv\\baidu.png")
    # driver.quit()


def retry_if_result_false(result):
    """Return True if we should retry (in this case when result is None), False otherwise"""
    return result is False


def print_exception():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)

    exception = 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)
    return exception


# @retry(stop_max_attempt_number=100, stop_max_delay=10000, wait_random_min=1000, wait_random_max=2000,
#      retry_on_result=retry_if_result_none)
def getHtml(url):
    """ͨ��ҳ��url��ȡ���Ӧ��html���� 
    """
    try:
        response = request.urlopen(url=url, timeout=10)
        page = response.read()
        html = page.decode('utf-8')
        return html
    except Exception as e:
        execute_logger.info('url request exception,info:{info}'.format(info=print_exception()))
        return ''


def parse(html):
    # open("D:\\sogou\\HTML")
    soup = BeautifulSoup(html, "lxml")
    taglist = soup.find_all('h2', attrs={'class': 'banner_title'})  # ��ȡ��ǩΪimg������һ�����ԣ�class="BDE_Image" �������ݣ��Ž�����

    contents = [tag.string for tag in taglist]

    if 'CLOSED' not in contents:
        global last_time
        current_time = time.time()
        interval = current_time - last_time
        if interval > 300:
            ret = mail(title=sfv_title, content=has_info, img=has_info_img)
            last_time = current_time
        else:
            ret = False
        if ret:
            execute_logger.info('Email send successful')
        else:
            execute_logger.info('Email send frequently')


def check_time(rate):
    monitor_info = 'the {current_count} time monitor excute finished,success rate:{rate}'.format(
        current_count=current_count,
        rate=rate)
    today = datetime.datetime.now()
    time_format = today.strftime('%H:%M')
    if time_format == "15:40":
        ret = mail(title=monitor_title, content=monitor_info, img=has_info_img)
        if ret:
            execute_logger.info('Monitor Email send successful')
        else:
            execute_logger.info('Monitor Email send unsuccessful')
        time.sleep(60)


@retry(stop_max_attempt_number=3, retry_on_result=retry_if_result_false)
def mail(title, content, img):
    ret = True
    try:

        server = smtplib.SMTP_SSL("smtp.163.com", 465)  # �����������е�SMTP���������˿���25
        server.login(my_sender, my_pass)  # �����ж�Ӧ���Ƿ����������˺š���������
        # msg = MIMEText('��д�ʼ�����', 'plain', 'utf-8')
        msgRoot = MIMEMultipart('related')
        msgRoot['Subject'] = title  # �ʼ������⣬Ҳ����˵�Ǳ���

        msg = MIMEText(content, 'html', 'utf-8')
        msgRoot.attach(msg)

        # fp = open(img, 'rb')
        # msgImage = MIMEImage(fp.read())
        # fp.close()
        # msgImage.add_header('Content-ID', '<image1>')
        # msgRoot.attach(msgImage)

        msgRoot['From'] = formataddr(["mvplove123", my_sender])  # ������Ķ�Ӧ�����������ǳơ������������˺�
        msgRoot['To'] = formataddr(["mvplove123", my_user])  # ������Ķ�Ӧ�ռ��������ǳơ��ռ��������˺�
        server.sendmail(my_sender, [my_user, ], msgRoot.as_string())  # �����ж�Ӧ���Ƿ����������˺š��ռ��������˺š������ʼ�
        server.quit()  # �ر�����


    except Exception as e:  # ��� try �е����û��ִ�У����ִ������� ret=False
        execute_logger.info('Email exception,info:{info}'.format(info=print_exception()))
        ret = False
    return ret


def execuse():
    url = 'https://www.immigration.govt.nz/new-zealand-visas/apply-for-a-visa/visa-factsheet/silver-fern-job-search-work-visa'
    html = getHtml(url)
    if html == '':
        return 1
    parse(html)
    return 0


if __name__ == '__main__':

    while True:
        try:
            result_code = execuse()
            if result_code == 1:
                info = 'failure'
                error_count += 1
            else:
                info = 'success'
            rate = '%.2f%%' % (float(current_count - error_count) / float(current_count) * 100)

            info = 'the {current_count} time monitor excute finished,status:{status},success rate:{rate}'.format(
                current_count=current_count,
                status=info, rate=rate)
            execute_logger.info(info)
            if result_code == 1:
                time.sleep(1)

            check_time(rate)

            current_count += 1
        except Exception as e:
            execute_logger.info(print_exception())
