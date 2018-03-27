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


# 创建logger
def get_logger(log_file_name):
    # 创建一个logger
    logger = logging.getLogger(log_file_name)
    logger.setLevel(logging.DEBUG)
    # 创建一个handler，用于写入日志文件
    fh = logging.FileHandler(log_file_name + ".log")
    fh.setLevel(logging.DEBUG)
    # 再创建一个handler，用于输出到控制台
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # 定义handler的输出格式
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # 给logger添加handler
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


execute_logger = get_logger('sfv_monitor')
my_user = 'mvplove123@163.com'  # 收件人邮箱账号，我这边发送给自己
my_sender = 'mvplove123@163.com'  # 发件人邮箱账号
my_pass = 'tyb278867066'  # 发件人邮箱密码
has_info = '<b><br> <a href="https://onlineservices.immigration.govt.nz/?SF">登陆链接</a></p><br><a href="https://www.immigration.govt.nz/new-zealand-visas/apply-for-a-visa/visa-factsheet/silver-fern-job-search-work-visa">抢名额链接</a><br><img src="cid:image1">'
no_info = '名额已抢完'
sfv_title = '新西兰sfv签证出现'
monitor_title = '新西兰签证监控'
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
    """通过页面url获取其对应的html内容 
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
    taglist = soup.find_all('h2', attrs={'class': 'banner_title'})  # 获取标签为img，其中一个属性：class="BDE_Image" 所有数据，放进集合

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

        server = smtplib.SMTP_SSL("smtp.163.com", 465)  # 发件人邮箱中的SMTP服务器，端口是25
        server.login(my_sender, my_pass)  # 括号中对应的是发件人邮箱账号、邮箱密码
        # msg = MIMEText('填写邮件内容', 'plain', 'utf-8')
        msgRoot = MIMEMultipart('related')
        msgRoot['Subject'] = title  # 邮件的主题，也可以说是标题

        msg = MIMEText(content, 'html', 'utf-8')
        msgRoot.attach(msg)

        # fp = open(img, 'rb')
        # msgImage = MIMEImage(fp.read())
        # fp.close()
        # msgImage.add_header('Content-ID', '<image1>')
        # msgRoot.attach(msgImage)

        msgRoot['From'] = formataddr(["mvplove123", my_sender])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
        msgRoot['To'] = formataddr(["mvplove123", my_user])  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        server.sendmail(my_sender, [my_user, ], msgRoot.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
        server.quit()  # 关闭连接


    except Exception as e:  # 如果 try 中的语句没有执行，则会执行下面的 ret=False
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
