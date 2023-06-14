# -*- coding = utf-8 -*-
# @Time :2023/2/8 17:14
# @Author : yds
# @File : jobSpider.py
# @Software : PyCharm

# 导包
from time import sleep

from selenium import webdriver
from selenium.webdriver import ChromeOptions
from lxml import html
import json
import pymysql

etree = html.etree


# 得到每一页的json数据，并存储到json文件里,返回json文件目录
def get_Page_Json_Data(url):
    # ------------ 规避检测 ------------
    # 实例化对象
    option = ChromeOptions()
    option.add_experimental_option('excludeSwitches', ['enable-automation'])  # 开启实验性功能
    # 去除特征值
    option.add_argument("--disable-blink-features=AutomationControlled")
    # 实例化谷歌
    driver = webdriver.Chrome(options=option)
    # 获取页面源码
    driver.get(url)
    sleep(2)
    page_text = driver.page_source
    # 解析
    html = etree.HTML(page_text)
    page_json_data = html.xpath('//pre/text()')[0]
    fileName = './jsondata/' + url.split("=")[-1] + '.json'
    with open(fileName, 'w', encoding='utf-8') as fp:
        fp.write(page_json_data)
    return fileName


# 处理json数据，返回一个json文件里的所有工作信息（二维列表）
def processing_data(jsonPath):
    fp = open(jsonPath, 'r', encoding='utf-8')
    data = fp.read()
    json_data = json.loads(data, strict=False)
    job_json_list = json_data["data"]["list"]
    # 所有职位信息的列表
    all_job_list = []

    for each in job_json_list:
        # 存放每一个职位的信息
        job = []
        # jobInfo信息
        # 职位id
        jobId = each["jobId"]
        job.append(jobId)
        # 职位名称
        jobName = each["jobName"]
        job.append(jobName)
        # 薪资
        lowMonthPay = int(each["lowMonthPay"])
        MonthPay = lowMonthPay * 1000
        job.append(MonthPay)
        # 学历要求
        degreeName = each["degreeName"]
        job.append(degreeName)
        # 学历
        major = each["major"]
        job.append(major)
        # 福利
        recTags = each["recTags"]
        job.append(recTags)
        # 招聘人数
        headCount = each["headCount"]
        job.append(headCount)

        # compInfo信息
        # 公司id
        recId = each["recId"]
        job.append(recId)
        # 公司名称
        recName = each["recName"]
        job.append(recName)
        # 地区
        areaCodeName = each["areaCodeName"]
        job.append(areaCodeName)
        # 公司规模
        recScale = each["recScale"]
        job.append(recScale)
        # 公司性质
        recProperty = each["recProperty"]
        job.append(recProperty)

        # 把处理好的一个职位的信息存储到all_job_list中
        all_job_list.append(job)
    return all_job_list


# 存储数据
def savData(all_job_list, conn):
    # 添加数据
    # 创建游标对象
    cursor = conn.cursor()
    for jobs1 in all_job_list:
        com_sql = "insert into compinfo(recId,recName,areaCodeName,recScale,recProperty) values('%s','%s','%s','%s','%s')" % (
            jobs1[7], jobs1[8], jobs1[9], jobs1[10], jobs1[11])
        try:
            # 执行数据数据
            cursor.execute(com_sql)
            # 提交
            conn.commit()
            print("数据插入成功")
        except pymysql.Error as e:
            conn.rollback()
            print(e)
    cursor.close()
    cursor = conn.cursor()
    for jobs2 in all_job_list:
        job_sql = "insert into jobinfo(jobId,jobName,MonthPay,degreeName,major,recTags,headCount,recId,recName) " \
                  "values('%s','%s','%s','%s','%s','%s','%d','%s','%s')" % (
                      jobs2[0], jobs2[1], jobs2[2], jobs2[3], jobs2[4], jobs2[5], jobs2[6], jobs2[7], jobs2[8])
        try:
            # 执行数据数据
            cursor.execute(job_sql)
            # 提交
            conn.commit()
            print("数据插入成功")
        except pymysql.Error as e:
            conn.rollback()
            print(e)
    cursor.close()


if __name__ == '__main__':
    baseurl = "https://www.ncss.cn/student/jobs/jobslist/ajax/?limit=10&offset="

    # 连接数据库
    dbHost = 'localhost'
    dbUser = 'root'
    dbPsw = '123456'
    dbName = 'db_flaskdemo'
    try:
        conn = pymysql.connect(host=dbHost, user=dbUser, password=dbPsw, database=dbName)
        print("数据库连接成功")
    except pymysql.Error as e:
        print("数据库连接失败" + str(e))

    # 处理数据，并存入数据库
    fileName_list = []
    for i in range(1, 15):
        url = baseurl + str(i)
        fileName_list.append(get_Page_Json_Data(url))
    for fileName in fileName_list:
        all_job_list = processing_data(fileName)
        savData(all_job_list, conn)

    # 关闭数据库
    conn.close()
