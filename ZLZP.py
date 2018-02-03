import requests
from time import sleep,ctime
from pyquery import PyQuery as pq
from urllib.parse import urlencode
from requests.exceptions import RequestException
from multiprocessing import Process,Lock
from config import *

def get_index_page(page):
    data = {
        'jl': '选择地区',
        'kw': '数据分析师',
        'sm': '0',
        'sg': 'c39ae80f7be54f39b8f8a0bca180c91a',
        'p': page
    }
    url = 'http://sou.zhaopin.com/jobs/searchresult.ashx?' + urlencode(data)
    try:
        response = requests.get(url,headers=headers)
        if response.status_code == 200:
            # print('第%s页网站访问成功'%page)
            response_text = response.text
            response.close()
            return response_text
        else:
            return get_index_page(page)
    except RequestException:
        print('网站请求失败'+url+',正在重新进行访问')
        return get_index_page(page)

def parse_index_page(lock,html,page):
    try:
        doc = pq(html)
        tables = doc('.main .search_newlist_main .newlist_main #newlist_list_content_table .newlist').items()
        if tables:
            for table in tables:
                url = table.find('.zwmc div a:first-child').attr.href
                # print(url)
                if url:
                    info_html=get_info_url(url)
                    information = parse_info_page(info_html)
                    if information:
                    # print(infomation)
                        for key in columnslist:
                            info = information.get(key,0)
                            write_to_txt(lock,info,key)
        # print(page)
        current_Q = float(page)/90 *100
        print('第%(a)s页爬取成功,当前进度%(b).2f'%{'a':str(page),'b':current_Q}+'%')
                    # set_enter()
    except Exception:
        print('爬取第%s页失败,正在重新爬取'%page)
        parse_index_page(lock,html,page)


def get_info_url(url):
    try:
        response = requests.get(url,headers=headers)
        if response.status_code == 200:
            response_text = response.text
            response.close()
            return response_text
        else:
            return get_info_url(url)
    except RequestException:
        print('职位信息请求失败',url,',正在重新进行访问')
        return get_info_url(url)

def parse_info_page(html):
    try:
        doc = pq(html)
        lis = doc('.terminalpage.clearfix .terminal-ul.clearfix li').items()
        information= {}
        for li in lis:
            information[li('span').text()[:-1]] = li.find('strong').text()
        return information
    except Exception:
        parse_info_page(html)

def write_to_txt(lock,content,key):
    lock.acquire()
    try:
        with open(path,'a',encoding='UTF-8') as f:
            if key.strip() !='公司行业':
                f.write(str(content)+',')
            else:
                f.write(str(content)+'\n')
            f.close()
    finally:
        lock.release()

def set_columns():
    with open(path,'a',encoding='UTF-8') as f:
        for key in columnslist:
            if key.strip() != '公司行业':
                f.write(key+',')
            else:
                f.write(key+'\n')
        f.close()

def main(lock,page):
        html=get_index_page(page)
        parse_index_page(lock,html,page)

if __name__=='__main__':
    with open(path,'w+') as f:
        f.truncate()
    state_time = ctime()
    print('程序启动时间：', state_time)
    set_columns()
    lock = Lock()
    for page in range(1,91):
        process=Process(target=main,args=(lock,page))
        process.start()
        process.join()
        sleep(1)
    end_time = ctime()
    print('数据已经下载完成,时间：',end_time)