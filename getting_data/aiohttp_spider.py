# -*- coding: utf-8 -*-
import asyncio
import re
import aiohttp
import aiomysql
import json
import requests
from pyquery import PyQuery
# pyquery, beautiful soup , xpath

waiting_urls = {
    "stk_ipo_audit": [],
    "stk_ipo_information": [],
    "stk_ipo_result": [],
    "stk_ipo_sh_si": [],
}  # 待爬取的url
seen_urls = set()  # 已爬取的url用于去重过滤(布容过滤器可过滤上亿条url)
stopping = False


async def get_proxy_ip(session):

    # resp = requests.get(url)
    resp = await fetch(url, session)
    resp = json.loads(resp)
    proxy_url = 'http://{}'.format(resp['data']['proxy_list'][0])
    print(proxy_url)
    return proxy_url


async def fetch(url, session, proxy=None):
    # proxy = "快代理IP"
    try:
        async with session.get(url, proxy=proxy) as resp:
            print(resp.status)
            if resp.status in [200, 201]:
                data = await resp.text()
                return data
    except Exception as e:
        print(e)


async def ipo_audit_save(data, pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for i in data:
                i = i.split(",")
                if i[7] == "":
                    i[7] = "null"
                    values = "({}, {}, '{}', '{}', '{}', '{}', {}, {}, '{}', '{}')".format(i[1], i[2], i[3], i[5], i[6],i[15], i[7], i[10], i[16],i[17])
                else:
                    values = "({}, {}, '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}')".format(i[1], i[2], i[3], i[5], i[6], i[15], i[7], i[10], i[16],i[17])
                colunm = "(ID1, ID2, SecurityName, SubmitDate, AuditDate, Status, PurchaseDate, IssueVolume, ListingLocation, Underwriter)"

                insert_sql = """replace into sp_ipo_audit {} values {}""".format(colunm, values)
                print(insert_sql)
                await cur.execute(insert_sql)


async def init_urls(start_url, session, pool):
    content = await fetch(start_url, session)
    # 初始化生成所有的url用于高并发爬取开始
    # 提取数据, 存入数据库
    data = json.loads(re.findall('(?<=data:)(.*[^}])', content)[0])
    await ipo_audit_save(data, pool)
    # 取页数值, 生成其它的链接用于并发
    pages = int(re.findall('(?<=^{pages:)(\d+)', content)[0]) + 1
    url_end = """&js={pages:(pc),data:[(x)]}"""
    base_url = r"http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=NS&sty=NSSH&p={}"
    waiting_urls["stk_ipo_audit"].extend([base_url.format(i) + url_end for i in range(2, pages)])
    # print(waiting_urls)


async def consumer(pool, session, proxy):
    for i in waiting_urls["stk_ipo_audit"]:
        print("starting to get url: {}".format(i))
        content = await fetch(i, session, proxy)
        # 提取数据, 存入数据库
        data = json.loads(re.findall('(?<=data:)(.*[^}])', content)[0])
        await ipo_audit_save(data, pool)


async def main(loop):
    # 等待mysql连接建立好
    pool = await aiomysql.create_pool(host=TEST_DB["host"], port=TEST_DB["port"], user=TEST_DB["user"], password=TEST_DB["password"], db=TEST_DB["db"], loop=loop, charset='utf8', autocommit=True)
    async with aiohttp.ClientSession() as session:
        proxy = await get_proxy_ip(session)
        start_url = """http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=NS&sty=NSSH&p=1&js={pages:(pc),data:[(x)]}"""
        await init_urls(start_url, session, pool)
        await consumer(pool, session, proxy)
    pool.close()
    await pool.wait_closed()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))

