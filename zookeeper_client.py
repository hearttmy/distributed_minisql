# -*- coding: UTF-8 -*-
import hashlib
import logging
import math
import random
import sys
import time
from threading import Condition

from kazoo.client import KazooClient

hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
logging.basicConfig(level=logging.WARNING, stream=sys.stdout)
# 创建一个客户端，可以指定多台zookeeper，
zk = KazooClient(hosts=hosts, logger=logging)
server_list = ["minisql1", "minisql2", "minisql3"]
condition = Condition()
dataWatchFinished = 0


def cmd_get_sql():
    sql = ''
    s = input('MiniSQL>  ')
    while True:
        if s.rstrip().endswith(';'):
            sql += ' ' + s
            return sql
        else:
            sql += ' ' + s
            s = input()


def take_weight(elem):
    return elem['weight']


def get_create_table_server(num):
    candidate = []
    for server in server_list:
        data1, stat1 = zk.get("/servers/{}/info/recordNum".format(server))
        data_str1 = data1.decode('utf-8')
        data2, stat2 = zk.get("/servers/{}/info/tableNum".format(server))
        data_str2 = data2.decode('utf-8')
        tmp = int(data_str1) + int(data_str2)
        candidate.append({'server': server, 'weight': tmp})
    candidate.sort(key=take_weight)
    ans = []
    for i in range(num):
        ans.append(candidate[i]['server'])
    return ans


def get_create_index_server(table_name):
    i = table_name.find('(')
    if not i == -1:
        table_name = table_name[:table_name.find('(')]
    return get_normal_server(table_name)


def get_drop_index_server(index_name):
    if not zk.exists("/indexes/" + index_name):
        return server_list
    else:
        ans = zk.get_children("/indexes/" + index_name)
        return ans


def get_select_server(table_name):
    if not zk.exists("/tables/" + table_name):
        return server_list
    else:
        ans = [zk.get_children("/tables/" + table_name)[math.floor(random.random() * 2)]]
        return ans


def get_normal_server(table_name):
    if not zk.exists("/tables/" + table_name):
        return server_list
    else:
        return zk.get_children("/tables/" + table_name)


# modified
def get_target_server(sql):
    tmp = sql.lstrip(' ').split(' ')
    ans = []
    if tmp[0] == 'create':  # backup
        if tmp[1] == 'table':
            ans = get_create_table_server(2)
        elif tmp[1] == 'index':
            ans = get_create_index_server(tmp[4])
    elif tmp[0] == 'select':  # balancing
        if tmp[3][len(tmp[3]) - 1] == ';':
            ans = get_select_server(tmp[3][:len(tmp[3]) - 1])
        else:
            ans = get_select_server(tmp[3])
    elif tmp[0] == 'drop' and tmp[1] == 'index':
        ans = get_drop_index_server(tmp[2])
    else:
        ans = get_normal_server(tmp[2])
    return ans


def get_path_list(target_server, sql):
    ans = []
    m = hashlib.sha256()
    for server in target_server:
        m.update((str(random.random()) + sql + str(time.time())).encode('utf-8'))
        node_name = m.hexdigest()
        ans.append('/servers/{}/instructions/{}'.format(server, node_name))
    return ans


def delete_finished_node(path_list):
    for path in path_list:
        if zk.exists(path):
            zk.delete(path, recursive=True)


def set_sql_and_watchers(path_list, sql):
    for path in path_list:
        zk.create(path, sql)
        zk.ensure_path(path + '/result')
        zk.DataWatch(path + '/result', watch_result_node)


# 当节点kazoo的数据变化时这个函数会被调用
# 如果节点被删除这个函数也会被调用，但是data和stat都是None
def watch_result_node(data, stat):
    global dataWatchFinished
    condition.acquire()

    if stat and data:
        dataWatchFinished += 1
        condition.notify()
        data_str = data.decode("utf-8")
        print("Result: \n" + data_str, end='')

    condition.release()


if __name__ == '__main__':
    zk.start()
    target_server = []
    path_list = []
    sql_input = ''
    while True:
        condition.acquire()
        while dataWatchFinished != len(target_server):
            # print('before wait', dataWatchFinished)
            condition.wait()
            # print('after wait', dataWatchFinished)

        delete_finished_node(path_list)

        sql_input = cmd_get_sql()
        target_server = get_target_server(sql_input)
        print(target_server)
        path_list = get_path_list(target_server, sql_input)

        dataWatchFinished = 0
        # quit and file exec maybe
        set_sql_and_watchers(path_list, bytes(sql_input, encoding="utf-8"))

        condition.release()
