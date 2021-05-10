# -*- coding: UTF-8 -*-
import logging
import sys
from threading import Condition

from kazoo.client import KazooClient

hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
logging.basicConfig(level=logging.WARNING, stream=sys.stdout)
# 创建一个客户端，可以指定多台zookeeper，
zk = KazooClient(hosts=hosts, logger=logging)
zk.start()
server_list = ["minisql1", "minisql2", "minisql3"]


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


def set_sql(target_path_list, sql):
    for target in target_path_list:
        zk.set(target, sql)


condition = Condition()


# 当节点kazoo的数据变化时这个函数会被调用
# 如果节点被删除这个函数也会被调用，但是data和stat都是None
def watch_instruction_node(data, stat):
    if stat and data:
        data_str = data.decode("utf-8")
        print("Result: \n" + data_str)
    else:
        print("节点未初始化或已被删除")

    condition.notify()


if __name__ == '__main__':
    for server in server_list:
        zk.DataWatch("/servers/" + server + "/result", watch_instruction_node)
    while True:
        sql = cmd_get_sql()
        set_sql(["/servers/minisql1/instruction"], bytes(sql, encoding="utf-8"))
        semaphore.acquire()
