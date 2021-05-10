import logging
import sys
from time import sleep

from kazoo.client import KazooClient

from interpreter import parser, zookeeper_result

hosts = '172.16.238.2:2181,172.16.238.3:2182,172.16.238.4:2183'
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
server_path = "/servers/" + sys.argv[1]

# 创建一个客户端，可以指定多台zookeeper，
zk = KazooClient(hosts=hosts, logger=logging)


@zk.DataWatch(server_path + "/instruction")  # 当节点kazoo的数据变化时这个函数会被调用
def watch_instruction_node(data, stat):
    # 如果节点被删除这个函数也会被调用，但是data和stat都是None
    if stat and data and watch_instruction_node.flag:
        data_str = data.decode("utf-8")
        print("Version: %s, data: %s" % (stat.version, data_str))
        parser.parse(data_str)
        zookeeper_result(zk, server_path + "/result")
    else:
        watch_instruction_node.flag = 1
        print("节点未初始化或已被删除")


if __name__ == '__main__':
    # 开始心跳
    zk.start()
    zk.ensure_path(server_path + "/result")
    zk.ensure_path(server_path + "/instruction")
    while True:
        sleep(60)
        print("Watching...")
