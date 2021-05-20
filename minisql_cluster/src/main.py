import logging
import sys
from time import sleep

from kazoo.client import KazooClient

from interpreter import parser, zookeeper_result

hosts = '172.16.238.2:2181,172.16.238.3:2182,172.16.238.4:2183'
test_hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
server_path = "/servers/" + sys.argv[1]
# 创建一个客户端，可以指定多台zookeeper，
zk = KazooClient(hosts=test_hosts, logger=logging)


def watch_instruction_children(children):
    print(children)
    for ch in children:
        data, stat = zk.get('{}/instructions/{}'.format(server_path, ch))
        if data and stat:
            data_str = data.decode('utf-8')
            parser.parse(data_str)
            zk.ensure_path('{}/instructions/{}/result'.format(server_path, ch))
            zookeeper_result(zk, '{}/instructions/{}/result'.format(server_path, ch))


if __name__ == '__main__':
    # 开始心跳
    zk.start()
    zk.ensure_path('/tables')
    zk.ensure_path("{}/tables".format(server_path))
    zk.ensure_path("{}/info".format(server_path))
    if not zk.exists("{}/info/recordNum".format(server_path)):
        zk.create("{}/info/recordNum".format(server_path), b'0')
    if not zk.exists("{}/info/tableNum".format(server_path)):
        zk.create("{}/info/tableNum".format(server_path), b'0')
    zk.delete("{}/instructions".format(server_path), recursive=True)
    zk.ensure_path("{}/instructions".format(server_path))
    zk.ChildrenWatch(server_path + "/instructions", watch_instruction_children)

    while True:
        sleep(60)
        print("Watching...")
