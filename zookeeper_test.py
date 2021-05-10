# -*- coding: UTF-8 -*-
import logging
import sys

from kazoo.client import KazooClient

hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
logging.basicConfig(level=logging.WARNING, stream=sys.stdout)

# 创建一个客户端，可以指定多台zookeeper，
zk = KazooClient(hosts=hosts, logger=logging)
zk.start()

zk.set("/servers/minisql1/instruction",
       b"select * from t;")
