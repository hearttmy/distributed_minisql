# -*- coding: UTF-8 -*-
import logging
import sys

from kazoo.client import KazooClient

hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
logging.basicConfig(level=logging.WARNING, stream=sys.stdout)

# 创建一个客户端，可以指定多台zookeeper，
zk = KazooClient(hosts=hosts, logger=logging)
zk.start()

zk.ensure_path("/tables")

zk.ensure_path("/servers/minisql1/tables")
zk.ensure_path("/servers/minisql1/info")
zk.ensure_path("/servers/minisql1/instruction")
zk.ensure_path("/servers/minisql1/result")

zk.ensure_path("/servers/minisql2/tables")
zk.ensure_path("/servers/minisql2/info")
zk.ensure_path("/servers/minisql2/instruction")
zk.ensure_path("/servers/minisql2/result")

zk.ensure_path("/servers/minisql3/tables")
zk.ensure_path("/servers/minisql3/info")
zk.ensure_path("/servers/minisql3/instruction")
zk.ensure_path("/servers/minisql3/result")
