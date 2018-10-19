#!/usr/bin/env python3
import errno
import logging
import os
import platform
import re
import time
from logging.config import dictConfig

from apscheduler.schedulers.background import BackgroundScheduler
from pathlib2 import Path
from ruamel.yaml import YAML
from tabulate import tabulate

from source.cli import Cli
from source.node import Node, State
from source.utils import parse_tag


def setup_logging(default_path='logging.yaml', default_level=logging.INFO):
    """Setup logging configuration

    """
    if os.path.exists(default_path):
        config = load_cfg(default_path)
        # logging.config.dictConfig(config)
        dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def create_dir(dir_):
    if not os.path.exists(dir_):
        try:
            os.makedirs(dir_)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def load_cfg(path='config.yaml'):
    if os.path.exists(path):
        path = Path(path)
        yaml_ = YAML(typ='safe')
        return yaml_.load(path)


def validate_eth_addr(eth_addr):
    pattern = re.compile("^0x[a-fA-F0-9]{40}$")
    if not pattern.match(eth_addr):
        logger.info("Incorrect eth address or not specified")
        return None
    else:
        logger.info("Eth address was parsed successfully: " + eth_addr)
        return eth_addr


def set_sonmcli():
    if platform.system() == "Darwin":
        return "sonmcli_darwin_x86_64"
    else:
        return "sonmcli"


def init():
    create_dir("out/orders")
    create_dir("out/tasks")

    config = load_cfg()
    config_keys = ["numberofnodes", "tag", "ets", "template_file", "identity", "ramsize",
                   "storagesize", "cpucores", "sysbenchsingle", "sysbenchmulti", "netdownload", "netupload", "price",
                   "overlay", "incoming", "gpucount", "gpumem", "ethhashrate"]
    missed_keys = [key for key in config_keys if key not in config]
    if len(missed_keys) > 0:
        raise Exception("Missed keys: '" + "', '".join(missed_keys) + "'")
    logger.info("Try to parse counterparty eth address:")
    counter_party = None
    if "counterparty" in config:
        counter_party = validate_eth_addr(config["counterparty"])
    return Cli(set_sonmcli()), config, counter_party


def get_nodes(cli_, config, counterparty):
    nodes_ = []
    for n in range(config["numberofnodes"]):
        nodes_.append(Node.create_empty(cli_, n + 1, config["tag"], config, counterparty))
    return nodes_


def init_nodes_state(cli_, nodes_num_, config, counter_party):
    nodes_ = []
    # get deals
    deals_ = cli_.deal_list(nodes_num_)
    if deals_ and deals_['deals']:
        for d in [d_["deal"] for d_ in deals_['deals']]:
            status = State.DEAL_OPENED
            deal_status = cli_.deal_status(d["id"])
            ntag = parse_tag(deal_status["bid"]["tag"])
            node_num = ntag.split("_")[len(ntag.split("_")) - 1]
            task_id = ""
            if "resources" not in deal_status:
                logger.info(
                    "Seems like worker is offline: no respond for the resources and tasks request. Closing deal")
                status = State.TASK_FAILED
            if "running" in deal_status and len(list(deal_status["running"].keys())) > 0:
                task_id = list(deal_status["running"].keys())[0]
                status = State.TASK_RUNNING
            bid_id_ = deal_status["bid"]["id"]
            node_ = Node(status, cli_, node_num, config["tag"], d["id"], task_id, bid_id_, config, counter_party)
            logger.info("Found deal, id " + d["id"] + " (Node " + node_num + ")")
            nodes_.append(node_)

    # get orders
    orders_ = cli_.order_list(nodes_num_)
    if orders_ and orders_["orders"] is not None:
        for order_ in list(orders_["orders"]):
            status = State.AWAITING_DEAL
            ntag = parse_tag(order_["tag"])
            node_num = ntag.split("_")[len(ntag.split("_")) - 1]
            node_ = Node(status, cli_, node_num, config["tag"], "", "", order_["id"], config, counter_party)
            logger.info("Found order, id " + order_["id"] + " (Node " + node_num + ")")
            nodes_.append(node_)
    if len(nodes_) == 0:
        nodes_ = get_nodes(cli_, config, counter_party)
    return nodes_


def print_state(nodes_):
    nodes_.sort(key=lambda x: int(x.node_num), reverse=False)
    if len([node_ for node_ in nodes_ if node_.status != State.WORK_COMPLETED]) == 0:
        logger.info("All nodes completed their work")
        scheduler.remove_job("print_state")
    tabul_nodes = [[n.node_num, n.bid_id, n.deal_id, n.task_id, n.task_uptime, n.status.name] for n in nodes_]
    logger.info("Nodes:\n" +
                tabulate(tabul_nodes,
                         ["Node", "Order id", "Deal id", "Task id", "Task uptime", "Node status"],
                         tablefmt="grid"))


def watch(nodes_num_, nodes_):
    # Check deals and change status to DEAL_OPENED
    futures = []
    for node in nodes_:
        futures.append(node.watch_node(nodes_num_))
        time.sleep(1)
    for future in futures:
        future.result()


def main():
    global scheduler
    cli_, config, counter_party = init()
    nodes_num_ = int(config["numberofnodes"])
    nodes_ = init_nodes_state(cli_, nodes_num_, config, counter_party)
    scheduler = BackgroundScheduler()
    print('Press Ctrl+{0} to interrupt script'.format('Break' if os.name == 'nt' else 'C'))
    try:
        scheduler.start()
        scheduler.add_job(print_state, 'interval', kwargs={"nodes_": nodes_}, seconds=10, id='print_state')
        watch(nodes_num_, nodes_)
        scheduler.shutdown()
    except (KeyboardInterrupt, SystemExit):
        pass


setup_logging()
logging.getLogger('apscheduler').setLevel(logging.FATAL)
logger = logging.getLogger('monitor')
if __name__ == "__main__":
    main()
