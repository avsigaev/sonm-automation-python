#!/usr/bin/env python3

import base64
import errno
import logging
import os
import platform
import re
import time

from apscheduler.schedulers.background import BackgroundScheduler
from pathlib2 import Path
from ruamel.yaml import YAML

from source.cli import Cli
from source.log import log
from source.node import Node, State


def create_dir(dir_):
    if not os.path.exists(dir_):
        try:
            os.makedirs(dir_)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def load_cfg():
    path = Path('config.yaml')
    yaml_ = YAML(typ='safe')
    return yaml_.load(path)


def validate_eth_addr(eth_addr):
    pattern = re.compile("^0x[a-fA-F0-9]{40}$")
    if not pattern.match(eth_addr):
        log("Incorrect eth address or not specified")
        return None
    else:
        log("Eth address was parsed successfully: " + eth_addr)
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
    config_keys = ["numberofnodes", "tag", "ets", "template_file", "iteration_time", "identity", "ramsize",
                   "storagesize", "cpucores", "sysbenchsingle", "sysbenchmulti", "netdownload", "netupload", "price",
                   "overlay", "incoming", "gpucount", "gpumem", "ethhashrate"];
    missed_keys = [key for key in config_keys if key not in config]
    if len(missed_keys) > 0:
        raise Exception("Missed keys: '" + "', '".join(missed_keys) + "'")
    log("Try to parse counterparty eth address:")
    counter_party = None
    if "counterparty" in config:
        counter_party = validate_eth_addr(config["counterparty"])
    return Cli(set_sonmcli()), config, counter_party


def get_nodes(cli_, config, counterparty):
    nodes_ = []
    for n in range(config["numberofnodes"]):
        nodes_.append(Node.create_empty(cli_, n + 1, config["tag"], config, counterparty))
    return nodes_


def watch(nodes_num_, nodes_, cli_):
    # Check deals and change status to DEAL_OPENED
    check_opened_deals(cli_, nodes_, nodes_num_)
    futures = []
    for node in nodes_:
        if node.status == State.START:
            node.create_yaml()
            futures.append(node.create_order())
            time.sleep(1)
        elif node.status == State.CREATE_ORDER:
            futures.append(node.create_order())
        elif node.status == State.DEAL_OPENED:
            node.start_task()
        elif node.status == State.DEAL_DISAPPEARED:
            node.status = State.CREATE_ORDER
        elif node.status == State.TASK_RUNNING:
            futures.append(node.check_task_status())
        elif node.status == State.TASK_FAILED or node.status == State.TASK_FAILED_TO_START:
            futures.append(node.close_deal(State.CREATE_ORDER, blacklist=True))
        elif node.status == State.TASK_BROKEN:
            futures.append(node.close_deal(State.CREATE_ORDER))
        elif node.status == State.TASK_FINISHED:
            futures.append(node.close_deal(State.WORK_COMPLETED))
    for future in futures:
        future.result()
    # Delete nodes with finished tasks from node list
    # (Create new list where status != DEAL_CLOSED)
    nodes_.sort(key=lambda x: int(x.node_num), reverse=False)
    if len([node_ for node_ in nodes_ if node_.status != State.WORK_COMPLETED]) == 0:
        log("All nodes completed their work")
        scheduler.remove_job("sonm_watch")
    log("Nodes:\n" + '\n '.join("\t{0.node_num} ({0.status.name})".format(n) for n in nodes_))


def check_opened_deals(cli_, nodes_, nodes_num_):
    # Match deals and nodes
    deal_list = cli_.deal_list(nodes_num_)
    orders_ = cli_.order_list(nodes_num_)
    all_orders = []

    for node in [node_ for node_ in nodes_ if node_.status == State.AWAITING_DEAL]:
        if orders_ and orders_["orders"] is not None:
            for order_ in list(orders_["orders"]):
                if order_["id"] not in all_orders:
                    all_orders.append(order_["id"])
                if parse_tag(order_["tag"]) == node.node_tag:
                    node.bid_id = order_["id"]
                    node.status = State.AWAITING_DEAL
        if deal_list and deal_list['deals']:
            for _, v in deal_list.items():
                for d in v:
                    if d["bidID"] not in all_orders:
                        all_orders.append(d["bidID"])
                    if d["bidID"] == node.bid_id:
                        node.deal_id = d["id"]
                        if node.status == State.AWAITING_DEAL:
                            node.status = State.DEAL_OPENED
                            log("Deal " + node.deal_id + " opened (Node " + node.node_num + ") ")
    # known_orders = [node_.bid_id for node_ in nodes_ if node_.status.value > 1]
    # result_list = [x for x in all_orders if x not in known_orders]
    # all_orders = [ord_["id"] for ord_ in orders_["orders"]]
    # known_orders = [node_.bid_id for node_ in nodes_ if node_.status.value > 1]
    # result_list = [x for x in all_orders if x not in known_orders]
    # for order_ in result_list:
    #     order_status = cli_.order_status(order_)
    #     for node in nodes_:
    #         if parse_tag(order_status["tag"]) == node.node_tag:
    #             node.bid_id = order_
    #             node.status = State.ORDER_PLACED


def init_nodes_state(cli_, nodes_num_, config, counter_party):
    nodes_ = []
    # get deals
    deals_ = cli_.deal_list(nodes_num_)
    if deals_ and deals_['deals']:
        for _, v in deals_.items():
            for d in v:
                status = State.DEAL_OPENED
                deal_status = cli_.deal_status(d["id"])
                ntag = parse_tag(deal_status["bid"]["tag"])
                node_num = ntag.split("_")[len(ntag.split("_")) - 1]
                task_id = ""
                if "resources" not in deal_status:
                    log("Seems like worker is offline: no respond for the resources and tasks request. Closing deal")
                    status = State.TASK_FAILED
                if "running" in deal_status and len(list(deal_status["running"].keys())) > 0:
                    task_id = list(deal_status["running"].keys())[0]
                    status = State.TASK_RUNNING
                bid_id_ = deal_status["bid"]["id"]
                node_ = Node(status, cli_, node_num, config["tag"], d["id"], task_id, bid_id_, config, counter_party)
                log("Found deal, id " + d["id"] + " (Node " + node_num + ")")
                nodes_.append(node_)

    # get orders
    orders_ = cli_.order_list(nodes_num_)
    if orders_ and orders_["orders"] is not None:
        for order_ in list(orders_["orders"]):
            status = State.AWAITING_DEAL
            ntag = parse_tag(order_["tag"])
            node_num = ntag.split("_")[len(ntag.split("_")) - 1]
            node_ = Node(status, cli_, node_num, config["tag"], "", "", order_["id"], config, counter_party)
            log("Found order, id " + order_["id"] + " (Node " + node_num + ")")
            nodes_.append(node_)
    if len(nodes_) == 0:
        nodes_ = get_nodes(cli_, config, counter_party)
    return nodes_


def parse_tag(order_):
    return base64.b64decode(order_).decode().strip("\0")


def main():
    logging.basicConfig()
    logging.getLogger('apscheduler').setLevel(logging.FATAL)
    global scheduler
    cli_, config, counter_party = init()
    nodes_num_ = int(config["numberofnodes"])
    nodes_ = init_nodes_state(cli_, nodes_num_, config, counter_party)
    scheduler = BackgroundScheduler()
    print('Press Ctrl+{0} to interrupt script'.format('Break' if os.name == 'nt' else 'C'))
    try:
        scheduler.start()
        scheduler.add_job(watch, 'interval', kwargs={"nodes_num_": nodes_num_, "nodes_": nodes_, "cli_": cli_},
                          seconds=int(config["iteration_time"]), id='sonm_watch')
        while len(scheduler.get_jobs()) > 0:
            time.sleep(1)
        scheduler.shutdown()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()
