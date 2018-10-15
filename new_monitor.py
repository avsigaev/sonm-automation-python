#!/usr/bin/env python3

import base64
import datetime
import errno
import json
import logging
import os
import platform
import re
import subprocess
import threading
import time
from enum import Enum

from pathlib2 import Path
from ruamel import yaml
from ruamel.yaml import YAML

from yaml_gen import template_bid, template_task


def threaded(fn):
    def wrapper(*args, **kwargs):
        threading.Thread(target=fn, args=args, kwargs=kwargs).start()

    return wrapper


class State(Enum):
    START = 0
    YAML_CREATED = 1
    PLACING_ORDER = 2
    ORDER_PLACED = 3
    DEAL_OPENED = 4
    DEAL_DISAPPEARED = 5
    STARTING_TASK = 6
    TASK_STARTED = 7
    TASK_FAILED = 8
    TASK_FAILED_TO_START = 9
    TASK_FINISHED = 10
    DEAL_CLOSED = 11


class Cli:
    def __init__(self, cli_):
        self.cli = cli_

    def exec(self, param, retry=False, attempts=3, sleep_time=1):
        command = [self.cli] + param
        command.append("--json")
        attempt = 1
        errors_ = []
        while True:
            result = subprocess.run(command, stdout=subprocess.PIPE)
            if result.returncode == 0:
                break
            if not retry or attempt > attempts:
                break
            errors_.append(str(result.stdout))
            attempt += 1
            time.sleep(sleep_time)
        if result.returncode != 0:
            log("Failed to execute command: " + ' '.join(command))
            log('\n'.join(errors_))
            return None
        if result.stdout.decode("utf-8") == "null":
            return {}
        return json.loads(result.stdout.decode("utf-8"))

    def save_task_logs(self, deal_id, task_id, rownum, filename):
        command = [self.cli, "task", "logs", deal_id, task_id, "--tail", rownum]
        with open(filename, "w") as outfile:
            subprocess.call(command, stdout=outfile)

    def order_create(self, bid_file):
        return self.exec(["order", "create", bid_file])

    def order_list(self, number_of_nodes):
        return self.exec(["order", "list", "--timeout=2m", "--limit", str(number_of_nodes)])

    def order_status(self, order_id):
        return self.exec(["order", "status", str(order_id)])

    def deal_list(self, number_of_nodes):
        return self.exec(["deal", "list", "--timeout=2m", "--limit", str(number_of_nodes)])

    def deal_status(self, deal_id):
        return self.exec(["deal", "status", deal_id, "--expand"])

    def deal_close(self, deal_id, bl_worker=False):
        close_d_command = ["deal", "close", deal_id]
        if bl_worker:
            close_d_command += ["--blacklist", "worker"]
        return self.exec(close_d_command, retry=True)

    def task_status(self, deal_id, task_id):
        return self.exec(["task", "status", deal_id, task_id, "--timeout=2m"], retry=True)

    def task_start(self, deal_id, task_file):
        return self.exec(["task", "start", deal_id, task_file, "--timeout=15m"], retry=True)

    def task_list(self, deal_id):
        return self.exec(["task", "list", deal_id, "--timeout=2m"], retry=True)


class Node:
    def __init__(self, status, cli_, node_num, tag, deal_id, task_id, bid_id):
        self.status = status
        self.cli = cli_
        self.node_num = str(node_num)
        self.node_tag = tag + "_" + self.node_num
        self.bid_file = "out/orders/" + self.node_tag + ".yaml"
        self.task_file = "out/tasks/" + self.node_tag + ".yaml"
        self.deal_id = deal_id
        self.task_id = task_id
        self.bid_id = bid_id

    @classmethod
    def create_empty(cls, cli_, node_num, tag):
        return cls(State.START, cli_, node_num, tag, "", "", "")

    def create_yaml(self):
        bid_ = template_bid(CONFIG, self.node_tag, COUNTER_PARTY)
        task_ = template_task(self.node_tag)
        log("Creating order file Node number " + str(self.node_num))
        self.dump_file(bid_, self.bid_file)
        log("Creating task file for Node number " + str(self.node_num))
        self.dump_file(task_, self.task_file)
        self.status = State.YAML_CREATED

    @threaded
    def create_order(self):
        self.status = State.PLACING_ORDER
        log("Create order for Node " + str(self.node_num))
        self.bid_id = self.cli.order_create(self.bid_file)["id"]
        self.status = State.ORDER_PLACED
        log("Order for Node " + self.node_num + " is " + self.bid_id)

    @threaded
    def start_task(self):
        # Start task on node
        self.status = State.STARTING_TASK
        log("Starting task on node " + str(self.node_num) + "...")
        task = self.cli.task_start(self.deal_id, self.task_file)
        if not task:
            log("Failed to start task (Node " + str(self.node_num) + ") on deal " + self.deal_id +
                ". Closing deal and blacklisting counterparty worker's address...")
            self.status = State.TASK_FAILED_TO_START
        else:
            log("Task (Node " + str(self.node_num) + ") started: deal " + self.deal_id + " with task_id " + task["id"])
            self.task_id = task["id"]
            self.status = State.TASK_STARTED

    def close_deal(self, blacklist=False):
        # Close deal on node
        log("Saving logs deal_id " + self.deal_id + " task_id " + self.task_id)
        if self.status == State.TASK_FAILED:
            self.save_task_logs("out/fail_")
        if self.status == State.TASK_FINISHED:
            self.save_task_logs("out/success_")
        log("Closing deal " + self.deal_id + " ...")
        self.cli.deal_close(self.deal_id, blacklist)

    def set_status(self, status_):
        self.status = status_

    @threaded
    def check_task_status(self):
        task_list = self.cli.task_list(self.deal_id)
        if task_list and len(task_list.keys()) > 0:
            if "error" in task_list.keys() or "message" in task_list.keys():
                if self.cli.deal_status(self.deal_id)["deal"]["status"] == 2:
                    log("Deal " + self.deal_id + " was closed")
                    self.status = State.DEAL_DISAPPEARED
                    return
                log("Cannot retrieve task list of deal " + self.deal_id + ", worker is offline?")
                self.status = State.TASK_FAILED
                return
            task_status = self.cli.task_status(self.deal_id, self.task_id)
            if task_status and "status" in task_status:
                status_ = task_status["status"]
            else:
                if self.cli.deal_status(self.deal_id)["deal"]["status"] == 2:
                    log("Deal " + self.deal_id + " was closed")
                    self.status = State.DEAL_DISAPPEARED
                    return
                else:
                    log("Cannot retrieve task status of deal " + self.deal_id +
                        ", task_id " + self.task_id + " worker is offline?")
                    self.status = State.TASK_FAILED
                    return
            time_ = str(int(float(int(task_status["uptime"]) / 1000000000)))
            if status_ == "RUNNING":
                log("Task " + self.task_id + " on deal " + self.deal_id + " (Node " + self.node_num +
                    ") is running. Uptime is " + time_ + " seconds")
            if status_ == "SPOOLING":
                log(
                    "Task " + self.task_id + " on deal " + self.deal_id +
                    " (Node " + self.node_num + ") is uploading...")
                self.status = State.STARTING_TASK
            if status_ == "BROKEN" or status_ == "FINISHED":
                if int(time_) > CONFIG["eta"]:
                    log("Task " + self.task_id + "  on deal " + self.deal_id + " (Node " + self.node_num +
                        " ) is finished. Uptime is " + time_ + "  seconds")
                    log("Task " + self.task_id + "  on deal " + self.deal_id + " (Node " + self.node_num +
                        " ) success. Fetching log, shutting down node...")
                    self.status = State.TASK_FINISHED
                else:
                    log("Task has failed/stopped (" + time_ + " seconds) on deal " + self.deal_id +
                        " (Node " + self.node_num + ") before ETA." +
                        " Closing deal and blacklisting counterparty worker's address...")
                    self.status = State.TASK_FAILED

    def save_task_logs(self, prefix):
        self.cli.save_task_logs(self.deal_id, self.task_id, "1000000",
                                prefix + self.node_tag + "-deal-" + self.deal_id + ".log")

    def dump_file(self, data, filename):
        with open(filename, 'w+') as file:
            yaml.dump(data, file, Dumper=yaml.RoundTripDumper)


def log(s):
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + s)


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

    global CONFIG, COUNTER_PARTY
    CONFIG = load_cfg()
    log("Try to parse counterparty eth address:")
    COUNTER_PARTY = validate_eth_addr(CONFIG["counterparty"])
    return Cli(set_sonmcli())


def get_nodes(cli_):
    nodes_ = []
    for n in range(CONFIG["numberofnodes"]):
        nodes_.append(Node.create_empty(cli_, n + 1, CONFIG["tag"]))
    return nodes_


def watch(nodes_num_, nodes_, cli_):
    while True:
        # Check deals and change status to DEAL_OPENED
        check_opened_deals(cli_, nodes_, nodes_num_)

        for node in nodes_:
            if node.status == State.START:
                node.create_yaml()
                node.create_order()
                time.sleep(1)
            elif node.status == State.YAML_CREATED:
                node.create_order()
            elif node.status == State.DEAL_OPENED:
                node.start_task()
            elif node.status == State.DEAL_DISAPPEARED:
                node.status = State.YAML_CREATED
            elif node.status == State.TASK_STARTED:
                node.check_task_status()
            elif node.status == State.TASK_FAILED or node.status == State.TASK_FAILED_TO_START:
                node.close_deal(blacklist=True)
                node.status = State.YAML_CREATED
            elif node.status == State.TASK_FINISHED:
                node.close_deal()
                node.status = State.DEAL_CLOSED
        # Delete nodes with finished tasks from node list
        # (Create new list where status != DEAL_CLOSED)
        nodes_ = [node_ for node_ in nodes_ if node_.status != State.DEAL_CLOSED]
        nodes_.sort(key=lambda x: int(x.node_num), reverse=False)
        if len(nodes_) == 0:
            log("No active nodes left")
            exit(0)
        log("Active nodes:\n" + '\n '.join("\t{0.node_num} ({0.status.name})".format(n) for n in nodes_))
        time.sleep(30)


def check_opened_deals(cli_, nodes_, nodes_num_):
    # Match deals and nodes
    deal_list = cli_.deal_list(nodes_num_)
    orders_ = cli_.order_list(nodes_num_)
    all_orders = []

    for node in [node_ for node_ in nodes_ if node_.status == State.ORDER_PLACED]:
        if orders_ and orders_["orders"] is not None:
            for order_ in list(orders_["orders"]):
                if order_["id"] not in all_orders:
                    all_orders.append(order_["id"])
                if parse_tag(order_["tag"]) == node.node_tag:
                    node.bid_id = order_["id"]
                    node.status = State.ORDER_PLACED
        if deal_list and deal_list['deals']:
            for _, v in deal_list.items():
                for d in v:
                    if d["bidID"] not in all_orders:
                        all_orders.append(d["bidID"])
                    if d["bidID"] == node.bid_id:
                        node.deal_id = d["id"]
                        if node.status == State.ORDER_PLACED:
                            node.status = State.DEAL_OPENED
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


def init_nodes_state(cli_, nodes_num_):
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
                if "running" in deal_status and len(list(deal_status["running"].keys())) > 0:
                    task_id = list(deal_status["running"].keys())[0]
                    status = State.TASK_STARTED
                bid_id_ = deal_status["bid"]["id"]
                node_ = Node(status, cli_, node_num, CONFIG["tag"], d["id"], task_id, bid_id_)
                log("Found deal, id " + d["id"] + " (Node " + node_num + ")")
                nodes_.append(node_)

    # get orders
    orders_ = cli_.order_list(nodes_num_)
    if orders_ and orders_["orders"] is not None:
        for order_ in list(orders_["orders"]):
            status = State.ORDER_PLACED
            ntag = parse_tag(order_["tag"])
            node_num = ntag.split("_")[len(ntag.split("_")) - 1]
            node_ = Node(status, cli_, node_num, CONFIG["tag"], "", "", order_["id"])
            log("Found order, id " + order_["id"] + " (Node " + node_num + ")")
            nodes_.append(node_)
    if len(nodes_) == 0:
        nodes_ = get_nodes(cli_)
    return nodes_


def parse_tag(order_):
    return base64.b64decode(order_).decode().strip("\0")


def main():
    logging.basicConfig()
    logging.getLogger('apscheduler').setLevel(logging.FATAL)
    cli_ = init()
    nodes_num_ = int(CONFIG["numberofnodes"])
    nodes_ = init_nodes_state(cli_, nodes_num_)
    watch(nodes_num_, nodes_, cli_)


if __name__ == "__main__":
    main()
