import logging
import time
from enum import Enum

from ruamel import yaml

from source.utils import load_cfg, threaded
from source.yaml_gen import template_bid, template_task


class State(Enum):
    START = 0
    CREATE_ORDER = 1
    PLACING_ORDER = 2
    AWAITING_DEAL = 3
    DEAL_OPENED = 4
    DEAL_DISAPPEARED = 5
    STARTING_TASK = 6
    TASK_RUNNING = 7
    TASK_FAILED = 8
    TASK_FAILED_TO_START = 9
    TASK_BROKEN = 10
    TASK_FINISHED = 11
    WORK_COMPLETED = 12


class Node:
    def __init__(self, status, cli_, node_num, deal_id, task_id, bid_id, counterparty):
        self.config = {}
        self.reload_config()
        self.status = status
        self.cli = cli_
        self.node_num = str(node_num)
        self.node_tag = "{}_{}".format(self.config["tag"], self.node_num)
        self.bid_file = "out/orders/{}.yaml".format(self.node_tag)
        self.task_file = "out/tasks/{}.yaml".format(self.node_tag)
        self.deal_id = deal_id
        self.task_id = task_id
        self.bid_id = bid_id
        self.counterparty = counterparty
        self.logger = logging.getLogger("monitor")
        self.task_uptime = 0
        self.logger.info("Creating task file for Node number {}".format(self.node_num))
        self.create_bid_yaml()
        self.logger.info("Creating order file Node number {}".format(self.node_num))
        self.create_task_yaml()

    @classmethod
    def create_empty(cls, cli_, node_num, counterparty):
        return cls(State.START, cli_, node_num, "", "", "", counterparty)

    def reload_config(self):
        self.config = load_cfg()

    def create_task_yaml(self):
        task_ = template_task(self.config["template_file"], self.node_tag)
        self.dump_file(task_, self.task_file)

    def create_bid_yaml(self):
        bid_ = template_bid(self.config, self.node_tag, self.counterparty)
        self.dump_file(bid_, self.bid_file)

    def create_order(self):
        self.reload_config()
        self.create_task_yaml()
        self.create_bid_yaml()
        self.status = State.PLACING_ORDER
        self.logger.info("Create order for Node {}".format(self.node_num))
        self.bid_id = self.cli.order_create(self.bid_file)["id"]
        self.status = State.AWAITING_DEAL
        self.logger.info("Order for Node {} is {}".format(self.node_num, self.bid_id))

    def check_order(self):
        order_status = self.cli.order_status(self.bid_id)
        self.logger.info("Checking order {} (Node {}) for new deal".format(self.bid_id, self.node_num))
        if order_status and order_status["orderStatus"] == 1 and order_status["dealID"] != "0":
            self.deal_id = order_status["dealID"]
            self.status = State.DEAL_OPENED
            self.logger.info("For order {} (Node {}) opened new deal {}"
                             .format(self.bid_id, self.node_num, self.deal_id))
            return 1
        elif order_status and order_status["orderStatus"] == 1 and order_status["dealID"] == "0":
            self.logger.info("Order {} was cancelled (Node {}), create new order".format(self.bid_id, self.node_num))
            self.bid_id = ""
            self.status = State.CREATE_ORDER
            return 1
        return 60

    def start_task(self):
        # Start task on node
        self.status = State.STARTING_TASK
        self.logger.info("Starting task on node {} ...".format(self.node_num))
        task = self.cli.task_start(self.deal_id, self.task_file)
        if not task:
            self.logger.info("Failed to start task (Node {}) on deal {}. Closing deal and blacklisting counterparty "
                             "worker's address...".format(self.node_num, self.deal_id))
            self.status = State.TASK_FAILED_TO_START
        else:
            self.logger.info("Task (Node {}) started: deal {} with task_id {}"
                             .format(self.node_num, self.deal_id, task["id"]))
            self.task_id = task["id"]
            self.status = State.TASK_RUNNING

    def close_deal(self, state_after, blacklist=False):
        # Close deal on node
        self.logger.info("Saving logs deal_id {} task_id {}".format(self.deal_id, self.task_id))
        if self.status == State.TASK_FAILED or self.status == State.TASK_BROKEN:
            self.save_task_logs("out/fail_")
        if self.status == State.TASK_FINISHED:
            self.save_task_logs("out/success_")
        self.logger.info("Closing deal {}{}..."
                         .format(self.deal_id, (" with blacklisting worker " if blacklist else " ")))
        self.cli.deal_close(self.deal_id, blacklist)
        self.deal_id = ""
        self.bid_id = ""
        self.task_uptime = 0
        self.task_id = ""
        self.status = state_after

    def check_task_status(self):
        deal_status = self.cli.deal_status(self.deal_id)
        if deal_status and "deal" in deal_status and deal_status["deal"]["status"] == 2:
            self.logger.info("Deal {} was closed".format(self.deal_id))
            self.status = State.DEAL_DISAPPEARED
            self.deal_id = ""
            self.bid_id = ""
            self.task_uptime = 0
            self.task_id = ""
            return 1
        elif deal_status and "error" in deal_status:
            self.logger.error("Cannot retrieve status deal {}".format(self.deal_id))
            return 60
        task_list = self.cli.task_list(self.deal_id)
        if task_list and len(task_list.keys()) > 0:
            if "error" in task_list.keys() or "message" in task_list.keys():
                self.logger.error("Cannot retrieve task list of deal {}, worker is offline?".format(self.deal_id))
                self.logger.error("Dump task list response: {}".format(task_list))
                self.status = State.TASK_FAILED
                return 1
            task_status = self.cli.task_status(self.deal_id, self.task_id)
            if task_status and "status" in task_status:
                status_ = task_status["status"]
            else:
                self.logger.error("Cannot retrieve task status of deal {},"
                                  " task_id {} worker is offline?".format(self.deal_id, self.task_id))
                self.logger.error("Dump of task status response: {}".format(task_status))
                self.status = State.TASK_FAILED
                return 1
            time_ = str(int(float(int(task_status["uptime"]) / 1000000000)))
            if status_ == "RUNNING":
                self.logger.info("Task {} on deal {} (Node {}) is running. Uptime is {} seconds"
                                 .format(self.task_id, self.deal_id, self.node_num, time_))
                self.task_uptime = time_
                return 60
            if status_ == "SPOOLING":
                self.logger.info("Task {} on deal {} (Node {}) is uploading..."
                                 .format(self.task_id, self.deal_id, self.node_num))
                self.status = State.STARTING_TASK
                return 60
            if status_ == "BROKEN":
                if int(time_) < self.config["ets"]:
                    self.logger.error("Task has failed ({} seconds) on deal {} (Node {}) before ETS."
                                      " Closing deal and blacklisting counterparty worker's address..."
                                      .format(time_, self.deal_id, self.node_num))
                    self.status = State.TASK_FAILED
                    return 1
                else:
                    self.logger.error("Task has failed ({} seconds) on deal {} (Node {}) after ETS."
                                      " Closing deal and recreate order..."
                                      .format(time_, self.deal_id, self.node_num))
                    self.status = State.TASK_BROKEN
                    return 1
            if status_ == "FINISHED":
                self.logger.info("Task {}  on deal {} (Node {} ) is finished. Uptime is {}  seconds"
                                 .format(self.task_id, self.deal_id, self.node_num, time_))
                self.logger.info("Task {}  on deal {} (Node {} ) success. Fetching log, shutting down node..."
                                 .format(self.task_id, self.deal_id, self.node_num))
                self.status = State.TASK_FINISHED
                return 1
        return 60

    @threaded
    def watch_node(self):
        sleep_time = 1
        while self.status != State.WORK_COMPLETED:
            if self.status == State.START or self.status == State.CREATE_ORDER:
                self.create_order()
                sleep_time = 60
            elif self.status == State.AWAITING_DEAL:
                sleep_time = self.check_order()
            elif self.status == State.DEAL_OPENED:
                self.start_task()
                sleep_time = 60
            elif self.status == State.DEAL_DISAPPEARED:
                self.status = State.CREATE_ORDER
                sleep_time = 1
            elif self.status == State.TASK_RUNNING:
                sleep_time = self.check_task_status()
            elif self.status == State.TASK_FAILED or self.status == State.TASK_FAILED_TO_START:
                self.close_deal(State.CREATE_ORDER, blacklist=True)
                sleep_time = 1
            elif self.status == State.TASK_BROKEN:
                self.close_deal(State.CREATE_ORDER)
                sleep_time = 1
            elif self.status == State.TASK_FINISHED:
                self.close_deal(State.WORK_COMPLETED)
                sleep_time = 1
            time.sleep(sleep_time if sleep_time else 60)

    def save_task_logs(self, prefix):
        self.cli.save_task_logs(self.deal_id, self.task_id, "1000000",
                                "{}{}-deal-{}.log".format(prefix, self.node_tag, self.deal_id))

    @staticmethod
    def dump_file(data, filename):
        with open(filename, 'w+') as file:
            yaml.dump(data, file, Dumper=yaml.RoundTripDumper)
