import logging
from concurrent.futures import Future
from enum import Enum
from threading import Thread

from ruamel import yaml

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


def call_with_future(fn, future, args, kwargs):
    try:
        result = fn(*args, **kwargs)
        future.set_result(result)
    except Exception as exc:
        future.set_exception(exc)


def threaded(fn):
    def wrapper(*args, **kwargs):
        future = Future()
        Thread(target=call_with_future, args=(fn, future, args, kwargs)).start()
        return future

    return wrapper


class Node:
    def __init__(self, status, cli_, node_num, tag, deal_id, task_id, bid_id, config, counterparty):
        self.status = status
        self.cli = cli_
        self.node_num = str(node_num)
        self.node_tag = "{}_{}".format(tag, self.node_num)
        self.bid_file = "out/orders/{}.yaml".format(self.node_tag)
        self.task_file = "out/tasks/{}.yaml".format(self.node_tag)
        self.deal_id = deal_id
        self.task_id = task_id
        self.bid_id = bid_id
        self.config = config
        self.counterparty = counterparty
        self.logger = logging.getLogger("monitor")
        self.task_uptime = 0

    @classmethod
    def create_empty(cls, cli_, node_num, tag, config, counterparty):
        return cls(State.START, cli_, node_num, tag, "", "", "", config, counterparty)

    def create_yaml(self):
        bid_ = template_bid(self.config, self.node_tag, self.counterparty)
        task_ = template_task(self.config["template_file"], self.node_tag)
        self.logger.info("Creating order file Node number {}".format(self.node_num))
        self.dump_file(bid_, self.bid_file)
        self.logger.info("Creating task file for Node number {}".format(self.node_num))
        self.dump_file(task_, self.task_file)
        self.status = State.CREATE_ORDER

    @threaded
    def create_order(self):
        self.status = State.PLACING_ORDER
        self.logger.info("Create order for Node {}".format(self.node_num))
        self.bid_id = self.cli.order_create(self.bid_file)["id"]
        self.status = State.AWAITING_DEAL
        self.logger.info("Order for Node {} is {}".format(self.node_num, self.bid_id))

    @threaded
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

    @threaded
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
        self.status = state_after

    @threaded
    def check_task_status(self):
        task_list = self.cli.task_list(self.deal_id)
        if task_list and len(task_list.keys()) > 0:
            if "error" in task_list.keys() or "message" in task_list.keys():
                if self.cli.deal_status(self.deal_id)["deal"]["status"] == 2:
                    self.logger.info("Deal {} was closed".format(self.deal_id))
                    self.status = State.DEAL_DISAPPEARED
                    return
                self.logger.error("Cannot retrieve task list of deal {}, worker is offline?".format(self.deal_id))
                self.logger.error("Dump task list response: {}".format(task_list))
                self.status = State.TASK_FAILED
                return
            task_status = self.cli.task_status(self.deal_id, self.task_id)
            if task_status and "status" in task_status:
                status_ = task_status["status"]
            else:
                if self.cli.deal_status(self.deal_id)["deal"]["status"] == 2:
                    self.logger.info("Deal {} was closed".format(self.deal_id))
                    self.status = State.DEAL_DISAPPEARED
                    return
                else:
                    self.logger.error("Cannot retrieve task status of deal {},"
                                      " task_id {} worker is offline?".format(self.deal_id, self.task_id))
                    self.logger.error("Dump of task status response: {}".format(task_status))
                    self.status = State.TASK_FAILED
                    return
            time_ = str(int(float(int(task_status["uptime"]) / 1000000000)))
            if status_ == "RUNNING":
                self.logger.info("Task {} on deal {} (Node {}) is running. Uptime is {} seconds"
                                 .format(self.task_id, self.deal_id, self.node_num, time_))
                self.task_uptime = time_
            if status_ == "SPOOLING":
                self.logger.info("Task {} on deal {} (Node {}) is uploading..."
                                 .format(self.task_id, self.deal_id, self.node_num))
                self.status = State.STARTING_TASK
            if status_ == "BROKEN":
                if int(time_) < self.config["ets"]:
                    self.logger.error("Task has failed ({} seconds) on deal {} (Node {}) before ETS."
                                      " Closing deal and blacklisting counterparty worker's address..."
                                      .format(time_, self.deal_id, self.node_num))
                    self.status = State.TASK_FAILED
                else:
                    self.logger.error("Task has failed ({} seconds) on deal {} (Node {}) after ETS."
                                      " Closing deal and recreate order..."
                                      .format(time_, self.deal_id, self.node_num))
                    self.status = State.TASK_BROKEN
            if status_ == "FINISHED":
                self.logger.info("Task {}  on deal {} (Node {} ) is finished. Uptime is {}  seconds"
                                 .format(self.task_id, self.deal_id, self.node_num, time_))
                self.logger.info("Task {}  on deal {} (Node {} ) success. Fetching log, shutting down node..."
                                 .format(self.task_id, self.deal_id, self.node_num))
                self.status = State.TASK_FINISHED

    def save_task_logs(self, prefix):
        self.cli.save_task_logs(self.deal_id, self.task_id, "1000000",
                                "{}{}-deal-{}.log".format(prefix, self.node_tag, self.deal_id))

    def dump_file(self, data, filename):
        with open(filename, 'w+') as file:
            yaml.dump(data, file, Dumper=yaml.RoundTripDumper)
