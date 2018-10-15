from concurrent.futures import Future
from enum import Enum
from threading import Thread

from ruamel import yaml

from source.log import log
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
        self.node_tag = tag + "_" + self.node_num
        self.bid_file = "out/orders/" + self.node_tag + ".yaml"
        self.task_file = "out/tasks/" + self.node_tag + ".yaml"
        self.deal_id = deal_id
        self.task_id = task_id
        self.bid_id = bid_id
        self.config = config
        self.counterparty = counterparty

    @classmethod
    def create_empty(cls, cli_, node_num, tag, config, counterparty):
        return cls(State.START, cli_, node_num, tag, "", "", "", config, counterparty)

    def create_yaml(self):
        bid_ = template_bid(self.config, self.node_tag, self.counterparty)
        task_ = template_task(self.config["template_file"], self.node_tag)
        log("Creating order file Node number " + str(self.node_num))
        self.dump_file(bid_, self.bid_file)
        log("Creating task file for Node number " + str(self.node_num))
        self.dump_file(task_, self.task_file)
        self.status = State.CREATE_ORDER

    @threaded
    def create_order(self):
        self.status = State.PLACING_ORDER
        log("Create order for Node " + str(self.node_num))
        self.bid_id = self.cli.order_create(self.bid_file)["id"]
        self.status = State.AWAITING_DEAL
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
            self.status = State.TASK_RUNNING

    @threaded
    def close_deal(self, state_after, blacklist=False):
        # Close deal on node
        log("Saving logs deal_id " + self.deal_id + " task_id " + self.task_id)
        if self.status == State.TASK_FAILED:
            self.save_task_logs("out/fail_")
        if self.status == State.TASK_FINISHED:
            self.save_task_logs("out/success_")
        log("Closing deal " + self.deal_id + (" with blacklisting worker" if blacklist else "") + " ...")
        self.cli.deal_close(self.deal_id, blacklist)
        self.status = state_after

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
            if status_ == "BROKEN":
                if int(time_) < self.config["ets"]:
                    log("Task has failed/stopped (" + time_ + " seconds) on deal " + self.deal_id +
                        " (Node " + self.node_num + ") before ETS." +
                        " Closing deal and blacklisting counterparty worker's address...")
                    self.status = State.TASK_FAILED
                else:
                    log("Task has failed/stopped (" + time_ + " seconds) on deal " + self.deal_id +
                        " (Node " + self.node_num + ") before ETA." +
                        " Closing deal and recreate order...")
                    self.status = State.TASK_BROKEN
            if status_ == "FINISHED":
                log("Task " + self.task_id + "  on deal " + self.deal_id + " (Node " + self.node_num +
                    " ) is finished. Uptime is " + time_ + "  seconds")
                log("Task " + self.task_id + "  on deal " + self.deal_id + " (Node " + self.node_num +
                    " ) success. Fetching log, shutting down node...")
                self.status = State.TASK_FINISHED

    def save_task_logs(self, prefix):
        self.cli.save_task_logs(self.deal_id, self.task_id, "1000000",
                                prefix + self.node_tag + "-deal-" + self.deal_id + ".log")

    def dump_file(self, data, filename):
        with open(filename, 'w+') as file:
            yaml.dump(data, file, Dumper=yaml.RoundTripDumper)
