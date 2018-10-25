import logging
import time
from enum import Enum
from os.path import join

import yaml

from source.utils import threaded, Config, template_bid, template_task, convert_price, TaskStatus, dump_file


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


class WorkNode:
    def __init__(self, status, sonm_api, node_tag, deal_id, task_id, bid_id, price):
        self.logger = logging.getLogger("monitor")
        self.node_tag = node_tag
        self.config = Config.get_node_config(self.node_tag)
        self.status = status
        self.sonm_api = sonm_api
        self.bid_file = "out/orders/{}.yaml".format(self.node_tag)
        self.task_file = "out/tasks/{}.yaml".format(self.node_tag)
        self.bid_ = {}
        self.task_ = {}
        self.deal_id = deal_id
        self.task_id = task_id
        self.bid_id = bid_id
        self.price = "{0:.4f} USD/h".format(convert_price(price)) if price != "" else ""
        self.task_uptime = 0
        self.create_task_yaml()

    @classmethod
    def create_empty(cls, sonm_api, node_tag):
        return cls(State.START, sonm_api, node_tag, "", "", "", "")

    def reload_config(self):
        Config.reload_config(self.node_tag)
        return Config.get_node_config(self.node_tag)

    def create_task_yaml(self):
        self.logger.info("Creating task file for Node {}".format(self.node_tag))
        file_ = join(Config.config_folder, self.config["template_file"])
        dump_file(template_task(file_, self.node_tag), self.task_file)
        with open(self.task_file) as f:
            self.task_ = yaml.safe_load(f)

    def create_bid_yaml(self):
        self.logger.info("Creating order file for Node {}".format(self.node_tag))
        self.bid_ = template_bid(self.config, self.node_tag, self.config["counterparty"])

        price_, predicted_ = self.get_price(self.bid_)
        self.price = self.format_price(price_, readable=True)
        self.bid_["price"] = self.format_price(price_)

        self.logger.info("Predicted price for Node {} is {:.4f} USD/h, order price is {}"
                         .format(self.node_tag, predicted_, self.price))
        dump_file(self.bid_, self.bid_file)

    def get_price(self, bid_):
        predicted_price = self.sonm_api.predict_bid(bid_["resources"])
        result_price = self.config["max_price"]
        price_ = 0
        if predicted_price:
            price_ = predicted_price["perHourUSD"] * (1 + int(self.config["price_coefficient"]) / 100)
            if price_ < float(self.config["max_price"]):
                result_price = price_
        return result_price, price_

    def create_order(self):
        self.reload_config()
        self.create_bid_yaml()
        self.status = State.PLACING_ORDER
        self.logger.info("Create order for Node {}".format(self.node_tag))
        create_order = self.sonm_api.order_create(self.bid_)
        if not create_order:
            raise Exception("Cannot create order. Check sonm-node status or your balance")
        self.bid_id = create_order["id"]
        self.status = State.AWAITING_DEAL
        self.logger.info("Order for Node {} is {}".format(self.node_tag, self.bid_id))

    def check_order(self):
        order_status = self.sonm_api.order_status(self.bid_id)
        self.logger.info("Checking order {} (Node {}) for new deal".format(self.bid_id, self.node_tag))
        if order_status and order_status["orderStatus"] == 1 and order_status["dealID"] != "0":
            self.deal_id = order_status["dealID"]
            self.status = State.DEAL_OPENED
            self.logger.info("For order {} (Node {}) opened new deal {}"
                             .format(self.bid_id, self.node_tag, self.deal_id))
            return 15
        elif order_status and order_status["orderStatus"] == 1 and order_status["dealID"] == "0":
            self.logger.info("Order {} was cancelled (Node {}), create new order".format(self.bid_id, self.node_tag))
            self.bid_id = ""
            self.status = State.CREATE_ORDER
            return 1
        return 60

    def start_task(self):
        # Start task on node
        self.status = State.STARTING_TASK
        self.logger.info("Starting task on node {} ...".format(self.node_tag))
        task = self.sonm_api.task_start(self.deal_id, self.task_)
        if not task:
            self.logger.info("Failed to start task (Node {}) on deal {}. Closing deal and blacklisting counterparty "
                             "worker's address...".format(self.node_tag, self.deal_id))
            self.status = State.TASK_FAILED_TO_START
        else:
            self.logger.info("Task (Node {}) started: deal {} with task_id {}"
                             .format(self.node_tag, self.deal_id, task["id"]))
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
        deal_status = self.sonm_api.deal_status(self.deal_id)
        if deal_status and deal_status["status"] == 2:
            self.logger.error("Deal {} (Node {}) already closed".format(self.deal_id, self.node_tag))
        else:
            self.sonm_api.deal_close(self.deal_id, blacklist)
            self.logger.info("Deal {} was closed".format(self.deal_id))
        self.deal_id = ""
        self.bid_id = ""
        self.task_uptime = 0
        self.task_id = ""
        self.status = state_after

    def check_task_status(self):
        deal_status = self.sonm_api.deal_status(self.deal_id)
        if deal_status and deal_status["status"] == 2:
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

        task_status = self.sonm_api.task_status(self.deal_id, self.task_id)
        if not task_status:
            self.logger.error("Cannot retrieve task status of deal {},"
                              " task_id {} worker is offline?".format(self.deal_id, self.task_id))
            self.status = State.TASK_FAILED
            return 1
        time_ = task_status["uptime"]
        if task_status["status"] == TaskStatus.running.value:
            self.logger.info("Task {} on deal {} (Node {}) is running. Uptime is {} seconds"
                             .format(self.task_id, self.deal_id, self.node_tag, time_))
            self.task_uptime = time_
            return 60
        if task_status["status"] == TaskStatus.spooling.value:
            self.logger.info("Task {} on deal {} (Node {}) is uploading..."
                             .format(self.task_id, self.deal_id, self.node_tag))
            self.status = State.STARTING_TASK
            return 60
        if task_status["status"] == TaskStatus.broken.value:
            if int(time_) < self.config["ets"]:
                self.logger.error("Task has failed ({} seconds) on deal {} (Node {}) before ETS."
                                  " Closing deal and blacklisting counterparty worker's address..."
                                  .format(time_, self.deal_id, self.node_tag))
                self.status = State.TASK_FAILED_TO_START
                return 1
            else:
                self.logger.error("Task has failed ({} seconds) on deal {} (Node {}) after ETS."
                                  " Closing deal and recreate order..."
                                  .format(time_, self.deal_id, self.node_tag))
                self.status = State.TASK_BROKEN
                return 1
        if task_status["status"] == TaskStatus.finished.value:
            self.logger.info("Task {}  on deal {} (Node {} ) is finished. Uptime is {}  seconds"
                             .format(self.task_id, self.deal_id, self.node_tag, time_))
            self.logger.info("Task {}  on deal {} (Node {} ) success. Fetching log, shutting down node..."
                             .format(self.task_id, self.deal_id, self.node_tag))
            self.status = State.TASK_FINISHED
            return 1
        return 60

    @threaded
    def watch_node(self):
        try:
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
                elif self.status == State.TASK_FAILED_TO_START:
                    self.close_deal(State.CREATE_ORDER, blacklist=True)
                    sleep_time = 1
                elif self.status == State.TASK_FAILED:
                    self.close_deal(State.CREATE_ORDER)
                    sleep_time = 1
                elif self.status == State.TASK_BROKEN:
                    self.close_deal(State.CREATE_ORDER)
                    sleep_time = 1
                elif self.status == State.TASK_FINISHED:
                    self.close_deal(State.WORK_COMPLETED)
                    sleep_time = 1
                time.sleep(sleep_time if sleep_time else 60)
        except Exception as exc:
            self.logger.exception("Node {} failed with exception".format(self.node_tag), exc)

    def save_task_logs(self, prefix):
        self.sonm_api.task_logs(self.deal_id, self.task_id, "1000000",
                                "{}{}-deal-{}.log".format(prefix, self.node_tag, self.deal_id))

    @staticmethod
    def format_price(price_, readable=False):
        return "{0:.4f}{1}USD/h".format(float(price_), " " if readable else "")
