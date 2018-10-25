import functools
import logging
import subprocess
import time

from pytimeparse.timeparse import timeparse
from sonm_pynode.main import Node

from source.utils import convert_price, parse_tag, parse_price, Identity, get_sonmcli


def retry_on_status(_func=None, *, attempts=3, sleep_time=3):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            attempt = 1
            while True:
                r = fn(*args, **kwargs)
                if "status_code" in r and r["status_code"] == 200:
                    return r
                if attempt > attempts:
                    break
                attempt += 1
                time.sleep(sleep_time)
            return None

        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)


def check_status(fn):
    def wrapper(*args, **kwargs):
        r = fn(*args, **kwargs)
        if "status_code" in r and r["status_code"] == 200:
            return r
        return None

    return wrapper


class SonmApi:
    def __init__(self, key_file: str, password: str, endpoint: str):
        self.node = Node(key_file, password, endpoint)
        self.logger = logging.getLogger("monitor")

    def get_node(self):
        if self.node:
            return self.node
        else:
            raise Exception("Sonm node api not initialized")

    def order_create(self, order):
        result = None
        if len([key for key in ["duration", "price", "identity"] if key not in order]) > 0:
            raise Exception("Bid order must have all this keys: duration, price, identity")
        order["duration"] = {"nanoseconds": int(timeparse(order["duration"]) * 1e9)}
        order["price"] = {"perSecond": str(parse_price(order["price"]))}
        order["identity"] = Identity[order["identity"]].value
        create_order = self.order_create_rest(order)
        if create_order:
            result = {"id": create_order["id"]}
        return result

    def order_list(self, limit):
        order_list_ = self.order_list_rest(limit)
        orders_ = None
        if order_list_ and "orders" in order_list_ is not None:
            orders_ = [{"id": order["order"]["id"],
                        "tag": parse_tag(order["order"]["tag"]),
                        "price": order["order"]["price"]}
                       for order in list(order_list_["orders"])]
        return {"orders": orders_}

    def order_status(self, order_id):
        result = None
        order_status_ = self.order_status_rest(order_id)
        if order_status_:
            result = {"orderStatus": order_status_["orderStatus"],
                      "tag": parse_tag(order_status_["tag"]),
                      "dealID": order_status_["dealID"]}
        return result

    def deal_list(self, limit):
        result = []
        deal_list_ = self.deal_list_rest(limit)
        if deal_list_ and "deals" in deal_list_:
            for d in [d_["deal"] for d_ in deal_list_['deals']]:
                result.append({"id": d["id"]})
        return result

    def deal_status(self, deal_id):
        result = None
        deal_status = self.deal_status_rest(deal_id)
        if deal_status and "deal" in deal_status:
            deal_status_ = deal_status["deal"]
            result = {"status": deal_status_["status"],
                      "bid_id": deal_status_["bidID"],
                      "running": None,
                      "worker_offline": True,
                      "price": deal_status_["price"]}
            if "running" in deal_status:
                result["running"] = list(deal_status["running"])
            if "resources" in deal_status:
                result["worker_offline"] = False
        return result

    def deal_close(self, deal_id, bl_worker=False):
        result = None
        close_deal = self.deal_close_rest(deal_id, bl_worker)
        if close_deal:
            result = {}
        return result

    def task_status(self, deal_id, task_id):
        result = None
        task_status_ = self.task_status_rest(deal_id, task_id)
        if task_status_ and "status" in task_status_:
            result = {"status": task_status_["status"],
                      "uptime": str(int(float(int(task_status_["uptime"]) / 1e9)))}
        return result

    def task_start(self, deal_id, task):
        result = None
        task_start = self.task_start_rest(deal_id, task)
        if task_start:
            result = {"id": task_start["id"]}
        return result

    def predict_bid(self, bid_):
        result = None
        predict_ = self.predict_bid_rest(bid_)
        if predict_ and "perSecond" in predict_:
            result = {"perHourUSD": convert_price(predict_["perSecond"])}
        return result

    @retry_on_status
    def predict_bid_rest(self, bid_):
        return self.get_node().predictor.predict(bid_)

    @retry_on_status
    def deal_status_rest(self, deal_id):
        return self.get_node().deal.status(deal_id)

    @retry_on_status
    def deal_list_rest(self, limit):
        filters = {"status": 1,
                   "consumerID": self.get_node().eth_addr,
                   "limit": limit}
        return self.get_node().deal.list(filters)

    @retry_on_status
    def deal_close_rest(self, deal_id, blacklist):
        return self.get_node().deal.close(deal_id, blacklist)

    @retry_on_status
    def order_create_rest(self, order):
        return self.get_node().order.create(order)

    @retry_on_status
    def order_list_rest(self, limit):
        return self.get_node().order.list(self.get_node().eth_addr, limit)

    @retry_on_status
    def order_status_rest(self, order_id):
        return self.get_node().order.status(order_id)

    @retry_on_status(attempts=10, sleep_time=10)
    def task_status_rest(self, deal_id, task_id):
        return self.get_node().task.status(deal_id, task_id)

    @check_status
    def task_start_rest(self, deal_id, task):
        return self.get_node().task.start(deal_id, task, timeout=900)

    @staticmethod
    def task_logs(deal_id, task_id, rownum, filename):
        command = [get_sonmcli(), "task", "logs", deal_id, task_id, "--tail", rownum]
        with open(filename, "w") as outfile:
            subprocess.call(command, stdout=outfile)
