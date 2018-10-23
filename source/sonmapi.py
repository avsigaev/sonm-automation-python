import json
import logging
import subprocess
import time


def retry_on_status(fn, retry=True, attempts=3, sleep_time=3):
    def wrapper(*args, **kwargs):
        attempt = 1
        while True:
            r = fn(*args, **kwargs)
            if "status_code" in r and r["status_code"] == 200:
                break
            if not retry or attempt > attempts:
                break
            attempt += 1
            time.sleep(sleep_time)
        return r

    return wrapper


class SonmApi:
    def __init__(self, cli, node):
        self.cli = cli
        self.node = node
        self.logger = logging.getLogger("monitor")

    @classmethod
    def only_cli(cls, cli):
        return cls(cli, None)

    def get_node(self):
        if self.node:
            return self.node
        else:
            raise Exception("Sonm node api not initialized")

    def exec(self, param, retry=False, attempts=3, sleep_time=1):
        command = [self.cli] + param
        command.append("--json")
        attempt = 1
        errors_ = []
        while True:
            result = subprocess.run(command, stdout=subprocess.PIPE)
            if result.returncode == 0:
                break
            errors_.append(str(result.stdout.decode("utf-8")))
            if not retry or attempt > attempts:
                break
            attempt += 1
            time.sleep(sleep_time)
        if result.returncode != 0:
            self.logger.error("Failed to execute command: {}".format(' '.join(command)))
            self.logger.error('\n'.join(errors_))
            return None
        if result.stdout.decode("utf-8") == "null":
            return {}
        return json.loads(result.stdout.decode("utf-8"))

    def save_task_logs(self, deal_id, task_id, rownum, filename):
        command = [self.cli, "task", "logs", deal_id, task_id, "--tail", rownum]
        with open(filename, "w") as outfile:
            subprocess.call(command, stdout=outfile)

    def order_create(self, bid_file):
        result = None
        create_order = self.exec(["order", "create", bid_file])
        if create_order:
            result = {"id": create_order["id"]}
        return result

    def order_list(self, number_of_nodes):
        order_list_ = self.exec(["order", "list", "--timeout=2m", "--limit", str(number_of_nodes)], retry=True)
        orders_ = None
        if order_list_ and order_list_["orders"] is not None:
            orders_ = [{"id": order["id"],
                        "tag": order["tag"],
                        "price": order["price"]}
                       for order in list(order_list_["orders"])]
        return {"orders": orders_}

    def order_status(self, order_id):
        order_status_ = self.exec(["order", "status", str(order_id)], retry=True)
        return {"orderStatus": order_status_["orderStatus"], "dealID": order_status_["dealID"]}

    def deal_list(self, number_of_nodes):
        result = []
        deal_list_ = self.exec(["deal", "list", "--timeout=2m", "--limit", str(number_of_nodes)], retry=True)
        if deal_list_ and deal_list_['deals']:
            for d in [d_["deal"] for d_ in deal_list_['deals']]:
                result.append({"id": d["id"]})
        return result

    def deal_status(self, deal_id):
        result = None
        deal_status_ = self.exec(["deal", "status", deal_id, "--expand"], retry=True)
        if deal_status_ and "deal" in deal_status_:
            result = {"status": deal_status_["deal"]["status"],
                      "bid_tag": deal_status_["bid"]["tag"],
                      "bid_id": deal_status_["bid"]["id"],
                      "has_running": False,
                      "running": [],
                      "worker_offline": True,
                      "bid_price": deal_status_["bid"]["price"]}
            if "running" in deal_status_:
                result["has_running"] = (len(deal_status_["running"].keys()) > 0)
                result["running"] = list(deal_status_["running"])
            if "resources" in deal_status_:
                result["worker_offline"] = False

        return result

    def deal_close(self, deal_id, bl_worker=False):
        close_d_command = ["deal", "close", deal_id]
        if bl_worker:
            close_d_command += ["--blacklist", "worker"]
        result = None
        close_deal = self.exec(close_d_command, retry=True)
        if close_deal:
            result = {}
        return result

    def task_status(self, deal_id, task_id):
        result = None
        task_status_ = self.exec(["task", "status", deal_id, task_id, "--timeout=2m"], retry=True)
        if task_status_ and "status" in task_status_:
            result = {"status": task_status_["status"],
                      "uptime": str(int(float(int(task_status_["uptime"]) / 1000000000)))}
        return result

    def task_start(self, deal_id, task_file):
        result = None
        task_start = self.exec(["task", "start", deal_id, task_file, "--timeout=15m"], retry=True)
        if task_start:
            result = {"id": task_start["id"]}
        return result

    @retry_on_status
    def predict_bid(self, bid_):
        return self.get_node().predictor.predict(bid_)

    def task_list(self, deal_id, attempts=10, sleep_time=20):
        result = None
        attempt = 1
        while True:
            resp = self.exec(["task", "list", deal_id, "--timeout=2m"], retry=True)
            if resp and "error" in resp.keys():
                if attempt > attempts:
                    self.logger.error("Received response: {}".format(resp))
                    break
                self.logger.error("Attempt {}, deal id {}  received response: {}".format(attempt, deal_id, resp))
                attempt += 1
                time.sleep(sleep_time)
                continue
            break
        if resp and len(resp.keys()) > 0:
            result = resp
        return result
