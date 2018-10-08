import datetime
import errno
import json
import os
import subprocess
import time
from enum import Enum

from pathlib2 import Path
from ruamel.yaml import YAML


class State(Enum):
    START = 0
    PLACING_ORDERS = 1
    WAIT_FOR_DEALS = 2
    WORK_WITH_DEALS = 3
    FINISH = 4


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
        order_ = self.exec(["order", "create", bid_file])
        return order_

    def order_list(self, number_of_nodes):
        orders = self.exec(["order", "list", "--timeout=2m", "--limit", str(number_of_nodes)])
        return orders

    def deal_list(self, number_of_nodes):
        deal_list = self.exec(["deal", "list", "--timeout=2m", "--limit", str(number_of_nodes)])
        return deal_list

    def deal_status(self, deal_id):
        deal_status = self.exec(["deal", "status", deal_id, "--expand"])
        return deal_status

    def deal_close(self, deal_id):
        close_d_command = ["deal", "close", deal_id, "--blacklist", "worker"]
        adsasd = self.exec(close_d_command, retry=True)
        return adsasd

    def task_status(self, deal_id, task_id):
        task_status = self.exec(["task", "status", deal_id, task_id, "--timeout=2m"], retry=True)
        return task_status

    def task_start(self, deal_id, task_file):
        task = self.exec(["task", "start", deal_id, task_file, "--timeout=15m"], retry=True)
        return task

    def task_list(self, deal_id):
        task_list = self.exec(["task", "list", deal_id, "--timeout=2m"], retry=True)
        return task_list


class Node:
    def __init__(self, cli_):
        self.cli = cli_


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


def init():
    create_dir("out/orders")
    create_dir("out/tasks")

    CONFIG = load_cfg()

    global CONFIG, STATE_NODE, SONM_CLI, STATE
    set_script_state(State.START)
    STATE_NODE = []
    set_state()
    SONM_CLI = Cli(set_sonmcli())
    # check_installed()


def watch():
    pass


def main():
    init()
    watch()


if __name__ == "__main__":
    main()
