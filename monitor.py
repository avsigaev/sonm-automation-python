#!/usr/bin/env python3

import base64
import datetime
import errno
import json
import os
import platform
import re
import subprocess
import time
import threading

from ruamel import yaml
from ruamel.yaml import YAML
from pathlib2 import Path
from shutil import which

from yaml_gen import template_bid, template_task
from enum import Enum


class State(Enum):
    START = 0
    PLACING_ORDERS = 1
    WAIT_FOR_DEALS = 2
    WORK_WITH_DEALS = 3


def set_script_state(s):
    global STATE
    try:
        if STATE != s:
            log("State changed to " + str(s.name))
            STATE = s
    except NameError:
        STATE = s


def is_state_equal(state_):
    global STATE
    return state_ == STATE


def exec_cli(param, retry=False, attempts=3, sleep_time=1):
    command = [SONM_CLI] + param
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


def save_task_logs(deal_id, task_id, rownum, filename):
    command = [SONM_CLI, "task", "logs", deal_id, task_id, "--tail", rownum]
    with open(filename, "w") as outfile:
        subprocess.call(command, stdout=outfile)


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


def set_state():
    STATE_NODE.append(0)
    for number in range(CONFIG["numberofnodes"]):
        STATE_NODE.append(0)


def set_sonmcli():
    if platform.system() == "Darwin":
        return "sonmcli_darwin_x86_64"
    else:
        return "sonmcli"


def check_installed():
    if which(SONM_CLI) is None:
        print("sonm is not installed")
        exit(1)


def validate_eth_addr(eth_addr):
    pattern = re.compile("^0x[a-fA-F0-9]{40}$")
    if not pattern.match(eth_addr):
        log("Incorrect eth address or not specified")
        return None
    else:
        return eth_addr


def load_generator():
    counterparty = validate_eth_addr(CONFIG["counterparty"])
    for n in range(CONFIG["numberofnodes"]):
        number = n + 1
        ntag = CONFIG["tag"] + "_" + str(number)
        bid_ = template_bid(CONFIG, ntag, counterparty)
        bid_file = "out/orders/" + ntag + ".yaml"
        log("Creating order file Node number " + str(number))
        dump_file(bid_, bid_file)
        log("Creating task file for Node number " + str(number))
        task_ = template_task(ntag)
        dump_file(task_, "out/tasks/" + ntag + ".yaml")
        threading.Thread(target=create_order, kwargs={'bid_file': bid_file, 'node_num': number}, ).start()
        time.sleep(1)
    set_script_state(State.WAIT_FOR_DEALS)


def create_order(bid_file, node_num):
    log("Creating order for Node number " + str(node_num))
    order_ = exec_cli(["order", "create", bid_file])
    log("Order for Node " + str(node_num) + " is " + order_["id"])


def dump_file(data, filename):
    with open(filename, 'w+') as file:
        yaml.dump(data, file, Dumper=yaml.RoundTripDumper)


def check_orders(number_of_nodes):
    orders = exec_cli(["order", "list", "--timeout=2m", "--limit", str(number_of_nodes)])
    deals = exec_deal_list(number_of_nodes)
    if orders["orders"] is not None or is_state_equal(State.WAIT_FOR_DEALS):
        log("Waiting for deals...")
        time.sleep(10)
    elif deals["deals"] is None:
        log("No deals or orders found. Creating new orders...")
        set_script_state(State.PLACING_ORDERS)
        load_generator()


def check_state(number_of_nodes):
    STATE_NODE[0] = 1
    if 0 in STATE_NODE:
        check_orders(number_of_nodes)
    else:
        log("All tasks are finished")
        exit(0)


def get_deals(number_of_nodes):
    deal_list_output = exec_deal_list(number_of_nodes)
    if deal_list_output and deal_list_output['deals']:
        set_script_state(State.WORK_WITH_DEALS)
        for _, v in deal_list_output.items():
            return [d['id'] for d in v]
    else:
        check_state(number_of_nodes)


def exec_deal_list(number_of_nodes):
    return exec_cli(["deal", "list", "--timeout=2m", "--limit", str(number_of_nodes)])


def get_deal_tag_node_num(deal_id):
    deal_status = exec_cli(["deal", "status", deal_id, "--expand"])
    ntag = base64.b64decode(deal_status["bid"]["tag"]).decode().strip("\0")
    node_num = ntag.split("_")[len(ntag.split("_")) - 1]
    status = deal_status["deal"]["status"]
    return node_num, ntag, status


def blacklist(deal_id, node_num, ntag):
    exec_cli(["deal", "close", deal_id, "--blacklist", "worker"], retry=True)
    log("Node " + node_num + " failure, new order will be created...")
    create_new_order(node_num, ntag)


def create_new_order(node_num, ntag):
    bidfile_ = "out/orders/" + ntag + ".yaml"
    order = exec_cli(["order", "create", bidfile_])
    log("Order for Node " + node_num + " is " + order["id"])


def close_deal(deal_id):
    closed = exec_cli(["deal", "close", deal_id], retry=True)
    if "id" in closed[0]:
        log("Closed deal " + deal_id)


def task_manager(deal_id, task_id, node_num, ntag):
    task_status = exec_cli(["task", "status", deal_id, task_id, "--timeout=2m"], retry=True)
    if not task_status:
        return close_deal_and_create_order(deal_id, node_num, ntag, task_id)
    status_ = task_status["status"]
    time_ = str(int(float(int(task_status["uptime"]) / 1000000000)))
    if status_ == "SPOOLING":
        log("Task " + task_id + " on deal " + deal_id + " (Node " + node_num + ") is uploading...")
    if status_ == "RUNNING":
        log("Task " + task_id + " on deal " + deal_id + " (Node " + node_num +
            ") is running. Uptime is " + time_ + " seconds")
    if status_ == "BROKEN" or status_ == "FINISHED":
        if int(time_) > CONFIG["eta"]:
            log("Task " + task_id + "  on deal " + deal_id + " (Node " + node_num +
                " ) is finished. Uptime is " + time_ + "  seconds")
            log("Task " + task_id + "  on deal " + deal_id + " (Node " + node_num +
                " ) success. Fetching log, shutting down node...")
            save_task_logs(deal_id, task_id, "1000000", "out/success_" + ntag + "-deal-" + deal_id + ".log")

            log("Closing deal " + deal_id + " ...")
            close_deal(deal_id)
            STATE_NODE[int(node_num)] = 1
            time.sleep(5)
        else:
            log("Task has failed/stopped (" + time_ + " seconds) on deal " + deal_id + " (Node " + node_num +
                ") before ETA." + " Closing deal and blacklisting counterparty worker's address...")
            save_task_logs(deal_id, task_id, "1000000", "out/fail_" + ntag + "-deal-" + deal_id + ".log")
            blacklist(deal_id, node_num, ntag)


def close_deal_and_create_order(deal_id, node_num, ntag, task_id=""):
    _, _, status = get_deal_tag_node_num(deal_id)
    if status == 1:
        log("Worker cannot retrieve task status " + task_id + " on deal " + deal_id +
            " (Node " + node_num + "), closing deal")
        close_deal(deal_id)
    if status == 2:
        log("Deal " + deal_id + " (Node " + node_num + ") has gone away")
    log("Recreating order for Node " + node_num)
    create_new_order(node_num, ntag)
    return


def start_task_on_deal(deal_id, task_file, node_num, ntag):
    task = exec_cli(["task", "start", deal_id, task_file, "--timeout=15m"], retry=True)
    if not task:
        log("Failed to start task on deal " + deal_id +
            ". Closing deal and blacklisting counterparty worker's address...")
        blacklist(deal_id, node_num, ntag)
    else:
        log("Task started: deal " + deal_id + " with task_id " + task["id"])


def task_valid(deal_id, task_state):
    node_num, ntag, status = get_deal_tag_node_num(deal_id)
    task_list = exec_cli(["task", "list", deal_id, "--timeout=2m"], retry=True)
    if task_list and len(task_list.keys()) > 0:
        if "error" in task_list.keys() or "message" in task_list.keys():
            return close_deal_and_create_order(deal_id, node_num, ntag)
        task_id = list(task_list.keys())[0]
        task_manager(deal_id, task_id, node_num, ntag)
        return 1

    if task_state == 0:
        log("Starting task on node " + str(node_num) + "...")
        task_file = "out/tasks/" + ntag + ".yaml"
        threading.Thread(target=start_task_on_deal,
                         kwargs={'deal_id': deal_id, 'task_file': task_file,
                                 'node_num': node_num, 'ntag': ntag}).start()
        return 1
    log("Task on deal " + deal_id + " (Node " + node_num + ") is still starting...")


def deal_manager(task_state=None):
    if task_state is None:
        task_state = {}
    number_of_nodes = CONFIG["numberofnodes"]
    deal_ids = get_deals(number_of_nodes)
    if deal_ids is not None:
        for deal_id in deal_ids:
            task_state[deal_id] = task_valid(deal_id, task_state.get(deal_id, 0))
    time.sleep(10)
    deal_manager(task_state)


def watch():
    log("Watching cluster...")
    deal_manager()


def init():
    create_dir("out/orders")
    create_dir("out/tasks")

    global CONFIG, STATE_NODE, SONM_CLI, STATE
    set_script_state(State.START)
    STATE_NODE = []
    CONFIG = load_cfg()
    set_state()
    SONM_CLI = set_sonmcli()
    check_installed()


init()
watch()
