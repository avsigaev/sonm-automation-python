import base64
import errno
import json
import logging
import os
import platform
import re
from concurrent.futures import Future
from enum import Enum
from os.path import join
from threading import Thread

import ruamel.yaml
from jinja2 import Template

from pathlib2 import Path
from ruamel.yaml import YAML
from tabulate import tabulate

logger = logging.getLogger("monitor")


class Identity(Enum):
    unknown = 0
    anonymous = 1
    registered = 2
    identified = 3
    professional = 4


class TaskStatus(Enum):
    unknown = 0
    spooling = 1
    spawning = 2
    running = 3
    finished = 4
    broken = 5


class Nodes(object):
    nodes_ = []

    @staticmethod
    def get_nodes():
        Nodes.nodes_.sort(key=lambda x: natural_keys(x.node_tag))
        return Nodes.nodes_


class Config(object):
    base_config = {}
    node_configs = {}
    config_folder = "conf/"

    @staticmethod
    def get_node_config(node_tag):
        return Config.node_configs.get(node_tag)

    @staticmethod
    def load_config():
        Config.base_config = Config.load_cfg()
        config_keys = ["node_address", "ethereum", "tasks"]
        missed_keys = [key for key in config_keys if key not in Config.base_config]
        if len(missed_keys) > 0:
            raise Exception("Missed keys: '{}'".format("', '".join(missed_keys)))

        logger.debug("Try to parse configs:")
        for task in Config.base_config["tasks"]:
            task_config = Config.load_cfg(task)
            for num in range(1, task_config["numberofnodes"] + 1):
                task_config["counterparty"] = validate_eth_addr(task_config["counterparty"])
                ntag = "{}_{}".format(task_config["tag"], num)
                Config.node_configs[ntag] = task_config
                logger.debug("Config for node {} was created successfully".format(ntag))
                logger.debug("Config: {}".format(json.dumps(task_config, sort_keys=True, indent=4)))

    @staticmethod
    def reload_config(node_tag):
        Config.base_config = Config.load_cfg()
        for task in Config.base_config["tasks"]:
            task_config = Config.load_cfg(task)
            if node_tag.startswith(task_config["tag"] + "_"):
                Config.node_configs[node_tag] = task_config

    @staticmethod
    def load_cfg(filename='config.yaml', folder=config_folder):
        path = join(folder, filename)
        if os.path.exists(path):
            p = Path(path)
            yaml_ = YAML(typ='safe')
            return yaml_.load(p)
        else:
            raise Exception("File {} not found".format(filename))


def atoi(text):
    return int(text) if text.isdigit() else text


def natural_keys(text):
    return [atoi(c) for c in re.split("(\d+)", text)]


def parse_tag(order_):
    return base64.b64decode(order_).decode().strip("\0")


def create_dir(dir_):
    if not os.path.exists(dir_):
        try:
            os.makedirs(dir_)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def convert_price(price_):
    return int(price_) / 1e18 * 3600


def parse_price(price_: str):
    if price_.endswith("USD/h") or price_.endswith("USD/s"):
        return int(float(price_[:-5]) * 1e18 / 3600)
    else:
        raise Exception("Cannot parse price {}".format(price_))


def set_sonmcli():
    if platform.system() == "Darwin":
        return "sonmcli_darwin_x86_64"
    else:
        return "sonmcli"


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


def validate_eth_addr(eth_addr):
    pattern = re.compile("^0x[a-fA-F0-9]{40}$")
    if eth_addr and pattern.match(eth_addr):
        logger.debug("Eth address was parsed successfully: " + eth_addr)
        return eth_addr
    else:
        logger.debug("Incorrect eth address or not specified")
        return None


def print_state():
    tabul_nodes = [[n.node_tag, n.bid_id, n.price, n.deal_id, n.task_id, n.task_uptime, n.status.name] for n in
                   Nodes.get_nodes()]
    logger.info("Nodes:\n" +
                tabulate(tabul_nodes,
                         ["Node", "Order id", "Order price", "Deal id", "Task id", "Task uptime", "Node status"],
                         tablefmt="grid"))


def template_bid(config, tag, counterparty=None):
    gpumem = config["gpumem"]
    ethhashrate = config["ethhashrate"]
    if config["gpucount"] == 0:
        gpumem = 0
        ethhashrate = 0
    bid_template = {
        "duration": config["duration"],
        "price": "0USD/h",
        "identity": config["identity"],
        "tag": tag,
        "resources": {
            "network": {
                "overlay": config["overlay"],
                "outbound": True,
                "incoming": config["incoming"]
            },
            "benchmarks": {
                "ram-size": config["ramsize"] * 1024 * 1024,
                "storage-size": config["storagesize"] * 1024 * 1024 * 1024,
                "cpu-cores": config["cpucores"],
                "cpu-sysbench-single": config["sysbenchsingle"],
                "cpu-sysbench-multi": config["sysbenchmulti"],
                "net-download": config["netdownload"] * 1024 * 1024,
                "net-upload": config["netupload"] * 1024 * 1024,
                "gpu-count": config["gpucount"],
                "gpu-mem": gpumem * 1024 * 1024,
                "gpu-eth-hashrate": ethhashrate * 1000000
            }
        }
    }
    if counterparty:
        bid_template["counterparty"] = counterparty
    return bid_template


def template_task(file_, node_tag):
    with open(file_, 'r') as fp:
        t = Template(fp.read())
        data = t.render(node_tag=node_tag)
        return ruamel.yaml.round_trip_load(data, preserve_quotes=True)
