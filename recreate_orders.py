#!/usr/bin/env python3

import base64
import threading

from ruamel import yaml

from source.cli import Cli
from new_monitor import validate_eth_addr, load_cfg, set_sonmcli
from source.yaml_gen import template_bid


def dump_file(data, filename):
    with open(filename, 'w+') as file:
        yaml.dump(data, file, Dumper=yaml.RoundTripDumper)


def recreate_order(order):
    node_num, ntag = get_tag_num(order)
    print("Cancelling order " + order["id"])
    SONM_CLI.exec(["order", "cancel", order["id"]])
    bidfile_ = "out/orders/" + ntag + ".yaml"
    order = SONM_CLI.exec(["order", "create", bidfile_])
    print("Order for Node " + node_num + " is " + order["id"])


def get_orders_list(number_of_nodes):
    orders = SONM_CLI.exec(["order", "list", "--timeout=2m", "--limit", str(number_of_nodes)])
    if orders and orders["orders"] is not None:
        return orders["orders"]
    else:
        print("No active orders found.")
        exit(0)


def create_new_yaml_files(orders_list, config):
    counterparty = validate_eth_addr(config["counterparty"])
    for order in orders_list:
        node_num, ntag = get_tag_num(order)
        bid_ = template_bid(config, ntag, counterparty)
        bid_file = "out/orders/" + ntag + ".yaml"
        print("Creating order file Node " + str(node_num))
        dump_file(bid_, bid_file)


def get_tag_num(order):
    ntag = base64.b64decode(order["tag"]).decode().strip("\0")
    node_num = ntag.split("_")[len(ntag.split("_")) - 1]
    return node_num, ntag


def main():
    global SONM_CLI
    config = load_cfg()
    SONM_CLI = Cli(set_sonmcli())
    orders_list = get_orders_list(config["numberofnodes"])
    create_new_yaml_files(orders_list, config)
    for order in orders_list:
        threading.Thread(target=recreate_order, kwargs={'order': order}).start()


if __name__ == "__main__":
    main()
