import logging
from genericpath import isfile
from os import listdir
from os.path import join

from sonm_pynode.main import Node

from source.sonmapi import SonmApi
from source.utils import parse_tag, Nodes, create_dir, set_sonmcli, Config
from source.worknode import WorkNode, State

logger = logging.getLogger("monitor")


def get_missed_nodes(sonm_api, nodes_):
    live_nodes_tags = [n.node_tag for n in nodes_]
    for node_tag, node_config in Config.node_configs.items():
        if node_tag not in live_nodes_tags:
            nodes_.append(WorkNode.create_empty(sonm_api, node_tag))
    return nodes_


def init_nodes_state(sonm_api):
    nodes_ = []
    nodes_num_ = len(Config.node_configs)
    # get deals
    deals_ = sonm_api.deal_list(nodes_num_)
    if deals_ and deals_['deals']:
        for d in [d_["deal"] for d_ in deals_['deals']]:
            status = State.DEAL_OPENED
            deal_status = sonm_api.deal_status(d["id"])
            ntag = parse_tag(deal_status["bid"]["tag"])
            task_id = ""
            if "resources" not in deal_status:
                logger.info(
                    "Seems like worker is offline: no respond for the resources and tasks request. Closing deal")
                status = State.TASK_FAILED
            if "running" in deal_status and len(list(deal_status["running"].keys())) > 0:
                task_id = list(deal_status["running"].keys())[0]
                status = State.TASK_RUNNING
            bid_id_ = deal_status["bid"]["id"]
            price = deal_status["bid"]["price"]
            node_ = WorkNode(status, sonm_api, ntag, d["id"], task_id, bid_id_, price)
            logger.info("Found deal, id {} (Node {})".format(d["id"], ntag))
            nodes_.append(node_)

    # get orders
    orders_ = sonm_api.order_list(nodes_num_)
    if orders_ and orders_["orders"] is not None:
        for order_ in list(orders_["orders"]):
            status = State.AWAITING_DEAL
            ntag = parse_tag(order_["tag"])
            price = order_["price"]
            node_ = WorkNode(status, sonm_api, ntag, "", "", order_["id"], price)
            logger.info("Found order, id {} (Node {})".format(order_["id"], ntag))
            nodes_.append(node_)
    get_missed_nodes(sonm_api, nodes_)
    Nodes.nodes_ = nodes_


def init():
    create_dir("out/orders")
    create_dir("out/tasks")

    Config.load_config()
    key_file_path = Config.base_config["ethereum"]["key_path"]
    onlyfiles = [f for f in listdir(key_file_path) if isfile(join(key_file_path, f))]
    key_password = Config.base_config["ethereum"]["password"]
    node_addr = Config.base_config["node_address"]
    return SonmApi(set_sonmcli(), Node(join(key_file_path, onlyfiles[0]), key_password, node_addr))
