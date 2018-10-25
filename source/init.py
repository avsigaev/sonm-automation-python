import logging
from genericpath import isfile
from os import listdir
from os.path import join

from sonm_pynode.main import Node

from source.sonmapi import SonmApi
from source.utils import Nodes, create_dir, set_sonmcli, Config
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
    if deals_:
        for deal in deals_:
            status = State.DEAL_OPENED
            deal_status = sonm_api.deal_status(deal["id"])
            order_ = sonm_api.order_status(deal_status["bid_id"])
            for node_tag, node_config in Config.node_configs.items():
                if node_tag == order_["tag"]:
                    task_id = ""
                    if deal_status["worker_offline"]:
                        logger.info(
                            "Seems like worker is offline: no respond for the resources and tasks request."
                            " Deal will be closed")
                        status = State.TASK_FAILED
                    if deal_status["running"]:
                        task_id = deal_status["running"][0]
                        status = State.TASK_RUNNING
                    bid_id_ = deal_status["bid_id"]
                    price = deal_status["price"]
                    node_ = WorkNode(status, sonm_api, order_["tag"], deal["id"], task_id, bid_id_, price)
                    logger.info("Found deal, id {} (Node {})".format(deal["id"], order_["tag"]))
                    nodes_.append(node_)

    # get orders
    orders_ = sonm_api.order_list(nodes_num_)
    if orders_ and orders_["orders"]:
        for order_ in list(orders_["orders"]):
            status = State.AWAITING_DEAL
            for node_tag, node_config in Config.node_configs.items():
                if node_tag == order_["tag"]:
                    price = order_["price"]
                    node_ = WorkNode(status, sonm_api, order_["tag"], "", "", order_["id"], price)
                    logger.info("Found order, id {} (Node {})".format(order_["id"], order_["tag"]))
                    nodes_.append(node_)
    get_missed_nodes(sonm_api, nodes_)
    Nodes.nodes_ = nodes_


def init():
    create_dir("out/orders")
    create_dir("out/tasks")

    Config.load_config()
    key_file_path = Config.base_config["ethereum"]["key_path"]
    keys = [f for f in listdir(key_file_path) if isfile(join(key_file_path, f))]
    if len(keys) == 0:
        raise Exception("Key storage doesn't contain any files")
    key_password = Config.base_config["ethereum"]["password"]
    node_addr = Config.base_config["node_address"]
    return SonmApi(join(key_file_path, keys[0]), key_password, node_addr)
