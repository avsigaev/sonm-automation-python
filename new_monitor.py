#!/usr/bin/env python3
import logging
import os
import re
import time
from logging.config import dictConfig
from threading import Thread

from apscheduler.schedulers.background import BackgroundScheduler
from tabulate import tabulate

from source.cli import Cli
from source.node import Node, State
from source.utils import parse_tag, create_dir, load_cfg, set_sonmcli

from http.server import BaseHTTPRequestHandler, HTTPServer


class HTTPServerRequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        tabul_nodes = [[n.node_num, n.bid_id, n.deal_id, n.task_id, n.task_uptime, n.status.name] for n in nodes_]

        html = """<html><table border="1"><tr>
        <th>Node</th><th>Order id</th><th>Deal id</th><th>Task id</th><th>Task uptime</th><th>Node status</th>
        </tr>"""
        for row in tabul_nodes:
            html += "<tr>"
            for cell in row:
                html += "<td>{}</td>".format(cell)
            html += "</tr>"
        html += "</table></html>"
        self.wfile.write(bytes(html, "utf8"))
        return

    def log_message(self, format, *args):
        http_logger.info("%s - - [%s] %s\n" %
                         (self.address_string(),
                          self.log_date_time_string(),
                          format % args))


def run_http_server(config):
    if "http_server" in config and "run" in config["http_server"]:
        if config["http_server"]["run"]:
            logger.info('starting server...')
            server_address = ('0.0.0.0', config["http_server"]["port"])
            httpd = HTTPServer(server_address, HTTPServerRequestHandler)
            logger.info('running server...')
            Thread(target=httpd.serve_forever).start()


def setup_logging(default_path='logging.yaml', default_level=logging.INFO):
    """Setup logging configuration

    """
    if os.path.exists(default_path):
        config = load_cfg(default_path)
        dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def validate_eth_addr(eth_addr):
    pattern = re.compile("^0x[a-fA-F0-9]{40}$")
    if not pattern.match(eth_addr):
        logger.info("Incorrect eth address or not specified")
        return None
    else:
        logger.info("Eth address was parsed successfully: " + eth_addr)
        return eth_addr


def init():
    create_dir("out/orders")
    create_dir("out/tasks")

    config = load_cfg()
    config_keys = ["numberofnodes", "tag", "ets", "template_file", "identity", "ramsize",
                   "storagesize", "cpucores", "sysbenchsingle", "sysbenchmulti", "netdownload", "netupload", "price",
                   "overlay", "incoming", "gpucount", "gpumem", "ethhashrate"]
    missed_keys = [key for key in config_keys if key not in config]
    if len(missed_keys) > 0:
        raise Exception("Missed keys: '" + "', '".join(missed_keys) + "'")
    logger.info("Try to parse counterparty eth address:")
    counter_party = None
    if "counterparty" in config:
        counter_party = validate_eth_addr(config["counterparty"])
    return Cli(set_sonmcli()), config, counter_party


def get_nodes(cli_, nodes_num_, counterparty):
    nodes_ = []
    for n in range(nodes_num_):
        nodes_.append(Node.create_empty(cli_, n + 1, counterparty))
    return nodes_


def init_nodes_state(cli_, nodes_num_, counter_party):
    nodes_ = []
    # get deals
    deals_ = cli_.deal_list(nodes_num_)
    if deals_ and deals_['deals']:
        for d in [d_["deal"] for d_ in deals_['deals']]:
            status = State.DEAL_OPENED
            deal_status = cli_.deal_status(d["id"])
            ntag = parse_tag(deal_status["bid"]["tag"])
            node_num = ntag.split("_")[len(ntag.split("_")) - 1]
            task_id = ""
            if "resources" not in deal_status:
                logger.info(
                    "Seems like worker is offline: no respond for the resources and tasks request. Closing deal")
                status = State.TASK_FAILED
            if "running" in deal_status and len(list(deal_status["running"].keys())) > 0:
                task_id = list(deal_status["running"].keys())[0]
                status = State.TASK_RUNNING
            bid_id_ = deal_status["bid"]["id"]
            node_ = Node(status, cli_, node_num, d["id"], task_id, bid_id_, counter_party)
            logger.info("Found deal, id " + d["id"] + " (Node " + node_num + ")")
            nodes_.append(node_)

    # get orders
    orders_ = cli_.order_list(nodes_num_)
    if orders_ and orders_["orders"] is not None:
        for order_ in list(orders_["orders"]):
            status = State.AWAITING_DEAL
            ntag = parse_tag(order_["tag"])
            node_num = ntag.split("_")[len(ntag.split("_")) - 1]
            node_ = Node(status, cli_, node_num, "", "", order_["id"], counter_party)
            logger.info("Found order, id " + order_["id"] + " (Node " + node_num + ")")
            nodes_.append(node_)
    if len(nodes_) == 0:
        nodes_ = get_nodes(cli_, nodes_num_, counter_party)
    elif len(nodes_) < nodes_num_:
        live_nodes_nums = [n.node_num for n in nodes_]
        for n_num in [n for n in range(1, nodes_num_ + 1) if str(n) not in live_nodes_nums]:
            nodes_.append(Node.create_empty(cli_, n_num, counter_party))
    return nodes_


def print_state(nodes_):
    nodes_.sort(key=lambda x: int(x.node_num), reverse=False)
    tabul_nodes = [[n.node_num, n.bid_id, n.deal_id, n.task_id, n.task_uptime, n.status.name] for n in nodes_]
    logger.info("Nodes:\n" +
                tabulate(tabul_nodes,
                         ["Node", "Order id", "Deal id", "Task id", "Task uptime", "Node status"],
                         tablefmt="grid"))


def watch():
    futures = []
    for node in nodes_:
        futures.append(node.watch_node())
        time.sleep(1)
    for future in futures:
        future.result()


def main():
    global scheduler, nodes_
    cli_, config, counter_party = init()
    nodes_num_ = int(config["numberofnodes"])
    nodes_ = init_nodes_state(cli_, nodes_num_, counter_party)
    scheduler = BackgroundScheduler()
    print('Press Ctrl+{0} to interrupt script'.format('Break' if os.name == 'nt' else 'C'))
    try:
        scheduler.start()
        scheduler.add_job(print_state, 'interval', kwargs={"nodes_": nodes_}, seconds=60, id='print_state')
        run_http_server(config)
        watch()
        scheduler.shutdown()
        print_state(nodes_)
        logger.info("Work completed")
    except (KeyboardInterrupt, SystemExit):
        pass


setup_logging()
logging.getLogger('apscheduler').setLevel(logging.FATAL)
logging.getLogger('HTTPServer').setLevel(logging.FATAL)
logger = logging.getLogger('monitor')
http_logger = logging.getLogger('monitor_http')
if __name__ == "__main__":
    main()
