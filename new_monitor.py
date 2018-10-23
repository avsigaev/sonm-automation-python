#!/usr/bin/env python3
import logging
import os
import time
from http.server import HTTPServer
from logging.config import dictConfig

from apscheduler.schedulers.background import BackgroundScheduler
from source.http_server import HTTPServerRequestHandler
from source.utils import Nodes, Config, print_state
from source.init import init_nodes_state, init


def setup_logging(default_path='logging.yaml', default_level=logging.INFO):
    """Setup logging configuration

    """
    if os.path.exists(default_path):
        config = Config.load_cfg(default_path)
        dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def run_http_server(config):
    if "http_server" in config and "run" in config["http_server"]:
        if config["http_server"]["run"]:
            logger.info('starting server...')
            server_address = ('0.0.0.0', config["http_server"]["port"])
            httpd = HTTPServer(server_address, HTTPServerRequestHandler)
            logger.info('running server...')
            httpd.serve_forever()


def watch():
    futures = []
    for node in Nodes.get_nodes():
        futures.append(node.watch_node())
        time.sleep(1)
    for future in futures:
        future.result()


def main():
    sonm_api = init()
    init_nodes_state(sonm_api)
    scheduler = BackgroundScheduler()
    try:
        scheduler.start()
        scheduler.add_job(print_state, 'interval', seconds=60, id='print_state')
        scheduler.add_job(run_http_server, kwargs={"config": Config.base_config}, id='http_server')
        watch()
        scheduler.shutdown()
        print_state()
        logger.info("Work completed")
    except (KeyboardInterrupt, SystemExit):
        pass


setup_logging()
logging.getLogger('apscheduler').setLevel(logging.FATAL)
logging.getLogger('HTTPServer').setLevel(logging.FATAL)
logger = logging.getLogger('monitor')

if __name__ == "__main__":
    print('Press Ctrl+{0} to interrupt script'.format('Break' if os.name == 'nt' else 'C'))
    main()
