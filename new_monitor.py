#!/usr/bin/env python3.7
import concurrent
import logging
import os
import time
from logging.config import dictConfig
from os.path import join

from apscheduler.schedulers.background import BackgroundScheduler

from source.http_server import run_http_server, SonmHttpServer
from source.utils import Nodes, print_state, create_dir
from source.config import Config
from source.init import init_nodes_state, init, reload_config


def setup_logging(default_config='logging.yaml', default_level=logging.INFO):
    """Setup logging configuration

    """
    create_dir("out/logs")
    if os.path.exists(join(Config.config_folder, default_config)):
        config = Config.load_cfg(default_config)
        dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def watch(executor, futures):
    for node in Nodes.get_nodes():
        futures.append(executor.submit(node.watch_node))
        time.sleep(1)
    while True in [future.running() for future in futures]:
        # Clear finished futures
        for future in futures:
            if future.done():
                futures.remove(future)
        # Destroy nodes, if they aren't exist in reloaded config
        for n in Nodes.get_nodes():
            if n.node_tag not in Config.node_configs.keys():
                n.destroy()
                Nodes.get_nodes().remove(n)
        # Add new nodes to executor:
        for n in Nodes.get_nodes():
            if not n.is_running():
                logger.info("Adding new Node {} to executor".format(n.node_tag))
                futures.append(executor.submit(n.watch_node))
        time.sleep(1)


def main():
    sonm_api = init()
    init_nodes_state(sonm_api)
    scheduler = BackgroundScheduler()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=100)
    futures = []
    try:
        scheduler.start()
        scheduler.add_job(print_state, 'interval', seconds=60, id='print_state')
        scheduler.add_job(reload_config, 'interval', kwargs={"sonm_api": sonm_api}, seconds=60, id='reload_config')
        executor.submit(run_http_server)
        watch(executor, futures)
        print_state()
        logger.info("Work completed")
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        for n in Nodes.get_nodes():
            logger.info("Keyboard interrupt, script exiting. Sonm node will continue work")
            n.stop_work()
        SonmHttpServer.KEEP_RUNNING = False
        executor.shutdown(wait=False)
        scheduler.shutdown(wait=False)


setup_logging()
logging.getLogger('apscheduler').setLevel(logging.FATAL)
logging.getLogger('HTTPServer').setLevel(logging.FATAL)
logger = logging.getLogger('monitor')

if __name__ == "__main__":
    print('Press Ctrl+{0} to interrupt script'.format('Break' if os.name == 'nt' else 'C'))
    main()
