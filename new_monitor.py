#!/usr/bin/env python3.7
import concurrent
import logging
import os
import time
from logging.config import dictConfig
from os.path import join

from apscheduler.schedulers.background import BackgroundScheduler

from source.http_server import run_http_server, SonmHttpServer
from source.utils import Nodes, Config, print_state, create_dir
from source.init import init_nodes_state, init


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
        time.sleep(1)
    for future in futures:
        logger.info(future.result())


def main():
    sonm_api = init()
    init_nodes_state(sonm_api)
    scheduler = BackgroundScheduler()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=100)
    futures = []
    try:
        scheduler.start()
        scheduler.add_job(print_state, 'interval', seconds=60, id='print_state')
        scheduler.add_job(Config.check_config, 'interval', seconds=60, id='load_config')
        executor.submit(run_http_server)
        watch(executor, futures)
        print_state()
        logger.info("Work completed")
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        for n in Nodes.get_nodes():
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
