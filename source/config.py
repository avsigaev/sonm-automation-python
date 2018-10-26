import json
import os
from os.path import join

from pathlib2 import Path
from ruamel.yaml import YAML

from source.utils import logger, validate_eth_addr, Nodes


class Config(object):
    base_config = {}
    node_configs = {}
    config_folder = "conf/"

    @staticmethod
    def check_config():
        logger.debug("Checking config...")
        Config.load_config()
        if len(Config.node_configs) != len(Nodes.get_nodes()):
            logger.info("Configuration changed...")

    @staticmethod
    def get_node_config(node_tag):
        return Config.node_configs.get(node_tag)

    @staticmethod
    def load_config():
        Config.load_base_config()
        Config.node_configs = Config.load_task_configs()

    @staticmethod
    def load_task_configs():
        # TODO check tasks tag (must be different)
        temp_node_configs = {}
        logger.debug("Try to parse configs:")
        for task in Config.base_config["tasks"]:
            task_config = Config.load_cfg(task)
            for num in range(1, task_config["numberofnodes"] + 1):
                task_config["counterparty"] = validate_eth_addr(task_config["counterparty"])
                ntag = "{}_{}".format(task_config["tag"], num)
                temp_node_configs[ntag] = task_config
                logger.debug("Config for node {} was created successfully".format(ntag))
                logger.debug("Config: {}".format(json.dumps(task_config, sort_keys=True, indent=4)))
        return temp_node_configs

    @staticmethod
    def load_base_config():
        logger.debug("Loading base config")
        temp_config = Config.load_cfg()
        config_keys = ["node_address", "ethereum", "tasks"]
        missed_keys = [key for key in config_keys if key not in temp_config]
        if len(missed_keys) > 0:
            raise Exception("Missed keys: '{}'".format("', '".join(missed_keys)))
        Config.base_config = temp_config
        logger.debug("Base config loaded")

    @staticmethod
    def reload_node_config(node_tag):
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
