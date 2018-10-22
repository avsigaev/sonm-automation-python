import base64
import errno
import os
import platform
from concurrent.futures import Future
from threading import Thread

from pathlib2 import Path
from ruamel.yaml import YAML


class Nodes(object):
    nodes_ = []

    @staticmethod
    def get_nodes():
        Nodes.nodes_.sort(key=lambda x: int(x.node_num), reverse=False)
        return Nodes.nodes_


def parse_tag(order_):
    return base64.b64decode(order_).decode().strip("\0")


def create_dir(dir_):
    if not os.path.exists(dir_):
        try:
            os.makedirs(dir_)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def load_cfg(path='config.yaml'):
    if os.path.exists(path):
        path = Path(path)
        yaml_ = YAML(typ='safe')
        return yaml_.load(path)


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
