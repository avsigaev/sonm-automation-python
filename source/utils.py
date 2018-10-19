import base64
import errno
import os
import platform

from pathlib2 import Path
from ruamel.yaml import YAML


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