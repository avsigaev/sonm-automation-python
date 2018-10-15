from jinja2 import Template
from ruamel.yaml import ruamel


def template_bid(config, tag, counterparty=None):
    gpumem = config["gpumem"]
    ethhashrate = config["ethhashrate"]
    if config["gpucount"] == 0:
        gpumem = 0
        ethhashrate = 0
    bid_template = {
        "duration": config["duration"],
        "price": config["price"] + "USD/h",
        "identity": config["identity"],
        "tag": tag,
        "resources": {
            "network": {
                "overlay": config["overlay"],
                "outbound": True,
                "incoming": config["incoming"]
            },
            "benchmarks": {
                "ram-size": config["ramsize"] * 1024 * 1024,
                "storage-size": config["storagesize"] * 1024 * 1024 * 1024,
                "cpu-cores": config["cpucores"],
                "cpu-sysbench-single": config["sysbenchsingle"],
                "cpu-sysbench-multi": config["sysbenchmulti"],
                "net-download": config["netdownload"] * 1024 * 1024,
                "net-upload": config["netupload"] * 1024 * 1024,
                "gpu-count": config["gpucount"],
                "gpu-mem": gpumem * 1024 * 1024,
                "gpu-eth-hashrate": ethhashrate * 1000000
            }
        }
    }
    if counterparty:
        bid_template["counterparty"] = counterparty
    return bid_template


def template_task(file_, node_tag):
    with open(file_, 'r') as fp:
        t = Template(fp.read())
        data = t.render(node_tag=node_tag)
        return ruamel.yaml.round_trip_load(data, preserve_quotes=True)