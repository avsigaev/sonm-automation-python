import threading

from monitor import set_sonmcli, Cli


def clear_blacklist(address):
    print("Removing " + address + " from blacklist...")
    status = SONM_CLI.exec(["blacklist", "remove", address, "--timeout=2m"])
    if status and status is not None:
        print("[OK] " + address + " successfully removed from blacklist")
    else:
        print("[ERR] Error occurred while removing " + address + "from blacklist.")
    exit(0)


def get_blacklist():
    addresses = SONM_CLI.exec(["blacklist", "list", "--timeout=2m"])
    if addresses and len(addresses) > 1:
        print("Blacklist contains addresses: ")
        for i in addresses["addresses"]:
            print(i)
        print("========")
        return addresses["addresses"]
    else:
        print("Blacklist is empty.")
        exit(0)


def main():
    global SONM_CLI
    SONM_CLI = Cli(set_sonmcli())
    blacklist = get_blacklist()
    for address in blacklist:
        threading.Thread(target=clear_blacklist, kwargs={'address': address}).start()


if __name__ == "__main__":
    main()
