# sonm-automation-python

## Preparations

  `git clone git://github.com/avsigaev/sonm-automation-python.git`
  
**Mac OS X:**  
  `brew install python3`
  
  `pip3 install -r requirements.txt`

**Linux:**
  `apt-get install python3 python3-pip`
  
  `pip3 install -r requirements.txt`

## Configuration

- Edit config.yaml to describe hardware configuration requirements for your task.
- Create or edit specification of your task you want to run in SONM (see claymore.yaml for example).

## Run 

  `./new_monitor.py`

This will run bot. It will create orders and wait for deals.
When deal appears, it will start task and will track it.

If you want to change hardware requirements, you may change config and run:

`./recreate_orders.py`

and

`./amnesty.py` to clear blacklist.
