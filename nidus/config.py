import json
import logging.config
from copy import deepcopy

base_config = {
    # All the nodes in the cluster. The total number of nodes in the cluster
    # should be an odd number > 3. The keys are mnemonic names for the node
    # which will appear in the logs. The value should be two item list of
    # [host, port].
    "cluster": {},
    # How often the leader will send hearthbeats to followers in seconds.
    "heartbeat_interval": 0.05,  # 50ms
    # Python standard logging dictionary configuration
    # https://docs.python.org/3/library/logging.config.html#configuration-dictionary-schema
    "logging": {
        "version": 1,
        "formatters": {
            "node": {"format": "%(asctime)s - [%(node_id)s] %(message)s"},
        },
        "handlers": {
            "node_console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "node",
            }
        },
        "loggers": {"node_logger": {"level": "INFO", "handlers": ["node_console"]}},
    },
}


def merge_config_with_base(user_config):
    config = deepcopy(base_config)
    for k, v in user_config.items():
        config[k] = v
    return config


def load_user_config(config_file):
    with open(config_file, "r") as f:
        user_config = json.loads(f.read())
    config = merge_config_with_base(user_config)
    config["cluster"] = {k: tuple(v) for k, v in config["cluster"].items()}
    logging.config.dictConfig(config["logging"])
    return config
