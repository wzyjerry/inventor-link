import yaml

DEFAULT_MONGO_DB = "prod"
configuration = None
storage_client = None


def load_config(path="config.yml"):
    global configuration
    with open(path) as f_in:
        configuration = yaml.load(f_in)
        return configuration


def get_config():
    return configuration

