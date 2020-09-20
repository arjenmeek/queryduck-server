import os
import sys

import yaml

from wsgiref.simple_server import make_server

if "QDCONFIG" in os.environ:
    conffile = os.environ["QDCONFIG"]
else:
    conffile = os.path.expanduser("~/.config/queryduck/config.yml")

with open(conffile, "r") as f:
    config = yaml.load(f.read(), Loader=yaml.SafeLoader)

from qdserver import main

app = main(
    {
        "sqlalchemy.url": config["db"]["url"],
        "sqlalchemy.echo": config["db"]["echo"],
    }
)

server = make_server(config["http"]["host"], config["http"]["port"], app)
print("Serving on {}:{} ...".format(config["http"]["host"], config["http"]["port"]))
server.serve_forever()
