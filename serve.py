import yaml
import sys

from wsgiref.simple_server import make_server

sys.path.append('../common')

from crunchyserver import main


with open('config.yml', 'r') as f:
    config = yaml.load(f.read(), Loader=yaml.SafeLoader)

app = main({
    'sqlalchemy.url': config['db']['url'],
    'sqlalchemy.echo': config['db']['echo'],
})

server = make_server(config['http']['host'], config['http']['port'], app)
print("Serving on {}:{} ...".format(config['http']['host'], config['http']['port']))
server.serve_forever()
