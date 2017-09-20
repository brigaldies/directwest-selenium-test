import pysolr
from utils import *


def solr_connect(url, handler):
    solr_cnx = None
    try:
        print('Connecting to Solr at {0}...'.format(url))
        # Optional arg: results_cls=dict
        solr_cnx = pysolr.Solr(url, timeout=10, search_handler=handler, results_cls=dict)
        printGreen('Successfully connected to solr at "{}" with handler "{}"!'.format(url, handler))
    except Exception as e:
        printRed('ERROR: An exception occurred when connecting to Solr host "{}"'.format(url))
    return solr_cnx


def solr_search(cnx, what, where):
    params = {
        'LocalSearchCity': where,
        'fq': 'BUS_SERVICE_AREA:("{}","NoCity")'.format(where)
    }
    return cnx.search(what, **params)
