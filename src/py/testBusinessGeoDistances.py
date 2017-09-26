from utils import *


def testBusinessGeoDistances(args, db_cnx, solr_cnx):
    """
    SKLocations core tests.
    :param args: Python script's command-line arguments.
    :param db_cnx: Source SQL database connection.
    :param solr_cnx: Solr connection
    :return: Tuple (Number of tests, number of successful tests, number of skipped tests)
    """

    test_name = 'testBusinessGeoDistances'
    start_time = time.monotonic()

    tests_count = 0
    success_count = 0
    search_times = []
    recalls = []

    # http://apiosc.411.directwest.com:8983/solr/DWBusiness/select?q=CIBC&start=0&rows=20&fl=*,score,_dist_:geodist(BUS_GEOPOINT,50.3994077,-104.6612898)&fq=%7b!geofilt+pt=50.3994077,-104.6612898+sfield=BUS_GEOPOINT+d=10%7d&qt=/DWOmniSearch&sort=geodist(BUS_GEOPOINT,50.3994077,-104.6612898)+asc,score+desc&version=2.2

    q = 'Pizza'
    fl = '*,score,_dist_:geodist()'
    pt = '50.3994077,-104.6612898'
    sfield = 'BUS_GEOPOINT'
    fq = '{!geofilt d=10}'
    sort = '$dwScore desc, geodist() asc'

    params = {
        'start': 0,
        'rows': 15,
        'fl': fl,
        'pt': pt,
        'sfield': sfield,
        'fq': fq,
        'sort': sort,
    }

    tests_count += 1

    results = solr_cnx.search(q, **params)

    qtime = int(results['responseHeader']['QTime'])
    search_times.append(qtime)
    recall = results['grouped']['BUS_LISTING_ID']['matches']
    recalls.append(recall)
    print('\tRecall: {} results in {} ms'.format(recall, qtime))

    docs = results['grouped']['BUS_LISTING_ID']['doclist']['docs']

    try:
        assert docs is not None
        assert len(docs) > 0
        success_count += 1

    except Exception as e:
        print(e)
        printRed("Assert FAILED.")

    testEpilog(test_name, start_time, tests_count, success_count, search_times, recalls)

    return tests_count, success_count, 0
