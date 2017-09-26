from utils import *


def testResidential(args, db_cnx, solr_cnx):
    """
    DWResidential core tests.
    :param args: Python script's command-line arguments.
    :param db_cnx: Source SQL database connection.
    :param solr_cnx: Solr connection
    :return: Tuple (Number of tests, number of successful tests, number of skipped tests)
    """

    test_name = 'testResidential'
    start_time = time.monotonic()

    tests_count = 0
    success_count = 0
    search_times = []
    recalls = []

    q = '("Hardy T" OR ((RES_FNLN_SYNONYMS:("Hardy") AND RES_FNLN_SYNONYMS:("T")) OR (((RES_FN:("H")) AND (RES_LN:("T") OR RES_LN:("T"))) OR (RES_FN:("T") AND (RES_LN:("Hardy") OR RES_LN:("H"))))))'
    fq = 'RES_CITY:("regina")'

    params = {
        'start': 0,
        'rows': 15,
        'fq': fq,
    }

    tests_count += 1

    results = solr_cnx.search(q, **params)

    qtime = int(results['responseHeader']['QTime'])
    search_times.append(qtime)
    recall = results['response']['numFound']
    recalls.append(recall)
    print('\tRecall: {} results in {} ms'.format(recall, qtime))
    docs = results['response']['docs']

    try:
        assert docs is not None
        assert len(docs) == 2  # Expected two matches": "Hardy T" and "Hardy Tracey"

        assert docs[0]['RES_LN'] == 'Hardy'
        assert docs[0]['RES_FN'] == 'T'

        assert docs[1]['RES_LN'] == 'Hardy'
        assert docs[1]['RES_FN'] == 'Tracey'

        success_count += 1

    except Exception as e:
        print(e)
        printRed("Assert FAILED.")

    testEpilog(test_name, start_time, tests_count, success_count, search_times, recalls)

    return tests_count, success_count, 0
