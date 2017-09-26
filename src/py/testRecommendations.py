from utils import *


def testRecommendations(args, db_cnx, solr_cnx):
    """
    Recommendations core tests.
    :param args: Python script's command-line arguments.
    :param db_cnx: Source SQL database connection.
    :param solr_cnx: Solr connection
    :return: Tuple (Number of tests, number of successful tests, number of skipped tests)
    """

    tests_count = 0
    success_count = 0
    search_times = []
    recalls = []

    # http://apiosc.411.directwest.com:8983/solr/Recommendations/select?q=(REC_CITY:(Regina))&rows=3&fq=REC_HEADING:(%22%22Plumbing+Contractors%22%22)&qt=/Recommendations&sort=REC_RECOMMENDATION_COUNT+desc,REC_BUSINESS_NAME+asc&group=true&group.field=REC_LISTING_ID&group.main=true&group.format=grouped&version=2.2
    params = {
        'rows': 10,
        'fq': 'REC_HEADING:("Plumbing Contractors")',
        'sort': 'REC_RECOMMENDATION_COUNT desc, REC_BUSINESS_NAME asc',
        'group': 'true',
        'group.field': 'REC_LISTING_ID',
        'group.main': 'true',
        'group.format': 'grouped'

    }

    tests_count += 1

    results = solr_cnx.search('(REC_CITY:(Regina))', **params)

    qtime = int(results['responseHeader']['QTime'])
    search_times.append(qtime)
    recall = results['response']['numFound']
    recalls.append(recall)
    print('\tRecall: {} results in {} ms'.format(recall, qtime))
    docs = results['response']['docs']

    try:
        assert docs is not None
        assert len(docs) > 0
        success_count += 1

    except Exception as e:
        print(e)
        printRed("Assert FAILED.")

    return tests_count, success_count, 0
