from solr import *
from utils import *


def testJudgementList(args, db_cnx, solr_cnx):
    """
    Poor man's Judgement List-based tests.
    :param args: Python script's command-line arguments.
    :param db_cnx: Source SQL database connection.
    :param solr_cnx: Solr connection
    :return: Tuple (Number of tests, number of successful tests, number of skipped tests)
    """

    test_name = "testJudgementList"
    start_time = time.monotonic()

    # The poor man's judgement list is implemented as a dictionary with key=query and value=array of expected Solr document (DW) ids (One or more).

    # Some long expected results lists used by several queries
    results_seven_eleven = ['5320', '7197', '7198', '7199', '7200', '7201', '7202', '7203', '7204', '7205']

    judgementList = {

        # Business names
        'Nu Trend': {'location': 'Regina', 'expect': ['4379']},
        'Nu-Trend': {'location': 'Regina', 'expect': ['4379']},
        'Nu Trend Industry': {'location': 'Regina', 'expect': ['4379']},
        'Nu-Trend Industries': {'location': 'Regina', 'expect': ['4379']},

        'A & B Autobody': {'location': 'Regina', 'expect': ['2884']},
        'A B Autobody': {'location': 'Regina', 'expect': ['2884']},
        'AB Autobody': {'location': 'Regina', 'expect': ['2884']},
        'A & B Auto body': {'location': 'Regina', 'expect': ['2884']},
        'A B Auto body': {'location': 'Regina', 'expect': ['2884']},
        'AB Auto body': {'location': 'Regina', 'expect': ['2884']},

        "Amy's": {'location': '"Prince Albert"', 'expect': ['30186']},

        'Pizza Pizza': {'location': 'Regina', 'expect': ['9695', '74700', '120121', '120122']},

        '7 Eleven': {'location': 'Regina', 'expect': results_seven_eleven},
        '7-11': {'location': 'Regina', 'expect': results_seven_eleven},
        '7 11': {'location': 'Regina', 'expect': results_seven_eleven},
        'Seven Eleven': {'location': 'Regina', 'expect': results_seven_eleven},
        'Seven 11': {'location': 'Regina', 'expect': results_seven_eleven},

        # Categories
        'Pizza': {'location': 'Regina',
                  'expect': ['88072', '1218', '120921', '2826', '74070', '1958', '93699', '6587', '2819', '70040']},

        # The category 'Auto Electric Service' presents the situation of a category that matches a business
        # name that is in a different category.
        'Auto Electric Service': {'location': 'Regina', 'expect': ['2517', '2520', '10068', '4555', '1236']},

        # The category 'Ambulance Service' presents the situation of a category that matches a business
        # name that is in that category.
        'Ambulance Service': {'location': 'Regina', 'expect': ['10024', '89122']},

        'Courts': {'location': 'Regina',
                   'expect': ['104474', '101522', '102554', '104129', '101945', '103170', '101465', '102830', '102965',
                              '102042']},

        # Content
        'Gluten Free Pizza': {'location': 'Regina', 'expect': ['120921', '85963', '120929']},

        'Paul Mitchell shampoo': {'location': 'Regina', 'expect': ['12577', '73599', '12579', '12581', '12583']},

        'Canoes': {'location': 'Yorkton', 'expect': ['116058']},

        # Matching on both business name and business extended line
        'Cuelenaere Kendall Katzman Watson': {'location': 'Saskatoon',
                                              'expect': ['122706', '122696', '122699', '122697']},

    }

    tests_count = 0
    success_count = 0
    search_times = []
    recalls = []

    for count, query in enumerate(judgementList.keys()):
        tests_count += 1
        print('Searching for "{}" in "{}"...'.format(query.lower(), judgementList[query]['location']))

        rows_count = max(10, len(judgementList[query]['expect']))

        results = solr_search(solr_cnx, what=query.lower(), where=judgementList[query]['location'], rows=rows_count)

        qtime = int(results['responseHeader']['QTime'])
        search_times.append(qtime)
        recall = results['grouped']['BUS_LISTING_ID']['matches']
        recalls.append(recall)
        print('\tRecall: {} results in {} ms'.format(recall, qtime))
        docs = results['grouped']['BUS_LISTING_ID']['doclist']['docs']

        # Asserts
        try:
            print("\tAsserting the recall is >= expected...")
            assert len(docs) >= len(judgementList[query]['expect'])

            print("\tAsserting the expected results...")
            expected_results = judgementList[query]['expect']
            is_rank_mismatched = False
            for idx, expected_id in enumerate(expected_results):
                doc = docs[idx]
                actual_id = doc['id']
                max_field_print_len = 20
                print(
                    '\tAsserting expected result in position {:2d}: expected {:10d} ?== actual {:10d} [Sort: $LocalSearchCitySort={}, BUS_IS_TOLL_FREE={}, $dwScore={:010.5f}, BUS_PRIORITY_RANK={:4d}, BUS_CITY={:20.20}, BUS_BUSINESS_NAME={:20.20}, BUS_NAME_EL={:20.20}, BUS_HEADING={:20.20}, BUS_LISTING_ID={:10d}, id={:10d}]...'.format(
                        idx + 1,
                        int(expected_id),
                        int(actual_id),
                        doc['$LocalSearchCitySort'] if '$LocalSearchCitySort' in doc.keys() else '',
                        doc['BUS_IS_TOLL_FREE'],
                        doc['$dwScore'],
                        int(doc['BUS_PRIORITY_RANK']),
                        doc['BUS_CITY'],
                        doc['BUS_BUSINESS_NAME'][:(max_field_print_len - 3)] + '...' if len(
                            doc['BUS_BUSINESS_NAME']) > max_field_print_len else doc['BUS_BUSINESS_NAME'],
                        doc['BUS_NAME_EL'][0][:(max_field_print_len - 3)] + '...' if len(
                            doc['BUS_NAME_EL'][0]) > max_field_print_len else doc['BUS_NAME_EL'][0],
                        doc['BUS_HEADING'][:(max_field_print_len - 3)] + '...' if len(
                            doc['BUS_HEADING']) > max_field_print_len else doc['BUS_HEADING'],
                        int(doc['BUS_LISTING_ID']),
                        int(doc['id'])
                    ))
                try:
                    assert expected_id == actual_id
                except Exception as e:
                    is_rank_mismatched = True
                    printRed('\tRanking mismatch!')

            if not is_rank_mismatched:
                printGreen('\tAll asserts PASSED!')
                success_count += 1
            else:
                printRed("\tOne or more Asserts FAILED!")
        except Exception as e:
            print(e)
            printRed("Assert FAILED.")

    testEpilog(test_name, start_time, tests_count, success_count, search_times, recalls)

    return tests_count, success_count, 0
