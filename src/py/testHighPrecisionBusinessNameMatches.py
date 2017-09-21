import random
import re
import time
from datetime import timedelta

import numpy as np

from dbUtils import *
from solr import *


def testHighPrecisionBusinessNameMatches(args, db_cnx, solr_cnx, tests_count):
    """
    Test fixture for high-precision name matches.
    :param args: Python script's command-line arguments.
    :param db_cnx: Source SQL database connection.
    :param solr_cnx: Solr connection.
    :param tests_count: Maximum number of randomly (Seeded)-selected categories to test.
    :return: Number of successful tests.
    """
    test_name = "testHighPrecisionBusinessNameMatches"
    start_time = time.monotonic()
    count = 0
    success_count = 0
    count_precise_name_match = 0
    count_partial_name_match = 0
    count_el_match_skipped = 0
    search_times = []
    recalls = []

    try:
        print("{}: Executing {} tests.".format(test_name, tests_count))

        # Retrieve business names
        rows = dwDbGetBusinesses(db_cnx, args.loc, args.filter)

        if rows is None or len(rows) == 0:
            raise Exception("No businesses returned from the source database!")

        # Seed the random number generator for test repeatability.
        random.seed(10)

        for row in rows:
            count += 1
            random_row = random.choice(rows)
            print("\n[{}]: {}, {}".format(count, random_row.BUS_BUSINESS_NAME, random_row.BUS_CITY))

            # Prepare the search terms:
            search = random_row.BUS_BUSINESS_NAME
            regexs = [
                '[-_&,;]',
                '\s+[aA][nN][dD]\s+',  # Replace ' and '
                '\s+[dD][rR]\s+',  # Replace ' Dr '
                '\s+[iI][nN][cC]\s*$',  # Replace '... Ltd'
                '\s+[lL][tT][dD]\s*$',  # Replace '... Inc'
                '\'s',  # Replace 's
                '\s+',  # Remove extra space
            ]
            for regex in regexs:
                search = re.sub(regex, ' ', search)
                # print('\tregex="{}", search="{}"'.format(regex, search))
            # Remove leading and training white spaces
            search = search.strip()
            printBlue('Searching for "{}" in city "{}"...'.format(search, random_row.BUS_CITY))

            # Search!
            results = solr_search(solr_cnx, search, random_row.BUS_CITY)
            qtime = int(results['responseHeader']['QTime'])
            search_times.append(qtime)
            recall = results['grouped']['BUS_LISTING_ID']['matches']
            recalls.append(recall)
            print('\tRecall: {} results in {} ms'.format(recall, qtime))

            docs = results['grouped']['BUS_LISTING_ID']['doclist']['docs']
            top_doc = docs[0]
            print(
                '\tPrecision: Top result=id "{}", name="{}", business rank={}, $isPreciseNameMatch={}, $isPartialNameMatch={}'.format(
                    top_doc['id'],
                    top_doc['BUS_BUSINESS_NAME'],
                    top_doc['BUS_PRIORITY_RANK'],
                    top_doc['$isPreciseNameMatch'],
                    top_doc['$isPartialNameMatch']))

            # print(top_doc)

            if top_doc['$isPreciseNameMatch']:
                count_precise_name_match += 1
            elif top_doc['$isPartialNameMatch']:
                count_partial_name_match += 1

            # Asserts!
            try:
                print("\tAsserting business name...")
                # The business name comparison uses lower case in order to accommodate cases like 'SAIL - Aids To Independent Living',
                # which is also spelled 'SAIL - Aids to Independent Living'.
                assert top_doc['BUS_BUSINESS_NAME'].lower() == random_row.BUS_BUSINESS_NAME.lower()
                print("\tAsserting business name match types...")
                assert top_doc['$isPreciseNameMatch'] or top_doc['$isPartialNameMatch']

                # Retrieve the source database records for the given name, sorted by descending BUS_PRIORITY_RANK and BUS_LISTING_ID
                business_rows = dwDbGetBusinesses(db_cnx, args.loc, top_doc['BUS_BUSINESS_NAME'])
                print("\tAsserting business' source database records...")
                assert len(business_rows) > 0
                print("\tTop database source record: id={}, business rank={}".format(business_rows[0].ID_STR,
                                                                                     business_rows[
                                                                                         0].BUS_PRIORITY_RANK))
                print("\tAsserting ranking...")
                # Solr may return a different top result based on matching on BUS_EL_TEXT. The test code here does not account for that, hence the test is skipped in that situation.
                if top_doc['BUS_NAME_EL'][0] == '':
                    assert business_rows[0].ID_STR == top_doc['id']
                else:
                    printRed('Top Solr doc has a non-empty BUS_NAME_EL field: "{}". Ranking assert skipped'.format(
                        top_doc['BUS_NAME_EL'][0]))
                    count_el_match_skipped += 1

                printGreen("\tAll asserts PASSED.")
                success_count += 1
            except Exception as e:
                print(e)
                printRed("Assert FAILED.")

            # Stop after max count tests
            if count == tests_count:
                break

    except Exception as e:
        printRed(e)
    finally:
        print("\n")
        print("{} tests executed: {} precise and {} partial name matches.".format(count,
                                                                                  count_precise_name_match,
                                                                                  count_partial_name_match))
        if count > 0:
            if success_count == tests_count:
                printSuccess("All tests PASSED!")
            else:
                if success_count > 0:
                    printSuccess("{} tests PASSED.".format(success_count))
                printRed("{} tests FAILED".format(count - success_count))
            if count_el_match_skipped > 0:
                printRed("WARNING: {} tests skipped.".format(count_el_match_skipped))

            # Display some stats
            print("\n")
            print("QTime: median={} ms, average={} ms, min={} ms, max={} ms".format(
                np.round(np.median(search_times)),
                np.round(np.average(search_times)),
                np.round(np.min(search_times)),
                np.round(np.max(search_times))))
            print("Recall: median={}, average={}, min={}, max={}".format(
                np.round(np.median(recalls)),
                np.round(np.average(recalls)),
                np.round(np.min(recalls)),
                np.round(np.max(recalls))))

            end_time = time.monotonic()
            print("{}: execution time: {}".format(test_name, timedelta(seconds=end_time - start_time)))

    return tests_count, success_count, count_el_match_skipped