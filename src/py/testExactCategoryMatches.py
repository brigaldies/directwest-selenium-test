import random
import re
import time
from datetime import timedelta

import numpy as np

from dbUtils import *
from solr import *


def testExactCategoryMatches(args, db_cnx, solr_cnx, tests_count):
    """
    Test searches of categories in a given location.
    :param args: Python script's command-line arguments.
    :param db_cnx: Source SQL database connection.
    :param solr_cnx: Solr connection.
    :param tests_count: Maximum number of randomly (Seeded)-selected categories to test.
    :return: Tuple (Number of tests, number of successful tests, number of skipped tests)
    """
    test_name = "testExactCategoryMatches"
    start_time = time.monotonic()
    count = 0
    count_success = 0
    search_times = []
    recalls = []
    category_tokens_stats = {}
    tested_categories = {}
    try:
        print("{}: Executing {} tests...".format(test_name, tests_count))

        # Retrieve categories
        rows = dwDbGetCategories(db_cnx, args.loc)

        # Seed the random number generator for test repeatability.
        random.seed(10)

        for row in rows:
            count += 1
            random_row = random.choice(rows)
            category = random_row.BUS_HEADING
            print("\n[{}]: {}".format(count, category))

            # Prepare the search terms:
            search = category
            regexs = [
                '[-_&,;]',
                '\s+[aA][nN][dD]\s+',  # Replace ' and '
                '\'s',  # Replace 's
                '\s+',  # Remove extra space
            ]
            for regex in regexs:
                search = re.sub(regex, ' ', search)
                # print('\tregex="{}", search="{}"'.format(regex, search))

            # Remove leading and training white spaces
            search = search.strip()
            printBlue('Searching for category "{}" in city "{}"...'.format(search, random_row.BUS_CITY))

            category_tokens = search.split()
            tokens_count = len(category_tokens)
            if not tokens_count in category_tokens_stats:
                category_tokens_stats[tokens_count] = 0
            category_tokens_stats[tokens_count] += 1

            # Retrieve the count of businesses in the desired location and category
            business_rows = dwDbGetBusinessesCountByCategory(db_cnx, args.loc, random_row.BUS_HEADING)
            print("\tAsserting business' source database records...")
            assert len(business_rows) == 1
            category_business_count = business_rows[0].BUS_CAT_COUNT
            print('\tSource database: {} businesses in category "{}" in "{}"'.format(
                category_business_count,
                category,
                args.loc))
            assert category_business_count > 0

            # Search!
            results = solr_search(solr_cnx, search, random_row.BUS_CITY, rows=category_business_count)
            qtime = int(results['responseHeader']['QTime'])
            search_times.append(qtime)
            recall = results['grouped']['BUS_LISTING_ID']['matches']
            recalls.append(recall)
            print('\tRecall: {} results in {} ms'.format(recall, qtime))

            docs = results['grouped']['BUS_LISTING_ID']['doclist']['docs']
            docs_count = len(docs)
            print("\tReturned {} docs.".format(docs_count))

            # Asserts!
            try:
                print("\tAsserting the number of docs...")
                assert category_business_count == docs_count

                # Each Solr document should be of the expected category
                print("\tAsserting the docs' category...")
                for doc in docs:
                    print('\tDoc DW id="{}"\tname="{}"\tcategory="{}", $isExactCategoryMatch={}'.format(
                        doc['id'],
                        doc['BUS_BUSINESS_NAME'],
                        doc['BUS_HEADING'],
                        doc['$isExactCategoryMatch']))
                    assert doc['BUS_HEADING'] == category
                    assert doc['$isExactCategoryMatch']

                # TODO: Check the top document.
                print("\tAsserting the SQL-ranked businesses query...")
                sql_ranked_businesses = dwDbGetTopRankedBusinessInCategoryAndCity(db_cnx, args.loc, category)
                assert len(sql_ranked_businesses) > 0
                print("\tAsserting the SQL-ranked businesses count...")
                assert len(sql_ranked_businesses) == len(docs)
                print("\tAsserting the docs' ranking...")
                for idx, sql_ranked_business in enumerate(sql_ranked_businesses):
                    assert sql_ranked_business.ID_STR == docs[idx]['id']

                printGreen("\tAll asserts PASSED.")
                count_success += 1
                tested_categories[category] = 'PASSED'
            except Exception as e:
                print(e)
                printRed("\tAssert FAILED.")
                tested_categories[category] = 'FAILED'

            # Stop after max count tests
            if count == tests_count:
                break

    except Exception as e:
        printRed(e)
    finally:
        print("\n")
        total_categories_count = dwDbGetAllCategoriesCount(db_cnx)
        print("{} tests executed: {}% of all categories ({})".format(count,
                                                                     round((count / total_categories_count) * 100, 2),
                                                                     total_categories_count))
        print("Tested categories:")
        for key in sorted(tested_categories.keys()):
            result = tested_categories[key]
            if result == 'PASSED':
                printGreen('"{}" - {}'.format(key, result))
            else:
                printRed('"{}" - {}'.format(key, result))
        if count_success == tests_count:
            printSuccess("All tests PASSED!")
        else:
            if count_success > 0:
                printSuccess("{} tests PASSED.".format(count_success))
            printRed("{} tests FAILED".format(count - count_success))

        # Display some stats
        print("\n")
        print("QTime: median={} ms, average={} ms, min={} ms, max={} ms".format(
            np.round(np.median(search_times)),
            np.round(np.average(search_times)),
            np.round(np.min(search_times)),
            np.round(np.max(search_times))))
        print("Recall: median={} ms, average={}, min={}, max={}".format(
            np.round(np.median(recalls)),
            np.round(np.average(recalls)),
            np.round(np.min(recalls)),
            np.round(np.max(recalls))))

        print("Categories' tokens distribution:")
        sorted_keys = sorted(category_tokens_stats.keys())
        for key in sorted_keys:
            print("{}: {}".format(key, category_tokens_stats[key]))

        end_time = time.monotonic()
        print("Execution time: {}".format(timedelta(seconds=end_time - start_time)))

    return count, count_success, 0
