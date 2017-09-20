import argparse
import pyodbc
import random
import re
import time
from datetime import timedelta
from solr import *
from utils import *


def dwDbConnect(server, database, username, password):
    cnx = None
    try:
        cnxParams = r'DRIVER={ODBC Driver 13 for SQL Server};' + 'SERVER={};DATABASE={};UID={};PWD={}'.format(
            server, database, username, password)
        cnx = pyodbc.connect(cnxParams, autocommit=False)
        printGreen('Successfully connected to database "{}" on host "{}" as user "{}".'.format(
            database, server, username))
    except Exception as e:
        printRed('An exception occurred when connecting to database "{}" on host "{}" as user "{}":\n{}'.format(
            database, server, username, e))
    return cnx


def dwDbClose(cnx):
    try:
        cnx.close()
        printGreen('Successfully closed the database connection.')
        return True
    except Exception as e:
        printRed("ERROR: An exception occurred when closing the database connection!\n{}".format(
            e))
    return False


def dwDbVersion(cnx):
    sqlStatement = "SELECT @@version;"
    try:
        cursor = cnx.cursor()
        cursor.execute(sqlStatement)
        row = cursor.fetchone()
        return row[0]
    except Exception as e:
        printRed('ERROR: An exception occurred when executing "{}":\n{}'.format(
            sqlStatement, e))
    return None


def dwDbGetBusinesses(cnx, city, filter):
    rows = None
    sqlStatement = """
        select * from (
        select distinct LTRIM(STR(id, 10)) as ID_STR, LTRIM(STR(BUS_LISTING_ID, 10)) as BUS_LISTING_ID_STR, BUS_BUSINESS_NAME, BUS_CITY, BUS_PRIORITY_RANK
        from SolrDWBusiness 
        where lower(BUS_CITY)=lower(?) and BUS_BUSINESS_NAME like ?) t
        order by BUS_BUSINESS_NAME asc, BUS_CITY asc, BUS_PRIORITY_RANK desc, BUS_LISTING_ID_STR asc, ID_STR asc
    """
    try:
        cursor = cnx.cursor()
        rows = cursor.execute(sqlStatement, (city, filter)).fetchall()
        # print("'rows' data type: {}".format(type(rows)))
    except Exception as e:
        printRed('ERROR: An exception occurred when executing "{}":\n{}'.format(
            sqlStatement, e))
    return rows


if __name__ == "__main__":
    """Usage example: --server apiosc.411.directwest.com --database API --user apiuser --password api12! --solr "http://localhost:8983/solr/DWBusiness" --handler DWOmniSearch --test 10
    """
    start_time = time.monotonic()
    count = 0
    count_success = 0
    count_precise_name_match = 0
    count_partial_name_match = 0
    max_count = 10 # Default
    try:
        parser = argparse.ArgumentParser(description='Test DirectWest SQL Server database access.')
        parser.add_argument('-s', '--server', help='Database host', required=True)
        parser.add_argument('-d', '--database', help='Database name', required=True)
        parser.add_argument('-u', '--username', help='Database user name', required=True)
        parser.add_argument('-p', '--password', help='Database password', required=True)
        parser.add_argument('-r', '--solr', help='Solr url', required=True)
        parser.add_argument('-n', '--handler', help='Solr handler', required=True)
        parser.add_argument('-t', '--test', help='Tests count', required=False)

        args = parser.parse_args()
        if args.test:
            max_count = int(args.test)

        print("Executing {} tests.".format(max_count))

        print('Connecting to database "{}" on host "{}" as user "{}"...'.format(args.database, args.server,
                                                                                args.username))
        cnx = dwDbConnect(args.server, args.database, args.username, args.password)
        db_version = dwDbVersion(cnx)
        if cnx is not None:
            print('Database connection: {}'.format(cnx.getinfo(pyodbc.SQL_DATA_SOURCE_NAME)))
            print('Database version: {}'.format(db_version))

            # Connect to Solr
            solr_cnx = solr_connect(args.solr, args.handler)
            if solr_cnx is None:
                raise Exception('Solr connection issue')

            # Retrieve hyphenated business names
            rows = dwDbGetBusinesses(cnx, 'Regina', '%-%')

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
                results = solr_search(solr_cnx, search, random_row.BUS_CITY)
                print('\tRecall: {} results'.format(results['grouped']['BUS_LISTING_ID']['matches']))
                docs = results['grouped']['BUS_LISTING_ID']['doclist']['docs']
                top_doc = docs[0]
                print('\tPrecision: Top result=id "{}", name="{}", business rank={}, preciseNameMatch={}, partialNameMatch={}'.format(
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
                    print("Asserting business name...")
                    # The business name comparison uses lower case in order to accommodate cases like 'SAIL - Aids To Independent Living',
                    # which is also spelled 'SAIL - Aids to Independent Living'.
                    assert top_doc['BUS_BUSINESS_NAME'].lower() == random_row.BUS_BUSINESS_NAME.lower()
                    print("Asserting business name match types...")
                    assert top_doc['$isPreciseNameMatch'] or top_doc['$isPartialNameMatch']

                    # Retrieve the source database records for the given name, sorted by descending BUS_PRIORITY_RANK and BUS_LISTING_ID
                    business_rows = dwDbGetBusinesses(cnx, 'Regina', top_doc['BUS_BUSINESS_NAME'])
                    print("Asserting business' source database records...")
                    assert len(business_rows) > 0
                    print("\tTop database source record: id={}, business rank={}".format(business_rows[0].ID_STR, business_rows[0].BUS_PRIORITY_RANK))
                    print("Asserting ranking...")
                    assert business_rows[0].ID_STR == top_doc['id']
                    printGreen("All asserts PASSED.")
                    count_success += 1
                except Exception as e:
                    print(e)
                    printRed("Assert FAILED.")

                # Stop after max count tests
                if count == max_count:
                    break

            # Close the Solr connection
            print('Closing the solr connection...')
            try:
                solr_cnx.get_session().close()
                printGreen("Successfully closed the Solr connection.")
            except:
                printRed("An error occurred when closing the Solr connection:\n{}".format(e))

            print('Closing the database connection...')
            dwDbClose(cnx)
        else:
            print('No connection handler!')
    except Exception as e:
        printRed(e)
    finally:
        print("{} tests executed: {} precise and {} partial name matches.".format(count, count_precise_name_match,
                                                                                  count_partial_name_match))
        if count_success == max_count:
            printSuccess("All tests PASSED!")
        else:
            if count_success > 0:
                printSuccess("{} tests PASSED.".format(count_success))
            printRed("{} tests FAILED".format(count - count_success))
        end_time = time.monotonic()
        print("Tests execution time: {}".format(timedelta(seconds=end_time - start_time)))
