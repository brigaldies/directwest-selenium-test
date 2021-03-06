import argparse
import datetime

from analyzeCategories import *
from testExactCategoryMatches import *
from testHighPrecisionBusinessNameMatches import *
from testJudgementList import *
from testRecommendations import *
from testResidential import *
from testBusinessGeoDistances import *
from testSKLocations import *

if __name__ == "__main__":
    """Usage example: 
    --server apiosc.411.directwest.com 
    --database API 
    --user apiuser 
    --password api12!
    --solr "http://localhost:8983/solr/DWBusiness" 
    --handler DWOmniSearch 
    --test name 
    --count 10 
    --loc Regina
    --filter "%-%"
    """
    start_time = time.monotonic()
    tests_count = 10  # Default
    location = 'Regina'
    try:
        parser = argparse.ArgumentParser(description='Test DirectWest SQL Server database access.')
        parser.add_argument('-s', '--server', help='Database host', required=True)
        parser.add_argument('-d', '--database', help='Database name', required=True)
        parser.add_argument('-u', '--username', help='Database user name', required=True)
        parser.add_argument('-p', '--password', help='Database password', required=True)
        parser.add_argument('-r', '--solr', help='Solr url', required=True)
        parser.add_argument('-n', '--handler', help='Solr handler', required=True)
        parser.add_argument('-t', '--test', help='Test type', required=True)
        parser.add_argument('-c', '--count', help='Tests count', required=False)
        parser.add_argument('-l', '--loc', help='Location', required=False)
        parser.add_argument('-f', '--filter', help='Filter', required=False)

        args = parser.parse_args()
        if args.count:
            tests_count = int(args.count)
        if args.loc is None:
            args.loc = location

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

            with open("report.txt", "a") as report_file:
                test_fixture = ''
                if args.test == "name":
                    test_fixture = 'testHighPrecisionBusinessNameMatches'
                    results = testHighPrecisionBusinessNameMatches(args, cnx, solr_cnx, tests_count)
                elif args.test == "cat":
                    test_fixture = 'testExactCategoryMatches'
                    results = testExactCategoryMatches(args, cnx, solr_cnx, tests_count)
                elif args.test == "judgement":
                    test_fixture = 'testJudgementList'
                    results = testJudgementList(args, cnx, solr_cnx, )
                elif args.test == "analyzecat":
                    test_fixture = 'analyzeCategories'
                    results = analyzeCategories(args, cnx)
                elif args.test == 'recommendations':
                    test_fixture = 'testRecommendations'
                    results = testRecommendations(args, cnx, solr_cnx)
                elif args.test == 'residential':
                    test_fixture = 'testResidential'
                    results = testResidential(args, cnx, solr_cnx)
                elif args.test == 'busgeodist':
                    test_fixture = 'testBusinessGeoDistances'
                    results = testBusinessGeoDistances(args, cnx, solr_cnx)
                elif args.test == 'sklocations':
                    test_fixture = 'testSKLocations'
                    results = testSKLocations(args, cnx, solr_cnx)
                else:
                    raise Exception('Unknown test name'.format(args.test))
                report_file.write(
                    '{} Test fixture {:70.70}: tests count={:5d}, success={:5d}, failed={:5d}, skipped={:5d}, success rate=[{}%]\n'.format(
                        "{:%Y:%m:%d %H:%M:%S}".format(datetime.datetime.now()),
                        '{}(location="{}", filter="{}")'.format(
                            test_fixture,
                            args.loc,
                            args.filter if analyzeCategories is not None else 'None'),
                        results[0],
                        results[1],
                        results[0] - results[1],
                        results[2],
                        round(((results[1] - - results[2]) / results[0]) * 100, 2)))

            # Shutting down...
            print("\nShutting down...")

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
        end_time = time.monotonic()
        print("Tests execution time: {}".format(timedelta(seconds=end_time - start_time)))
