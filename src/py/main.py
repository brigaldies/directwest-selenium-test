import argparse

from analyzeCategories import *
from testExactCategoryMatches import *
from testHighPrecisionBusinessNameMatches import *

if __name__ == "__main__":
    """Usage example: --server apiosc.411.directwest.com --database API --user apiuser --password api12! --solr "http://localhost:8983/solr/DWBusiness" --handler DWOmniSearch --test name --count 10 --loc Regina
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

            if args.test == "name":
                testHighPrecisionBusinessNameMatches(args, cnx, solr_cnx, tests_count)
            elif args.test == "cat":
                testExactCategoryMatches(args, cnx, solr_cnx, tests_count)
            elif args.test == "analyzecat":
                analyzeCategories(args, cnx)
            else:
                raise Exception('Unknown test name'.format(args.test))

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
