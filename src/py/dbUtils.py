import pyodbc

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
    sql_where = 'lower(BUS_CITY)=lower(?)'
    if not filter is None:
        sql_where += ' and BUS_BUSINESS_NAME like ?'
    sqlStatement = """
        select * from (
            select distinct LTRIM(STR(id, 10)) as ID_STR, LTRIM(STR(BUS_LISTING_ID, 10)) as BUS_LISTING_ID_STR, BUS_BUSINESS_NAME, BUS_EL_TEXT, BUS_HEADING, BUS_CITY, BUS_PRIORITY_RANK, BUS_IS_TOLL_FREE
            from SolrDWBusiness 
            where {}) t
        order by BUS_IS_TOLL_FREE asc, BUS_PRIORITY_RANK desc, BUS_CITY asc, BUS_BUSINESS_NAME asc, BUS_EL_TEXT asc, BUS_LISTING_ID_STR asc, ID_STR asc
    """.format(sql_where)
    try:
        cursor = cnx.cursor()
        print('Executing SQL statement: {} for city "{}"{}'.format(
            sqlStatement,
            city,
            ' and business name filter "{}"'.format(filter) if not filter is None else ''))
        if not filter is None:
            rows = cursor.execute(sqlStatement, (city, filter)).fetchall()
        else:
            rows = cursor.execute(sqlStatement, (city)).fetchall()
        # print("'rows' data type: {}".format(type(rows)))
    except Exception as e:
        printRed('ERROR: An exception occurred when executing "{}":\n{}'.format(
            sqlStatement, e))
    return rows


def dwDbGetCategories(cnx, city):
    rows = None
    sqlStatement = """
            select distinct BUS_HEADING, BUS_CITY from SolrDWBusiness
            where lower(BUS_CITY)=lower(?) 
            order by BUS_HEADING asc 
        """
    try:
        cursor = cnx.cursor()
        rows = cursor.execute(sqlStatement, (city)).fetchall()
        # print("'rows' data type: {}".format(type(rows)))
    except Exception as e:
        printRed('ERROR: An exception occurred when executing "{}":\n{}'.format(
            sqlStatement, e))
    return rows


def dwDbGetAllCategoriesCount(cnx):
    rows = None
    sqlStatement = """
            select count(*) as BUS_ALL_HEADINGS_COUNT from (    
            select distinct BUS_HEADING from SolrDWBusiness) t             
        """
    try:
        cursor = cnx.cursor()
        rows = cursor.execute(sqlStatement).fetchall()
        # print("'rows' data type: {}".format(type(rows)))
        assert len(rows) == 1
    except Exception as e:
        printRed('ERROR: An exception occurred when executing "{}":\n{}'.format(
            sqlStatement, e))
    return rows[0].BUS_ALL_HEADINGS_COUNT


def dwDbGetAllCategories(cnx):
    rows = None
    sqlStatement = """
            select distinct BUS_HEADING from SolrDWBusiness
            order by BUS_HEADING asc 
        """
    try:
        cursor = cnx.cursor()
        rows = cursor.execute(sqlStatement).fetchall()
        # print("'rows' data type: {}".format(type(rows)))
    except Exception as e:
        printRed('ERROR: An exception occurred when executing "{}":\n{}'.format(
            sqlStatement, e))
    return rows


def dwDbGetBusinessesCountByCategory(cnx, city, heading):
    rows = None
    sqlStatement = """
        select count(*) as BUS_CAT_COUNT from (
            select BUS_LISTING_ID, count(*) as BUS_LISTING_ID_CAT_COUNT from SolrDWBusiness
            where BUS_CITY = ? and BUS_HEADING = ?
            group by BUS_LISTING_ID) t
    """
    try:
        cursor = cnx.cursor()
        rows = cursor.execute(sqlStatement, (city, heading)).fetchall()
        # print("'rows' data type: {}".format(type(rows)))
    except Exception as e:
        printRed('ERROR: An exception occurred when executing "{}":\n{}'.format(
            sqlStatement, e))
    return rows


def dwDbGetTopRankedBusinessInCategoryAndCity(cnx, city, heading):
    rows = None

    # Solr sort: $LocalSearchCitySort desc, BUS_IS_TOLL_FREE asc, $dwScore desc, BUS_PRIORITY_RANK desc, BUS_CITY asc, BUS_BUSINESS_NAME_SORT asc, BUS_NAME_EL_SORT asc, BUS_LISTING_ID asc, id asc
    # SQL sort: BUS_IS_TOLL_FREE asc, BUS_PRIORITY_RANK desc, BUS_CITY asc, BUS_BUSINESS_NAME_SORT asc, BUS_NAME_EL_SORT asc, BUS_LISTING_ID asc, id asc
    sqlStatement = """
    select * from (
        select 
        BUS_IS_TOLL_FREE , BUS_CITY , BUS_BUSINESS_NAME , BUS_EL_TEXT , BUS_LISTING_ID , LTRIM(STR(BUS_LISTING_ID, 10)) as BUS_LISTING_ID_STR, id , LTRIM(STR(id, 10)) as ID_STR, max(BUS_PRIORITY_RANK) as BUS_PRIORITY_RANK
        from SolrDWBusiness
        where BUS_CITY = ? and BUS_HEADING = ? 
        group by BUS_IS_TOLL_FREE , BUS_CITY , BUS_BUSINESS_NAME , BUS_EL_TEXT , BUS_LISTING_ID , id) t
    order by BUS_IS_TOLL_FREE asc, BUS_PRIORITY_RANK desc, BUS_CITY asc, BUS_BUSINESS_NAME asc, BUS_EL_TEXT asc, BUS_LISTING_ID_STR asc, ID_STR asc;
    """
    try:
        cursor = cnx.cursor()
        rows = cursor.execute(sqlStatement, (city, heading)).fetchall()
        # print("'rows' data type: {}".format(type(rows)))
    except Exception as e:
        printRed('ERROR: An exception occurred when executing "{}":\n{}'.format(
            sqlStatement, e))
    return rows
