from utils import *


def testSKLocations(args, db_cnx, solr_cnx):
    """
    SkLocations core tests.
    :param args: Python script's command-line arguments.
    :param db_cnx: Source SQL database connection.
    :param solr_cnx: Solr connection
    :return: Tuple (Number of tests, number of successful tests, number of skipped tests)
    """

    test_name = 'testSKLocations'
    start_time = time.monotonic()

    tests_count = 0
    success_count = 0
    search_times = []
    recalls = []

    # http://localhost:8983/solr/SKLocations/select?start=0&rows=10&q=CITY_EXACT_FIND:(%22%22Assiniboia%22%22,%22%22Biggar%22%22,%22%22Carlyle%22%22,%22%22Esterhazy%22%22,%22%22Estevan%22%22,%22%22Gravelbourg%22%22,%22%22Hudson Bay%22%22,%22%22Indian Head%22%22,%22%22Ituna%22%22,%22%22Kamsack%22%22,%22%22Kerrobert%22%22,%22%22Kyle%22%22,%22%22Luseland%22%22,%22%22Meadow Lake%22%22,%22%22Melfort%22%22,%22%22Melville%22%22,%22%22Moose Jaw%22%22,%22%22North Battleford%22%22,%22%22Outlook%22%22,%22%22Prince Albert%22%22,%22%22Regina%22%22,%22%22Saint Brieux%22%22,%22%22Saskatoon%22%22,%22%22Shaunavon%22%22,%22%22Swift+Current%22%22,%22%22Tisdale%22%22,%22%22Val+Marie%22%22,%22%22Waskesiu+Lake%22%22,%22%22Watrous%22%22,%22%22Weyburn%22%22,%22%22Yorkton%22%22)&qt=/DWFindNearestCity&pt=50.4552,-104.6376&sort=$DWGeoDistance+asc&version=2.2&wt=json

    # q = 'CITY_EXACT_FIND:("Assiniboia","Biggar","Carlyle","Esterhazy","Estevan","Gravelbourg","Hudson Bay","Indian Head","Ituna","Kamsack","Kerrobert","Kyle","Luseland","Meadow Lake","Melfort","Melville","Moose Jaw","North Battleford","Outlook","Prince Albert","Regina","Saint Brieux","Saskatoon","Shaunavon","Swift Current","Tisdale","Val Marie","Waskesiu Lake","Watrous","Weyburn","Yorkton")'
    q = 'CITY_EXACT_FIND("Regina", "Weyburn")'
    pt = '50.4552,-104.6376'
    sort = '$DWGeoDistance asc'
    wt = 'json'

    params = {
        'start': 0,
        'rows': 15,
        'pt': pt,
        'sort': sort,
        'wt': wt
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
        assert len(docs) == 2

        # {"CITY":"Regina","AtDistanceOf":2.0},{"CITY":"Weyburn","AtDistanceOf":104.0}

        assert docs[0]['CITY'] == 'Regina'
        assert docs[0]['AtDistanceOf'] == 2.0

        assert docs[1]['CITY'] == 'Weyburn'
        assert docs[1]['AtDistanceOf'] == 104.0

        success_count += 1

    except Exception as e:
        print(e)
        printRed("Assert FAILED.")

    testEpilog(test_name, start_time, tests_count, success_count, search_times, recalls)

    return tests_count, success_count, 0
