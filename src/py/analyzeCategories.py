from dbUtils import *


def analyzeCategories(args, cnx):
    """
    Analyze the DW categories.
    :param args: Python script's command-line arguments.
    :param cnx: The source SQL database connection.
    :return: Constant tuple (0, 0, 0) to be consistent with the return type expected by main.py
    """
    rows = dwDbGetAllCategories(cnx)

    print('{} distinct categories across all locations.'.format(len(rows)))

    cat_tokens_stats = {}

    # Create a dict counting the number of categories by tokens count
    for row in rows:
        category = row.BUS_HEADING
        category_tokens = category.split()
        tokens_count = len(category_tokens)
        # print('Category "{}": {} tokens'.format(category, tokens_count))
        if not tokens_count in cat_tokens_stats:
            cat_tokens_stats[tokens_count] = 0
        cat_tokens_stats[tokens_count] += 1

    # Sort the dict
    sorted_keys = sorted(cat_tokens_stats.keys())
    print("Counts: {}".format(sorted_keys))
    for key in sorted_keys:
        print("{}: {}".format(key, cat_tokens_stats[key]))

    return 0, 0, 0
