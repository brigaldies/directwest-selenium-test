import time
from datetime import timedelta

import numpy as np


def printRed(content): print("\033[31m{}\033[00m".format(content))


def printGreen(content): print("\033[01m\33[32m{}\033[00m".format(content))


def printSuccess(content): print("\033[01m\33[42m{}\033[00m".format(content))


def printBlue(content): print("\033[34m{}\033[00m".format(content))


def testEpilog(test_name, start_time, tests_count, success_count, search_times, recalls):
    print("\n")
    print("{} test{} executed.".format(tests_count, 's' if tests_count >= 2 else ''))
    if tests_count > 0:
        if success_count == tests_count:
            printSuccess("All tests PASSED!")
        else:
            if success_count > 0:
                printSuccess("{} tests PASSED.".format(success_count))
            printRed("{} tests FAILED".format(tests_count - success_count))

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
