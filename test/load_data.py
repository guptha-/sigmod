__author__ = 'Guptha'

from src import load_data_query_one
from src import load_data_query_two
from src import load_data_query_three
from src import load_data_query_four
from src import batching_tmp

import time


def main(timerfile, querydir):

    initindiv = time.time()
    batching_tmp.main(querydir)
    timerfile.write('Loading Query 1 time ' + str(time.time() - initindiv) + '\n')
    print "Load 1 done"

    initindiv = time.time()
    load_data_query_two.main(querydir)
    timerfile.write('Loading Query 2 time ' + str(time.time() - initindiv) + '\n')
    print "Load 2 done"

    initindiv = time.time()
    load_data_query_three.main(querydir)
    timerfile.write('Loading Query 3 time ' + str(time.time() - initindiv) + '\n')
    print "Load 3 done"

    initindiv = time.time()
    load_data_query_four.main(querydir)
    timerfile.write('Loading Query 4 time ' + str(time.time() - initindiv) + '\n')
    print "Load 4 done"


if __name__ == '__main__':
    main(open('currents/time', 'w'), "1k")