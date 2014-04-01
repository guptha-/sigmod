__author__ = 'Guptha'

from src import load_data_query_one
from src import load_data_query_two
from src import load_data_query_three
from src import load_data_query_four

import time


def main(timerfile):

    initindiv = time.time()
    load_data_query_one.main()
    timerfile.write('Loading Query 1 time ' + str(time.time() - initindiv) + '\n')

    initindiv = time.time()
    load_data_query_two.main()
    timerfile.write('Loading Query 2 time ' + str(time.time() - initindiv) + '\n')

    initindiv = time.time()
    load_data_query_three.main()
    timerfile.write('Loading Query 3 time ' + str(time.time() - initindiv) + '\n')

    initindiv = time.time()
    load_data_query_four.main()
    timerfile.write('Loading Query 4 time ' + str(time.time() - initindiv) + '\n')


if __name__ == '__main__':
    main(open('currents/time', 'w'))