__author__ = 'Guptha'

import sys
import re
import datetime
import filecmp
import time

from src import query1
from src import query2
from src import query3
from src import query4
from src import query4_opt
import load_data


def main():
    timerfile = open('currents/time', 'w')
    inittime = time.time()
    load_data.main(timerfile)
    timerfile.write("Total loading time " + str(time.time() - inittime) + '\n\n')

    initqfulltime = time.time()
    sys.stdout = open('currents/q1out', 'w')
    q1 = getqueryargsmaybeminus("1k-sample-queries1.txt")
    for arg in q1:
        initindiv = time.time()
        query1.main(arg[0], arg[1], arg[2])
        timerfile.write('Query 1 ' + arg[0] + ' ' + arg[1] + ' ' + arg[2] + ' time ' + str(time.time() - initindiv) \
                        + '\n')
    sys.stdout.close()
    timerfile.write('Total q1 time ' + str(time.time() - initqfulltime) + '\n\n')

    initqfulltime = time.time()
    sys.stdout = open('currents/q2out', 'w')
    q2 = getqueryargsnominus("1k-sample-queries2.txt")
    for arg in q2:
        initindiv = time.time()
        absolute_day = datetime.date(int(arg[1]), int(arg[2]), int(arg[3])).toordinal()
        query2.main(absolute_day, arg[0])
        timerfile.write('Query 2 ' + arg[0] + ' ' + str(absolute_day) + ' time ' + str(time.time() - initindiv) \
                        + '\n')
    sys.stdout.close()
    timerfile.write('Total q2 time ' + str(time.time() - initqfulltime) + '\n\n')

    initqfulltime = time.time()
    sys.stdout = open('currents/q3out', 'w')
    q3 = getqueryargsmaybeminus("1k-sample-queries3.txt")
    for arg in q3:
        initindiv = time.time()
        query3.main(arg[0], arg[1], arg[2])
        timerfile.write('Query 3 ' + arg[0] + ' ' + arg[1] + ' ' + arg[2] + ' time ' + str(time.time() - initindiv)
                        + '\n')
    sys.stdout.close()
    timerfile.write('Total q3 time ' + str(time.time() - initqfulltime) + '\n\n')

    initqfulltime = time.time()
    sys.stdout = open('currents/q4out', 'w')
    q4 = getqueryargsmaybeminus("1k-sample-queries4.txt")
    for arg in q4:
        initindiv = time.time()
        query4_opt.main(arg[0], arg[1])
        timerfile.write('Query 4 ' + arg[0] + ' ' + arg[1] + ' time ' + str(time.time() - initindiv) \
                        + '\n')
    sys.stdout.close()
    timerfile.write('Total q4 time ' + str(time.time() - initqfulltime) + '\n\n')

    sys.stdout = open('currents/report', 'w')
    filecmp.dircmp('actuals', 'currents').report()
    sys.stdout.close()

    timerfile.close()


def getqueryargsnominus(filename):
    inputfile = open(filename)
    text = inputfile.readlines()
    for i, line in enumerate(text):
        text[i] = re.findall(r"\w+", line[6:])
    return text


def getqueryargsmaybeminus(filename):
    inputfile = open(filename)
    text = inputfile.readlines()
    for i, line in enumerate(text):
        line = line.replace(" ", "")
        line = line[line.find("(")+1:line.find(")")]
        text[i] = line.split(',')
    return text


if __name__ == '__main__':
    main()