__author__ = 'Guptha'

import sys
import re
import datetime

from src import query1
from src import query2
from src import query3
import load_data


def main():
    load_data.main()

    sys.stdout = open('q1out', 'w')
    q1 = getqueryargsmaybeminus("1k-sample-queries1.txt")
    for arg in q1:
        query1.main(arg[0], arg[1], arg[2])
    sys.stdout.close()

    sys.stdout = open('q2out', 'w')
    q2 = getqueryargsnominus("1k-sample-queries2.txt")
    for arg in q2:
        absolute_day = datetime.date(int(arg[1]), int(arg[2]), int(arg[3])).toordinal()
        query2.main(absolute_day, arg[0])
    sys.stdout.close()

    sys.stdout = open('q3out', 'w')
    q3 = getqueryargsnominus("1k-sample-queries3.txt")
    for arg in q3:
        query3.main(arg[0], arg[1], arg[2])
    sys.stdout.close()


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
        text[i] = re.findall(r"\w+|-\w+", line[6:])
    return text

if __name__ == '__main__':
    main()