#!/usr/bin/env python
#
# Sairama
# Spring 2016
#
# BasicSkyline.py: strawman implementation of skyline


# stdlib
import Queue


def test_skyline():

    test_list = [[50, 3.0], [51, 5.0], [52, 4.0], [53, 2.0], [65, 1.0],
                 [25, 100.0], [26, 95.0], [49, 4.0], [51, 2.0], [31, 67]]
    skyline_basic(test_list)


def skyline_basic(inputs):
    window, nonSkylinePts = Queue.Queue(), Queue.Queue()
    tuples = Queue.Queue()
    for item in inputs:
        tuples.put(item)
    skyLinePts = skylineBNL(tuples, window, nonSkylinePts)
    print "The skyline points are:"
    while not skyLinePts.empty():
        point = skyLinePts.get_nowait()
        print point


def skylineBNL(in_tuples, window, nonSkylinePts):

    # handle the end cases
    cur_tup = None

    while not in_tuples.empty():
        cur_tup = in_tuples.get_nowait()

        # add the tuple if there is nothing to compare to
        if window.empty():
            window.put(cur_tup)
            continue

        is_dominated = False
        to_see = window.qsize()
        while to_see > 0:
            cmp_tup = window.get_nowait()
            to_see -= 1

            # if result is 1, remove the tuple from the window
            #
            # if result is -1, place the curTuple in the Non-skyline
            #
            # if result is 0, place the curTuple in the 1) window,
            # if place is there else in the temp file.
            is_dom = dominated(cur_tup, cmp_tup)
            if is_dom == 1:
                nonSkylinePts.put(cmp_tup)
            elif is_dom == 0:
                window.put(cmp_tup)
            elif is_dom == -1:
                window.put(cmp_tup)
                nonSkylinePts.put(cur_tup)
                is_dominated = True
                break

        if not is_dominated:
            window.put(cur_tup)

    return window


def dominated(tuple1, tuple2):
    """Function to determine if a tuple is dominated by looking at the min
    value

    """
    dimLength = len(tuple1)
    dominates, dominated = False, False
    index = 0
    while (index < dimLength):
        if (tuple1[index] > tuple2[index]):
            dominated = True
        elif (tuple1[index] < tuple2[index]):
            dominates = True

        index += 1

    if (dominates and (not dominated)):
        return 1
    elif ((not dominates) and dominated):
        return -1
    return 0


if __name__ == "__main__":
    test_skyline()
