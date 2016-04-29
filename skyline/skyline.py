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
    sky = Skyline()
    sky.compute_all_sky(test_list)

    print "The skyline points are:"
    while not sky.skyline.empty():
        point = sky.skyline.get_nowait()
        print point


class SkylineException(Exception):
    pass


class Skyline():
    """Class to perform skyline checks"""

    def __init__(self, skyline=None, non_sky=None):
        self.non_sky = Queue.Queue()
        self.skyline = Queue.Queue()
        if skyline is not None:
            self.skyline = skyline
        if non_sky is not None:
            self.non_sky = non_sky

    def compute_all_sky(self, in_tuples):
        """Compute the skyline from a full set of data points"""
        for point in in_tuples:
            self.update_sky_for_point(point)

    def check_dominated(self, point1, point2, remove_dups=True):
        """Compare the two points to see if one dominates the other

        Note: a point dominates another if it has a SMALLER value

        Return values:
        1 if point1 dominates point2
        -1 if point2 dominates point1
        0 if point1 and point2 are incomparable

        """
        dominates, dominated = False, False
        data1, data2 = point1['data'], point2['data']
        if len(data1) != len(data2):
            raise SkylineException("data points have unequal dimensions")
        for idx in range(len(data1)):
            if (data1[idx] > data2[idx]):
                dominated = True
            elif (data1[idx] < data2[idx]):
                dominates = True

        if (dominates and (not dominated)):
            return 1
        elif ((not dominates) and dominated):
            return -1
        if remove_dups and (not dominates):
            return -1
        return 0

    def update_sky_for_point(self, point):
        """Update the skyline for a new data point

        Note: if add_to_updates is true, then it will add an entry to
        skyline updates if the data point does not appear in the
        skyline. This is useful if we are at the master and want to
        know if a point was removed or not

        """
        # add the tuple if there is nothing to compare to
        if self.skyline.empty():
            self.skyline.put(point)
            return

        is_dominated = False
        to_see = self.skyline.qsize()
        while to_see > 0:
            cmp_tup = self.skyline.get_nowait()
            to_see -= 1

            # if result is 1, remove the tuple from the window
            #
            # if result is -1, place the curTuple in the Non-skyline
            #
            # if result is 0, place the curTuple in the 1) window,
            # if place is there else in the temp file.
            is_dom = self.check_dominated(point, cmp_tup)
            if is_dom == 1:
                self.non_sky.put(cmp_tup)
            elif is_dom == 0:
                self.skyline.put(cmp_tup)
            elif is_dom == -1:
                self.skyline.put(cmp_tup)
                self.non_sky.put(point)
                is_dominated = True
                break

        if not is_dominated:
            self.skyline.put(point)

        return not is_dominated

    def reset_updates(self):
        self.updates = []

if __name__ == "__main__":
    test_skyline()
