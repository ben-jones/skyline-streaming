#!/usr/bin/env python
#
# Spring 2016
# Ben Jones
#
# master.py: script that will take in the skylines from the workers
# and generate the global skyline at each time step
#
# assumptions:
#
# 1) the data will be ordered according to time step
#
# 2) all workers will have entries for each timestemp
#
# 3) workers will send a noop token to the master if their skyline did
# not change for the timestep
#
# 4) the first time step will be 0


import json
import multiprocessing
import Queue
import sys
import traceback


class Client():
    def __init__(self, infile, in_q, out_q):
        self.infile = infile
        self.input = open(infile, 'r')
        self.in_q = in_q
        self.out_q = out_q
        self.step = 0

    def run(self):
        """Method to read in the streaming entries, process the skyline, and
        send results to the master

        """
        # read in the entries for this step
        for line in self.input.xreadlines():
            entry = json.loads(line)

            # if we are moving beyond this timestep, then wait for
            # more data from the master
            if entry['step'] > self.step:
                self.get_master_updates()

            # now update the skyline using this point
            self.update_skyline(entry)
        self.input.close()

    def update_skyline(self, point):
        """Update the local skyline based on this point"""
        pass

    def get_master_updates(self):
        """Update the local skyline based on points from the master/central
        node's skyline

        """
        # update local skyline for each point from master
        break_out = False
        while (not break_out):
            try:
                data = self.in_q.get(10)
                point = data['data']
                self.update_skyline(point)
            except Queue.Empty:
                break_out = True


def run_worker(infile, in_q, out_q):
    worker = Client(infile, in_q, out_q)
    worker.run()
