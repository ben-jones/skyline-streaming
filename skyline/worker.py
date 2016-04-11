#!/usr/bin/env python
#
# Spring 2016
# Ben Jones
#
# worker.py: script to compute the skyline from a local file
#
# Assumptions:
#
# 1) all data in the input file is in chronological order
#
# 2) all skyline computation at the master will take less than
# WORKER_REQUERIES * WORKER_MASTER_WAIT seconds, which is currently 15
# minutes

# stlib imports
import argparse
import json
import time


# non stdlib imports
import requests


# local imports
from constants import WORKER_REQUERIES, WORKER_MASTER_WAIT, SERVER_TIMEOUT
from constants import SERVER_REQUERIES


class Worker():
    def __init__(self, infile, master, process_line=None):
        self.infile = infile
        self.inputf = open(infile, 'r')
        self.master_url = master

        self.process_line = json.loads
        if process_line is not None:
            self.process_line = process_line

        # verify that we can actually talk to the master by trying to
        # get information about the step size
        self.verify_master()

        self.skyline = []
        self.skyline_updates = []

    def verify_master(self):
        """Verify the location of the master and get the time step and size"""

        req = requests.get(self.master_url + "/step", timeout=SERVER_TIMEOUT)
        req.raise_for_status()
        entry = req.json()
        self.step = entry['step']
        self.step_size = entry['step_size']
        self.start_time = entry['start_time']
        self.window_start = entry['window_time']
        self.window_end = self.window_start + self.step_size

    def run(self):
        """Method to read in the streaming entries, process the skyline, and
        send results to the master

        """
        print ("Worker is now running at step {} with step_size {} starting "
               "at time {}".format(self.step, self.step_size, self.start_time))
        # read in the entries for this step
        processed = 0
        for line in self.inputf.xreadlines():
            entry = self.process_line(line)

            processed += 1
            if (processed % 1) == 0:
                print "Processed {} entries".format(processed)

            # if we are moving beyond this timestep, then wait for
            # more data from the master
            if entry['step'] > self.step:
                self.upload_data()
                self.get_master_updates()

            # now update the skyline using this point
            self.update_skyline(entry)
        self.inputf.close()

    def upload_data(self):
        """Upload the changes to the skyline to the master node"""

        url = self.master_url + "/update_master"
        # upload the data, but make sure that we try several times on failure
        for x in range(SERVER_REQUERIES):
            req = requests.get(url, timeout=SERVER_TIMEOUT)
            if req.status_code == 200:
                break
        # ensure that we actually uploaded successfully
        req.raise_for_status()

    def get_master_updates(self):
        """Update the local skyline based on points from the master/central
        node's skyline

        To get the skyline, we will query the master server a total of
        WORKER_REQUERIES times and wait WORKER_MASTER_WAIT seconds
        before declaring failure/ raising an exception

        """
        for x in range(WORKER_REQUERIES):
            url = self.master_url + "/get_skyline"
            req = requests.get(url, timeout=SERVER_TIMEOUT)

            # if we got a successful response, then let's break out
            if req.status_code == 200:
                break
            # if the computation is under way, so there is nothing for
            # us yet, then keep waiting
            elif req.status_code == 423:
                time.sleep(WORKER_MASTER_WAIT)
            # otherwise, we hit an error, so break out
            else:
                req.raise_for_status()

        data = req.json()
        self.step = data['step'] + 1
        for point in data['data']:
            self.update_skyline(point)

    def update_skyline(self, point):
        """Update the local skyline based on this point

        Note: when the skyline changes, we also need to update
        self.skyline_updates because that is what will be sent to the
        master

        """
        pass


def run_worker(infile, master):
    worker = Worker(infile, master)
    worker.run()


def parse_args():
    parser = argparse.ArgumentParser(prog='skyline-worker')
    parser.add_argument('--input', required=True,
                        help='file to read skyline data from')
    parser.add_argument('--master', required=True,
                        help='Base of URL for accessing master')
    return parser.parse_args()


if __name__ == "__main__":
    # parse the CLI arguments
    args = parse_args()

    run_worker(args.input, args.master)
