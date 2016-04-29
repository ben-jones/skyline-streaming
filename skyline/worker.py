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
import random
import string
import time


# non stdlib imports
import requests


# local imports
from constants import WORKER_REQUERIES, WORKER_MASTER_WAIT, SERVER_TIMEOUT
from constants import SERVER_REQUERIES
from skyline import Skyline

# constants
UPLOAD_WAIT = 5


def create_nonce(length):
    letters = [random.choice(string.letters) for x in range(length)]
    nonce = "".join(letters)
    return nonce

def process_line(line):
    entry = json.loads(line)
    entry['data'] = tuple(entry['data'])


class Worker():
    def __init__(self, infile, master, process_line=None):
        self.infile = infile
        self.inputf = open(infile, 'r')
        self.master_url = master

        # TODO: uncomment this
        self.worker_id = "test"
        # self.worker_id = create_nonce(10)

        self.process_line = json.loads
        if process_line is not None:
            self.process_line = process_line

        # verify that we can actually talk to the master by trying to
        # get information about the step size
        self.verify_master()

        # create the skyline stuff
        self.sky = Skyline()
        self.old_skys = {}

    def verify_master(self):
        """Verify the location of the master and get the time step and size"""

        req = requests.get(self.master_url + "/step", timeout=SERVER_TIMEOUT)
        req.raise_for_status()
        entry = req.json()
        self.step = entry['step']
        self.step_size = entry['step_size']
        self.win_size = entry['step_window']
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
                time.sleep(UPLOAD_WAIT)
                self.get_master_updates()

            # now update the skyline using this point
            self.update_skyline(entry)
        self.inputf.close()
        self.upload_data()
        req = requests.get(self.master_url + "/worker_done")
        req.raise_for_status()

    def upload_data(self):
        """Upload the changes to the skyline to the master node

        We will perform the following activities here:
        1) find difference in old and new skyline (skyline updates to
           send to master)
        2) send data to master

        """
        # find the difference in old and new skyline (skyline updates
        # to send to master
        added, removed = self.find_skyline_diff()

        url = self.master_url + "/update_master"
        headers = {'content-type': 'application/json'}
        params = {'worker_id': self.worker_id}
        upload_data = {'step': self.step, 'added': added, 'removed': removed,
                       'worker_id': self.worker_id}

        # upload the data, but make sure that we try several times on failure
        for x in range(SERVER_REQUERIES):
            req = requests.post(url, timeout=SERVER_TIMEOUT, headers=headers,
                                data=json.dumps(upload_data), params=params)
            if req.status_code == 200:
                break
            # wait a few seconds before retrying
            time.sleep(SERVER_TIMEOUT)
        # ensure that we actually uploaded successfully
        req.raise_for_status()

    def find_skyline_diff(self):
        # first compute the new skyline's set
        skys = {}
        while not self.sky.skyline.empty():
            item = self.sky.skyline.get_nowait()
            skys[tuple(item['data'])] = item
        new_keys = set(skys.keys())
        old_keys = set(self.old_skys.keys())
        added = new_keys - old_keys
        removed = old_keys - new_keys
        added = map(lambda x: skys[x], added)
        removed = map(lambda x: self.old_skys[x], removed)
        return added, removed

    def get_master_updates(self):
        """Update the local skyline based on points from the master/central
        node's skyline

        To get the skyline, we will query the master server a total of
        WORKER_REQUERIES times and wait WORKER_MASTER_WAIT seconds
        before declaring failure/ raising an exception

        We will perform the following activities here
        1) update local skyline based on master updates
        2) expire old points

        """
        params = {'worker_id': self.worker_id}
        for x in range(WORKER_REQUERIES):
            url = "{}/get_skyline/{}".format(self.master_url, self.step)
            req = requests.get(url, timeout=SERVER_TIMEOUT, params=params)

            # if we got a successful response, then let's break out
            if req.status_code == 200:
                break
            # if currently computing or waiting for other nodes, then
            # wait longer
            elif req.status_code == 423:
                time.sleep(WORKER_MASTER_WAIT)
            # otherwise, just break out now with an error
            else:
                req.raise_for_status()

        data = req.json()
        self.step += 1

        # handle the removals and additions in a single pass
        to_remove, old_skys = {}, {}
        for point in data['removed']:
            to_remove[point['data']] = point

        to_see = self.sky.skyline.qsize()
        for idx in range(to_see):
            point = self.sky.skyline.get_nowait()
            if point['data'] in to_remove:
                continue
            self.sky.skyline.put(point)
            old_skys[point['data']] = point
        for point in data['added']:
            self.sky.skyline.put(point)
            old_skys[point['data']] = point

        # now that we have the global skyline from the previous
        # timestep, let's create a datastructure to snapshot what we
        # will later add and remove
        self.old_skys = old_skys

        # expire points from the skyline
        self.expire_points()

    def expire_points(self):
        """Expire old points from the skyline"""

        has_expired = False
        while not self.sky.skyline.empty():
            item = self.sky.skyline.get_nowait()
            if item['step'] < (self.step - self.win_size):
                has_expired = True
            else:
                self.sky.skyline.put(item)

        # if we have not expired any skyline points, then we don't
        # need to check the non-skyline points and we are done
        if not has_expired:
            return

        # rerun and expire all of the non-skyline points in a single
        # check
        while not self.sky.non_sky.empty():
            item = self.sky.non_sky.get_nowait()
            if item['step'] < (self.step - self.win_size):
                has_expired = True
            else:
                self.update_skyline(item)

    def update_skyline(self, point):
        """Update the local skyline based on this point

        Note: when the skyline changes, we also need to update
        self.skyline_updates because that is what will be sent to the
        master

        """
        self.sky.update_sky_for_point(point)


def run_worker(infile, master):
    worker = Worker(infile, master)
    worker.run()


# note that we plan to operate on netflow data in a csv format. There
# is a 10k line sample of the data in netflow.example

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
