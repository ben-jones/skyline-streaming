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


# import json
import multiprocessing
import Queue
import sys
import time
import traceback
from constants import MASTER_TIMEOUT


class Master():
    def __init__(self, outfile, infiles, num_workers=2):
        self.outfile = outfile
        self.infiles = infiles
        self.num_workers = num_workers
        self.in_qs = []
        self.out_qs = []
        self.step = 0

    def start(self):
        # open output file
        self.output = open(self.outfile, 'w')

        # start the clients
        for worker in range(self.num_workers):
            infile = self.infiles[worker]
            in_q = multiprocessing.Queue()
            out_q = multiprocessing.Queue()
            skyline = []
            self.in_qs.append(in_q)
            self.out_qs.append(out_q)
            self.skylines.append(skyline)
            worker = multiprocessing.Process(target=worker.run_worker,
                                             args=(infile, in_q, out_q))

        # start the master
        self.run()

        # when we get here, we are done!
        self.output.close()

    def process_skyline(self, data, worker):
        """Script to process the skyline for each new data point

        There are 2 phases to this:
        1) update the individual skyline
        2) update the global skyline from the individuals
        3) send out updates to all of the changed skylines

        """
        try:
            # do something here to process the new data point
            pass
        except Exception:
            traceback.print_exc()

    def run(self):
        last_seen = time.time()
        last_seen_round = 0
        while True:
            # run the master loop until we don't have more data
            for worker in range(len(self.in_qs)):
                q = self.in_qs[worker]
                try:
                    # TODO: fix this to avoid a wait
                    data = q.get()

                    # TODO: process entry such that we drop this
                    # worker if it runs out of data

                    # if this is a timecycle ahead, then skip it
                    if data['step'] > self.step:
                        q.put(data)
                        last_seen = time.time()
                        continue

                    # note: ensure that the client sends a token to
                    # indicate that nothing changed
                    self.process_skyline(data, worker)
                    last_seen = time.time()
                    last_seen_round = self.step
                except Queue.empty:
                    continue

            # now that we have finished a round, write out the data
            # and increment the step
            self.write_out_skylines()

            # if we didn't see anything in the timeout period, then
            # stop computing
            if (time.time() - last_seen) > MASTER_TIMEOUT:
                return

            # if we didn't see anything in the last timestep, then
            # stop computing
            if self.step > last_seen_round:
                return

            self.step += 1


if __name__ == "__main__":
    mast = Master(sys.argv[3])
    mast.run(sys.argv[1], sys.argv[2])
