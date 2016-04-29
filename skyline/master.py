#!/usr/bin/env python
#
# Spring 2016
# Ben Jones
#
# master.py: web server that will act as the central/master node for
# computing the global skyline.
#
# assumptions:
#
# 1) the data will be ordered according to time step
#
# 2) workers will send a noop token to the master if their skyline did
# not change for the timestep or they had no data
#
# 3) workers will register with the master before starting computation


# stdlib imports
import argparse
import json
import Queue
import time
import threading
import traceback


# non-stdlib imports
import flask
from flask import Flask

# this creates the global web server object
app = Flask(__name__)


# local imports
from constants import MASTER_TIMEOUT_TO_END, MASTER_WAIT_TIME
from skyline import Skyline


class Master():
    def __init__(self, outfile, start_time, step_size, win_size,
                 num_workers=2, workers=None):
        self.outfile = outfile
        self.num_workers = num_workers
        self.skyline = []
        self.non_skyline = []
        self.sky_received = 0
        self.unprocessed_sky = []
        self.recv_workers = {}

        self.start_time = start_time
        self.window_time = start_time
        self.win_size = win_size
        self.step = 0
        self.step_size = step_size
        self.last_received_time = time.time()

        self.is_running = False
        self.is_waiting = False
        self.is_computing = False
        self.have_new_data = False

        self.status_lock = threading.Lock()
        self.data_lock = threading.Lock()

        self.sky = Skyline()
        self.skylines = {}

    def process_skyline(self):
        """Script to process the skyline for each new data point

        There are 2 phases to this:
        1) update the individual skyline
        2) update the global skyline from the individuals

        """
        work_seen = {}
        global_seen = {}
        try:
            # update each local skyline
            for sky in self.unprocessed_sky:
                worker = sky['worker_id']
                added, removed = sky['added'], sky['removed']
                work_seen[worker] = {}

                # add and remove the entries
                if worker not in self.skylines:
                    self.skylines[worker] = Queue.Queue()

                # remove the entries we don't need
                to_see = self.skylines[worker].qsize()
                for idx in range(to_see):
                    item = self.skylines[worker].get_nowait()
                    if item in removed:
                        continue
                    self.skylines[worker].put(item)
                    work_seen[worker][item['data']] = item
                    global_seen[item['data']] = item

                # and add the new entries
                for item in added:
                    self.skylines[worker].put(item)
                    work_seen[worker][item['data']] = item
                    global_seen[item['data']] = item

            # now update the global skyline based on the items
            self.sky = Skyline()
            for key in global_seen:
                item = global_seen[key]
                self.sky.update_sky_for_point(item)

            # snapshot the global skyline
            skys = {}
            while not self.sky.skyline.empty():
                item = self.sky.skyline.get_nowait()
                skys[item['data']] = item
            old_keys = set(skys.keys())

            # return the difference to each worker
            for worker in work_seen:
                new_keys = set(work_seen[worker].keys())
                added = new_keys - old_keys
                removed = old_keys - new_keys
                added = map(lambda x: work_seen[worker][x], added)
                removed = map(lambda x: skys[x], removed)
                update = {'step': self.step, 'added': added,
                          'removed': removed, 'worker_id': worker}
                self.skyline_changes[worker] = update

            self.unprocessed_sky = []
            self.recv_workers = {}
            self.sky_received = 0
        except (SystemExit, KeyboardInterrupt) as exp:
            raise(exp)
        except Exception:
            traceback.print_exc()

    def write_out_skyline(self):
        entry = {'step': self.step - 1, 'data': self.skyline}
        self.output.write(json.dumps(entry))

    def run_loop(self):
        # open output file
        self.output = open(self.outfile, 'w')

        # initialize our variables
        self.status_lock.acquire()
        self.is_running = True
        self.is_waiting = True
        self.is_computing = False
        self.step = 0
        self.window_time = self.start_time
        self.status_lock.release()

        # the main run loop will see if the web server has brought us
        # any goodies, then run the skyline if possible or preempt for
        # a few seconds until we can
        keep_running = True
        try:
            while keep_running:
                self.status_lock.acquire()

                # if we are out of workers, then quit
                if self.num_workers <= 0:
                    print "Out of workers, so quitting"
                    self.status_lock.release()
                    keep_running = False
                    continue

                # if we don't have anything new, preempt until we have
                # something. If we have waited more than the timeout
                # period, then end
                if not self.have_new_data:
                    time_since_update = time.time() - self.last_received_time
                    if (time_since_update > MASTER_TIMEOUT_TO_END):
                        print ("Didn't receive anything in the timeout, "
                               "so ending")
                        self.status_lock.release()
                        keep_running = False
                        continue

                    self.status_lock.release()
                    time.sleep(MASTER_WAIT_TIME)
                    continue

                # check how many workers have given us the skyline for
                # this timestep to determine if we can compute the
                # skyline
                if self.sky_received == self.num_workers:
                    self.waiting = False
                    self.is_computing = True
                    self.sky_received = 0
                    self.step += 1
                    time_elapsed = (self.step * self.step_size)
                    self.window_time = self.start_time + time_elapsed
                    self.status_lock.release()

                    self.data_lock.acquire()
                    self.process_skyline()
                    # now that we have finished a round, write out the
                    # data and increment the step
                    self.write_out_skyline()
                    self.data_lock.release()

                    self.status_lock.acquire()
                    self.is_computing = False
                    self.is_waiting = True
                    self.status_lock.release()
                    continue
                else:
                    self.status_lock.release()
                    continue
            self.data_lock.acquire()
            self.process_skyline()
            # write out data from the final round
            self.write_out_skyline()
            self.data_lock.release()

        except (SystemExit, KeyboardInterrupt) as exp:
            raise(exp)
        except Exception:
            # self.status_lock.release()
            traceback.print_exc()
            print "Encountered problem in backend"
        self.is_running = False
        # when we get here, we are done!
        self.output.close()


@app.errorhandler(404)
def not_found(error):
    return flask.make_response(flask.jsonify({'error': 'Not found'}), 404)


@app.errorhandler(400)
def bad_request(error):
    return flask.make_response(flask.jsonify({'error': 'Bad request'}), 400)


@app.errorhandler(423)
def locked_data(error):
    error_json = flask.jsonify({'error': 'Data is locked for computation'})
    return flask.make_response(error_json, 423)


@app.route('/status')
def check_status():
    data.status_lock.acquire()
    running = {'is_running': data.is_running,
               'is_computing': data.is_computing,
               'is_waiting': data.is_waiting,
               'num_workers': data.num_workers,
               'num_received': data.sky_received,
               'num_threads': threading.active_count()}
    data.status_lock.release()
    return flask.make_response(flask.jsonify(running), 200)


@app.route('/step')
def get_step():
    data.status_lock.acquire()
    step = {'step': data.step, 'step_size': data.step_size,
            'start_time': data.start_time, 'window_time': data.window_time,
            'step_window': data.win_size}
    data.status_lock.release()
    return flask.make_response(flask.jsonify(step), 200)


@app.route('/worker_done')
def remove_worker():
    """When a worker is done, reduce the count of the number of workers"""
    # worker_id = flask.request.args.get('worker_id')
    data.status_lock.acquire()
    step = {'step': data.step, 'step_size': data.step_size,
            'start_time': data.start_time, 'window_time': data.window_time}
    data.num_workers -= 1
    data.status_lock.release()
    return flask.make_response(flask.jsonify(step), 200)


@app.route('/get_skyline')
@app.route('/get_skyline/<step>')
def get_skyline(step=None):
    """Return the skyline for the most recent step or return an error if
    requested step is not available

    """
    worker_id = flask.request.args.get('worker_id')

    try:
        data.status_lock.acquire()
        is_computing = data.is_computing
        is_running = data.is_running
        already_recv_worker = (worker_id in data.recv_workers)
        proc_step = data.step - 1
    finally:
        data.status_lock.release()

    if not is_running:
        flask.abort(400)

    # tell the worker that we are still computing with status code 423
    # if a) we are currently computing the skyline or b) the worker
    # has already given us data for the next time step
    if is_computing:
        error_json = flask.jsonify({'error': 'Data is locked for computation'})
        return flask.make_response(error_json, 423)

    # if a specific timestep is requested, make sure that it is the
    # last time step
    if step is not None:
        step = int(step)
        if step != proc_step:
            # if we already received data from this worker, then wait longer
            if already_recv_worker:
                error_json = flask.jsonify({'error':
                                            'Data is locked for computation'})
                return flask.make_response(error_json, 423)

            err_text = ("last processed step is {}, not {}"
                        "".format(proc_step, step))
            error_json = flask.jsonify({'status': 'error', 'error': err_text,
                                        'step': proc_step})
            return flask.make_response(error_json, 400)

    # otherwise return the latest skyline
    data.data_lock.acquire()
    # sky = {'step': (data.step - 1), 'data': data.skyline}
    sky = {'step': (data.step - 1), 'data': data.skyline_changes[worker_id]}
    data.data_lock.release()
    return flask.make_response(flask.jsonify(sky), 200)


@app.route('/update_master', methods=['POST'])
def accept_data():
    """Add the skyline from this client to the list of skylines to be
    updated

    """
    # if they didn't include data, then generate an error
    if not flask.request.json:
        flask.abort(400)

    # ensure that the data point corresponds to the correct time step
    local_skyline = flask.request.get_json()
    try:
        data.status_lock.acquire()
        if local_skyline.get('step') != data.step:
            error = {'error': 'Incorrect time step', 'status': 'error'}
            return flask.make_response(flask.jsonify(error), 400)
    finally:
        data.status_lock.release()

    # now add the skyline into the potential skylines
    #
    # TODO: consider adding a check to ensure that each worker only
    # contributes once
    data.data_lock.acquire()
    data.unprocessed_sky.append(local_skyline)
    data.sky_received += 1
    data.recv_workers[local_skyline['worker_id']] = True
    data.data_lock.release()

    data.status_lock.acquire()
    data.have_new_data = True
    data.last_received_time = time.time()
    data.status_lock.release()

    return flask.make_response(flask.jsonify({'status': 'success'}), 200)


def parse_args():
    parser = argparse.ArgumentParser(prog='skyline-master')
    parser.add_argument('--output', required=True,
                        help='file to write skyline at each step to')
    parser.add_argument('--start', required=True, type=int,
                        help='epoch timestamp for the start time for analysis')
    parser.add_argument('--step', required=True, type=int,
                        help='step size in seconds')
    parser.add_argument('--win-size', required=True, type=int,
                        help='window size (number of steps)')
    parser.add_argument('--num-workers', required=True, type=int,
                        help='number of workers to wait for')
    return parser.parse_args()


if __name__ == "__main__":
    # parse the CLI arguments
    args = parse_args()

    # create appropriate global datastructures
    data = Master(args.output, args.start, args.step, args.win_size,
                  num_workers=args.num_workers)

    # start the background computation thread. This thread will
    # compute the skyline when appropriate and will update info on the
    # state of computation
    print "starting background thread"
    bg = threading.Thread(target=data.run_loop)
    bg.daemon = True
    bg.start()

    # start the webserver
    app.run(debug=True)
