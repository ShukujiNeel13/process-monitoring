import os
import time
from pprint import pformat

import subprocess
import signal
import redis
import multiprocessing
from ast import literal_eval

# R = redis.StrictRedis(decode_responses=True, db=1)
# R.flushdb()


def handle_msg(msg: str, p_q: multiprocessing.Queue, p_index: int, wait_seconds: int):

    print('In handle_msg (worker child process). Message received is:')
    print(msg)

    current_pid = os.getpid()
    print(f'  => In handler process: os.getpid() is: {current_pid}')
    p_obj = multiprocessing.current_process()
    print(f'  => In handler process: mp.current_process() is:\n{pformat(p_obj)}')
    p_obj_pid = p_obj.pid
    print(f'  => In handler process: mp.current_process().pid is: {p_obj_pid}')
    p_name = p_obj.name
    print('Current process name is:')
    print(p_name)

    current_time = int(time.time())
    process_data = {'pid': current_pid, 'spawn_time': current_time, 'index': p_index}

    p_q.put(process_data)

    # text = f'PID: {pid} -> Message received: {msg} at time: {unix_time}'
    # response = {'text': text, 'unix_time': unix_time, 'pid': pid}
    # print(text)
    # R.hset('ProcessKey', pid, response)
    time.sleep(wait_seconds)


# Sub Process Task 1
def start_worker_process(p_q: multiprocessing.Queue):

    worker_process_id = os.getpid()
    print(f'In start_worker_process() | Current PID: {worker_process_id}')

    # p_q.put(current_pid)
    # print('Added this ProcessID in p_q')

    no_of_times_to_run = 10
    for i in range(no_of_times_to_run):
        unix_time = int(time.time())
        msg_data = f'Message:{i+1} @ Time: {unix_time}'
        msg = {'data': msg_data}

        if i == 3 or i == 6 or i == 9:
            start_worker_child_process(msg, p_q, p_index=i, wait_seconds=12)
        else:
            start_worker_child_process(msg, p_q, p_index=i, wait_seconds=5)
        time.sleep(0.5)


# Sub Process Task 2
def start_monitoring_process(p_q: multiprocessing.Queue, max_process_duration_seconds: int = 9):

    current_pid = os.getpid()
    print(f'Started monitoring process | Current PID: {current_pid}')

    count = 0
    while True:
        item_in_queue = p_q.get()
        print(f'\nItem in queue is:')
        print(pformat(item_in_queue))

        worker_sub_pid = item_in_queue['pid']
        worker_sub_p_start_time = item_in_queue['spawn_time']
        worker_sub_p_index = item_in_queue['index']

        curr_time = int(time.time())

        seconds_since_worker_sub_p_started = curr_time - worker_sub_p_start_time
        print(f'SubProcessID: {worker_sub_pid}|Index: {worker_sub_p_index} is alive for {seconds_since_worker_sub_p_started}')

        if seconds_since_worker_sub_p_started > max_process_duration_seconds:
            print(f'Worker SubProcess is alive for {seconds_since_worker_sub_p_started}.')
            print(f'Its Lifetime has exceeded {max_process_duration_seconds}')
            print(f'It has index: {worker_sub_p_index} (Must have been 3 / 6 / 9)')
            expected_indexes = {3, 6, 9}

            if worker_sub_p_index in expected_indexes:
                os.kill(worker_sub_pid, signal.SIGTERM)
                print(f'\nWorker SubProcess: {worker_sub_pid} killed @ {curr_time}.\nThank you friend for your service _||_')
            else:
                print(f'Worker SubProcess exceeded max allowed lifetime, but does not have expected index {expected_indexes}')

        count += 1
        print(f'process_monitor has run {count} times')
        time.sleep(5)


def start_worker_child_process(msg: dict, p_q: multiprocessing.Queue, p_index: int, wait_seconds: int):

    print(f'In spawn_handler_process()')
    msg_data = msg['data']
    p = multiprocessing.Process(target=handle_msg, args=(msg_data, p_q, p_index, wait_seconds))
    p.start()
    p.join(0.01)


def main():

    print('In main() ...')
    unix_time = int(time.time())

    p_q = multiprocessing.Queue()
    print('\nmultiprocessing.Queue object created as:')
    print(p_q)

    monitoring_process = multiprocessing.Process(target=start_monitoring_process, args=(p_q,))
    print(f'Monitoring SubProcess created at time: {unix_time}')

    consumer_process = multiprocessing.Process(target=start_worker_process, args=(p_q,))
    print(f'Consumer SubProcess created at time: {unix_time}')

    monitoring_process.start()
    print('Monitoring SubProcess started')
    time.sleep(1)

    consumer_process.start()
    print('Consumer SubProcess started')
    monitoring_process.join(0.001)
    consumer_process.join(0.001)


if __name__ == '__main__':
    print('Program started')
    main()
    print('Main process execution Reached the end of program. (The sub processes must continue to run...)')
