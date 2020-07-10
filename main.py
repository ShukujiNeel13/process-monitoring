import os
import time
import redis
import multiprocessing
from ast import literal_eval

# R = redis.StrictRedis(decode_responses=True, db=1)
# R.flushdb()


def handle_msg(msg: str, p_q: multiprocessing.Queue):

    print('In handle_msg. Message received is:')
    print(msg)

    current_pid = os.getpid()
    # p_obj = multiprocessing.current_process()
    # p_name = p_obj.name
    # print('Current Process Object is:')
    # print(p_obj)
    # print('Current process name is:')
    # print(p_name)
    # os_pid = os.getpid()
    # p_obj_pid = p_obj.pid
    # print(f'PID is -> {p_obj_pid}')
    p_q.put(current_pid)

    # text = f'PID: {pid} -> Message received: {msg} at time: {unix_time}'
    # print(text)
    # p_q.put(pid)
    # response = {'text': text, 'unix_time': unix_time, 'pid': pid}
    # print(text)
    # R.hset('ProcessKey', pid, response)
    time.sleep(5)


def spawn_handler_process(msg: dict, p_q: multiprocessing.Queue):

    print(f'In spawn_handler_process()')
    msg_data = msg['data']
    p = multiprocessing.Process(target=handle_msg, args=(msg_data, p_q))
    p.start()
    p.join(0.01)


# Sub Process Task 1
def start_consumer_process(p_q: multiprocessing.Queue):
    current_pid = os.getpid()
    print(f'In start_consumer_process() | Current PID: {current_pid}')
    # p_q.put(current_pid)
    # print('Added this ProcessID in p_q')

    no_of_times_to_run = 10
    for i in range(no_of_times_to_run):
        unix_time = int(time.time())
        msg_data = f'Message:{i+1} | Time: {unix_time}'
        msg = {'data': msg_data}
        spawn_handler_process(msg, p_q)
        time.sleep(1)


# Sub Process Task 2
def start_monitoring_process(p_q: multiprocessing.Queue):
    current_pid = os.getpid()
    print(f'In start_monitoring_process() | Current PID: {current_pid}')
    # p_q.put(current_pid)
    # print('Added this ProcessID in p_q')

    count = 0
    while True:
        items_in_queue = p_q.get()
        print(f'Item in queue is:')
        print(items_in_queue)
        print('Type of item in queue is:')
        print(type(items_in_queue))

        count += 1
        print(f'process_monitor has printed {count} times')
        time.sleep(5)


def main():

    print('In main() ...')
    unix_time = int(time.time())
    p_q = multiprocessing.Queue()
    print('multiprocessing.Queue object created as:')
    print(p_q)

    monitoring_process = multiprocessing.Process(target=start_monitoring_process, args=(p_q,))
    print(f'Monitoring SubProcess created at time: {unix_time}')
    consumer_process = multiprocessing.Process(target=start_consumer_process, args=(p_q,))
    print(f'Consumer SubProcess created at time: {unix_time}')

    monitoring_process.start()
    print('Monitoring SubProcess started')
    consumer_process.start()
    print('Consumer SubProcess started')
    monitoring_process.join(0.001)
    consumer_process.join(0.001)


if __name__ == '__main__':
    print('Program started')
    main()
    print('Main process execution Reached the end of program. (The sub processes must continue to run...)')
