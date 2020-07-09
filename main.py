import os
import time
import redis
import multiprocessing
from pprint import pformat

R = redis.StrictRedis(decode_responses=True, db=1)


def handle_msg(msg: str):

    unix_time = int(time.time())
    pid = os.getpid()
    text = f'PID: {pid} -> Message received: {msg} at time: {unix_time}'
    response = {'text': text, 'unix_time': unix_time, 'pid': pid}
    # print(text)
    R.hset('ProcessKey', pid, response)
    time.sleep(10)


def spawn_handler_process(msg: dict):

    print('In spawn_handler_process() ...')
    msg_data = msg['data']
    p = multiprocessing.Process(target=handle_msg, args=(msg_data,))
    p.start()
    p.join(0.01)


# Sub Process Task 1
def start_consumer_loop(unix_time: int):
    print(f'Received command to Start Task 1 at time: {unix_time}')
    print('Task 1 is: Message Consumer Loop')

    no_of_times_to_run = 10
    for i in range(no_of_times_to_run):
        msg_data = f'Message:{i+1}'
        msg = {'data': msg_data}
        spawn_handler_process(msg)
        time.sleep(2)


# Sub Process TasK 2
def start_process_monitor_loop(unix_time: int):
    print(f'Received command to Start Task 2: start_process_monitor_loop at time: {unix_time}')
    print(f'Parent PID: {os.getppid()} | Current PID: {os.getpid()}')
    count = 0
    while True:
        time.sleep(5)
        print(pformat(R.hgetall('ProcessKey')))
        count += 1
        print(f'process_monitor has printed {count} times')


def main():

    unix_time = int(time.time())

    monitoring_process = multiprocessing.Process(target=start_process_monitor_loop, args=(unix_time,))
    consumer_process = multiprocessing.Process(target=start_consumer_loop, args=(unix_time,))

    monitoring_process.start()
    consumer_process.start()
    monitoring_process.join(0.001)
    consumer_process.join(0.001)


if __name__ == '__main__':
    print('Program started')
    main()
    print('Main process execution Reached the end of program. (The sub processes must continue to run...)')
