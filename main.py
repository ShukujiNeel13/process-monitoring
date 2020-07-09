import os
import time
import redis
import multiprocessing
from pprint import pformat

PROCESS_MAP = {}

R = redis.StrictRedis(decode_responses=True)

CHANNEL = 'shukuji'

FREQUENCY_SECONDS_CHECK_PROCESS_MAP = 5

PROCESS_MONITOR_LAST_CHECK_TIME = None

# SWITCH = 1 stands for multiprocess.Process
# SWITCH = 2 stands for multiprocess.Pool
SWITCH = 1


def subscribe_to_channel(channel: str):

    # manager = multiprocessing.Manager()
    # return_dict = manager.dict()

    ps = R.pubsub()
    ps.subscribe(**{channel: spawn_handler})
    print(f'Subscribed to channel: {channel}.')
    thread = ps.run_in_thread(sleep_time=0.001)
    print('Subscriber thread is running now...')

    # subscribe_confirmation_msg_obj = ps.get_message()
    # print(subscribe_confirmation_msg_obj)


def spawn_handler(msg: dict):

    manager = multiprocessing.Manager()
    return_dict = manager.dict()

    print('In spawn_handler:')
    msg_data = msg['data']
    p = multiprocessing.Process(target=handle_msg, args=(msg_data, return_dict))
    p.start()
    p.join()

    print('Printing the Return dict:')
    print(pformat(return_dict.items()))


def spawn_handler_process(msg: dict):

    print('In spawn_handler_process() ...')

    msg_data = msg['data']

    p = multiprocessing.Process(target=handle_msg, args=(msg_data, ))

    unix_time = int(time.time())

    global PROCESS_MAP
    p.start()
    process_id = p.pid
    # alive_status = p.is_alive()
    PROCESS_MAP[process_id] = unix_time
    print(f'Created new {process_id} process at time: {unix_time}')
    # print(f'is_alive() for this process {process_id} is: {alive_status}')
    p.join(0.01)


# def spawn_sub_process_monitor():
#
#     while True:
#         if PROCESS_MAP is None:
#             continue
#
#         for process_id, unix_time in PROCESS_MAP.items():
#             process_id
#
#         time.sleep(20)


def handle_msg(msg: str, return_dict):

    unix_time = int(time.time())
    pid = os.getpid()
    text = f'PID: {pid} -> Message received: {msg} at time: {unix_time}'
    response = {'text': text, 'unix_time': unix_time, 'pid': pid}
    print(text)
    return_dict[pid] = response
    time.sleep(10)


def print_process_map(unix_time: int):

    # global PROCESS_MONITOR_LAST_CHECK_TIME
    # seconds_since_last_check = unix_time - PROCESS_MONITOR_LAST_CHECK_TIME

    # if seconds_since_last_check < FREQUENCY_SECONDS_CHECK_PROCESS_MAP:
    #     print('Process map not required to be checked')
    #     return

    print(f'\nChecking the Process Map at time: {unix_time}')
    global PROCESS_MAP
    if not PROCESS_MAP:
        print('PROCESS_MAP is empty')
        return
    print('PROCESS_MAP is:')
    print(pformat(PROCESS_MAP))

    # PROCESS_MONITOR_LAST_CHECK_TIME = unix_time


# Sub Process Task 1
def start_message_consumer_loop(unix_time: int):
    print(f'Received command to Start Task 1: start_message_consumer_loop at Unix Time: {unix_time}')
    subscribe_to_channel(CHANNEL)


# Sub Process TasK 2
def start_process_monitor_loop(unix_time: int):
    print('Received command to Start Task 2: start_process_monitor_loop')
    print(f'Parent PID: {os.getppid()} | Current PID: {os.getpid()}')
    count = 0
    while True:
        time.sleep(5)
        print_process_map(unix_time)
        count += 1
        print(f'print_process_map has executed {count} times')


def main():

    unix_time = int(time.time())
    # monitoring_process = multiprocessing.Process(target=start_process_monitor_loop, args=(unix_time, shared_queue))
    consumer_process = multiprocessing.Process(target=start_message_consumer_loop, args=(unix_time,))

    # monitoring_process.start()
    consumer_process.start()
    # monitoring_process.join(0.001)
    consumer_process.join(0.001)


if __name__ == '__main__':
    # p = multiprocessing.Process(target=spawn_sub_process_monitor, args=())
    # queue = multiprocessing.Queue()
    # main(queue)
    main()

    print('\nReturned from the main process.')
