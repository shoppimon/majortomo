"""An 'echo' service, here for testing purposes and as a sample client / worker implementation
"""

# Copyright (c) 2018 Shoppimon LTD
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import logging
import signal
import time
from typing import Iterable, List  # noqa: F401

from . import Client, Worker, WorkerRequestsIterator, protocol

SHELL_COLOR_RED = 31
SHELL_COLOR_GREEN = 32


def _run_worker(broker_url, service_name='mdp-echo', hb_interval=protocol.DEFAULT_HEARTBEAT_INTERVAL, delay=None,
                return_garbage=False):
    """Run worker code
    """
    worker = Worker(broker_url=broker_url, service_name=service_name, heartbeat_interval=hb_interval)

    def sig_handler(*_):
        worker.close()

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    worker_iter = WorkerRequestsIterator(worker)
    for request in worker_iter:
        reply = _handle_worker_request(request, delay, return_garbage)
        worker_iter.send_reply_final(reply)


def _run_client(broker_url, service_name='mdp-echo', message='echo', repeat=None, delay=None, timeout=None):
    """Run client code
    """
    if repeat is None:
        repeat = 1

    message = message.encode('utf8')

    with Client(broker_url) as client:
        for _ in range(repeat):
            client.send(service_name.encode('ascii'), message)
            response = client.recv_all_as_list(timeout=timeout)

            if response == [message]:
                color = SHELL_COLOR_GREEN
            else:
                color = SHELL_COLOR_RED
            print(_colorize("Got response: {}".format(response), color))

            if delay is not None:
                time.sleep(delay)


def _handle_worker_request(request, delay, return_garbage=False):
    # type: (List[bytes], float, bool) -> Iterable[bytes]
    if delay is not None:
        time.sleep(delay)
    if return_garbage:
        reply = reversed([bytes(reversed(f)) for f in request])  # type: Iterable[bytes]
    else:
        reply = request
    return reply


def _colorize(text, color_code):
    # type: (str, int) -> str
    """Return shell-escaped colored text string
    """
    return "\x1B[38;{}m{}\x1B[0m".format(color_code, text)


def _parse_args():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-b', '--broker_url', required=False, help='Broker URL', default='tcp://127.0.0.1:5555')
    argparser.add_argument('-n', '--service-name', required=False, help='Service name', default='mdp-echo')
    argparser.add_argument('--debug', action='store_true', help='Enable debug logging')
    subparsers = argparser.add_subparsers(dest='command')
    subparsers.required = True

    worker = subparsers.add_parser('worker', help='Run in Worker mode')
    worker.add_argument('-d', '--delay', help='Delay in sec before replying', type=float, required=False)
    worker.add_argument('-i', '--heartbeat-interval', help='Heartbeat interval in seconds', type=float, default=2.5)
    worker.add_argument('-f', '--garbage', help='Reply with unexpected garbage', action='store_true')

    client = subparsers.add_parser('client', help='Run in Client mode')
    client.add_argument('-r', '--repeat', help='Number of times to repeat message', type=int, required=False)
    client.add_argument('-d', '--delay', help='Delay in sec between messages', type=float, default=0.5, required=False)
    client.add_argument('-m', '--message', help='Message text', required=True)
    client.add_argument('-t', '--timeout', help='Reply timeout', type=float, required=False)

    args = argparser.parse_args()
    return args


def main():
    args = _parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format='%(asctime)-15s %(name)-15s %(levelname)s %(message)s')

    if args.command == 'worker':
        _run_worker(broker_url=args.broker_url, service_name=args.service_name, hb_interval=args.heartbeat_interval,
                    delay=args.delay, return_garbage=args.garbage)
    elif args.command == 'client':
        _run_client(broker_url=args.broker_url, service_name=args.service_name, message=args.message,
                    repeat=args.repeat, delay=args.delay, timeout=args.timeout)
    else:
        raise ValueError("Unexpected command: {}".format(args.command))


if __name__ == '__main__':
    main()
