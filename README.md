Majortomo
=========
*Majortomo* is a pure-Python [ZeroMQ MDP 0.2](https://rfc.zeromq.org/spec:18/MDP/)
("Majordomo") implementation. It provides a ready-to-use MDP Service Broker,
as well as a Python 2.7 / 3.5+ library for implementing MDP clients and workers
with just a few lines of code. 

MDP / Majordomo is a protocol for implementing a highly scalable, lightweight 
service oriented messaging on top of [ZeroMQ](https://zeromq.org). It is very 
useful, for example, for facilitating communication between different
micro-services in a scalable, robust and fault-tolerant manner. 

[![Build Status](https://travis-ci.org/shoppimon/majortomo.svg?branch=master)](https://travis-ci.org/shoppimon/majortomo)
[![Documentation Status](https://readthedocs.org/projects/majortomo/badge/?version=latest)](https://majortomo.readthedocs.io/en/latest/?badge=latest)

Installation
------------
The simplest way to install Majortomo is via `pip`:

    $ pip install majortomo

If you just want to run the MDP broker, for example if you already have MDP 
workers and clients implemented in some other language / library, you can run
simply run the Docker image without installing any Python packages:

    # This doesn't actually work yet, but will at some point...
    # $ docker run shoppimon/majortomo-broker:latest

Quick Start
-----------
### Running the Broker

### Exposing a Service using an MDP Worker

### Consuming a Service using an MDP Client

Full Documentation
------------------
Project documentation is available here: https://majortomo.readthedocs.io/en/latest/

Usage
-----
### Running the Broker
In most cases the MDP broker can be used as-is, simply by running it with the 
right command line arguments. You can do this by building and running it using 
Docker:

    # Build the Docker image
    $ docker build -t shoppimon/mdp-broker -f mdp-broker/Dockerfile .
    
    # Run the broker from Docker
    $ docker run --rm -ti shoppimon/mdp-broker -b tcp://0.0.0.0:5555 --verbose
    
You can run the broker with `--help` for more command line options. 

Of course, you can also run the broker directly using Python 3.5 and up:

    $ python -m majortomo.broker --help

Note that this requires setting up a virtual environemnt with the project 
dependencies, which is described below. 

### Installing & Using the Client and Worker modules
TBD

### Using the Client class
See `majortomo.echo` for a sample client implementation.

The `Client` class should normally be used directly (without subclassing) to
send requests to the broker (and workers). 

##### Opening and closing client connections
While a lower-level API is available (through the `connect`, `is_connected`, 
and `close`), managing the connection to the broker is easiest done through
the context manager protocol:

```python
with Client(broker_url='tcp://127.0.0.1:5555') as client:
    client.send(b'my-service', b'frame1', b'frame2')
    reply = client.recv_all_as_list(timeout=10.0)
```

The example above takes care of opening and closing the ZeroMQ socket as 
needed.

**NOTE**: ZeroMQ takes care of re-creating dropped TCP connections and waiting
for not-yet-bound peers automatically. 

##### Sending Requests & Receiving Replies
To send a request, use the `send` method:

```python
client.send(service_name, frame1, frame2, frame3)
```

This method takes the service name (as `bytes`) as a first argument. 
All other arguments are sent as message frames - the MDP protocol supports
sending requests with more than one frame to the broker. The contents of these
frames is application dependent and is up to you.

Once a request has been sent, you *must* read the entire reply send back from
the broker (or close the connection to the broker and reconnect if you wish
to retry). 

There are multiple methods for reading replies, depending on your needs: 

**`recv_part(timeout: float=None) -> Optional[List[bytes]]`**

Receive one reply part from the broker (a reply part is a list of bytes, as it
may contain multiple ZeroMQ frames). 

If no more parts are available (i.e. the last part was a `FINAL` reply), will
return `None`.

**`recv_all(timeout: float=None) -> Iterable[List[bytes]]`**

Returns an iterator that yields each message part as it is received, and exists
after the `FINAL` reply has been received:

```python
for part in client.recv_all(timeout=5.0): 
    do_something_with_reply_part(part)
```

**Note**: the `timeout` parameter in this case relates to the time between reply
chunks, not to the time it would take to receive the entire reply until `FINAL`. 

**`recv_all_as_list(timeout: float=None) -> List[bytes]`**

Returns a flat list of all message frames from all reply parts. Regardless of 
how many `PARTIAL` replies were sent by the worker until the `FINAL` reply, 
this method will always return a single one-dimentional list of `bytes` with
message frames.

##### Timeouts & Retrying
All `recv_*` methods of the client receive a `timeout` argument which should
specify the number of seconds to wait for a response (a `float` is expected 
so you can specify second fractions as well). If no `timeout` is specified, 
the function will wait forever. 

Once `recv_*` times out, a `majortomo.error.Timeout` will
be raised. It is sometimes useful to catch this exception and retry the 
operation after reconnecting to the broker:

```python
while True:
    with Client(broker_url='tcp://127.0.0.1:5555') as client:
        try:
        
            client.send(b'my-service', b'frame1', b'frame2')
            reply = client.recv_all_as_list(timeout=10.0)
            break
        except majortomo.error.Timeout:
            logging.warning("Timed out waiting for a reply from broker, reconnecting")
            time.sleep(1.0)
            continue
```

Or, if you do not wish to rely on the context manager for reconnecting (e.g. if
the context is managed in an outer scope):

```python
# Here `client` is passed from an outer scope
while True:
    try:
        client.send(b'my-service', b'frame1', b'frame2')
        reply = client.recv_all_as_list(timeout=10.0)
        break
    except majortomo.error.Timeout:
        logging.warning("Timed out waiting for a reply from broker, reconnecting")
        time.sleep(1.0)
        client.connect(reconnect=True)
        continue
```

Even better, it is advisable to manage the number of retries and the `sleep` time
between them using some kind of exponential backoff & retry library, for example
[backoff](https://pypi.org/project/backoff/) or [redo](https://pypi.org/project/redo)

#### Implementing MDP Workers
See `majortomo.echo` for a sample worker implementation

More details TBD

Copyright & Credits
-------------------
Majortomo was created and is maintained by the [Shoppimon](https://www.shoppimon.com) 
team, and is distributed under the terms of the Apache 2.0 License 
(see `LICENSE`).

Majortomo is (C) Copyright 2018 Shoppimon LTD. 

ØMQ is Copyright (c) 2007-2014 iMatix Corporation and Contributors.
