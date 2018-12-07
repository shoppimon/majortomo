Majortomo Documentation
=======================
*Majortomo* is a pure-Python `ZeroMQ MDP 0.2 <https://rfc.zeromq.org/spec:18/MDP/>`_
("Majordomo") implementation. It provides a ready-to-use MDP Service Broker,
as well as a Python 2.7 / 3.5+ library for implementing MDP clients and workers
with just a few lines of code.

MDP / Majordomo is a protocol for implementing a highly scalable, lightweight
service oriented messaging on top of `ØMQ <https://zeromq.org>`_. It is very
useful, for example, for facilitating communication between different
micro-services in a scalable, robust and fault-tolerant manner.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   client
   worker
   broker


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
