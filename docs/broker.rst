MDP Broker
==========
In most cases the MDP broker can be used as-is, simply by running it with the right command line arguments.

Running Using a Docker Image
----------------------------
If you have a `Docker <https://www.docker.com/>`_ environment set up, it might be easiest to run the Broker using
Docker::

    # Build the Docker image
    $ docker build -t shoppimon/mdp-broker -f mdp-broker/Dockerfile .

    # Run the broker from Docker
    $ docker run --rm -ti shoppimon/mdp-broker -b tcp://0.0.0.0:5555 --verbose

Running in a Virtual Environment
--------------------------------
If you prefer, you can run the Docker in a virtual environment using Python 2.7 or 3.5 and up::

    # Set up a virtual environemnt (Python 3.x):
    $ python3 -m venv .venv

    # Set up a virtual environment (Python 2.7):
    $ virtualenv .venv

    # Activate the virtual environment and install runtime requirements:
    $ . .venv/bin/activate
    $ pip install -r requirements.txt -e .

    # Run the broker:
    $ python -m majortomo.broker --help


Command Line Arguments
----------------------
You can run the broker with ``--help`` for an up-to-date list of all command line arguments.

Logging Configuration
---------------------
Beyond the ``--verbose`` and ``--debug`` command line arguments, fine-grained control over logging is possible via a
YAML-based configuration file containing Python's :py:func:`logging.config.dictConfig`
(https://docs.python.org/3.7/library/logging.config.html#logging.config.dictConfig) configuration.

Broker API
----------
.. note::
   In most cases the broker should be run as-is as described above.
   Use the broker API directly if you need to somehow modify its behavior or wrap in it other code.

.. automodule:: majortomo.broker
   :members:
