MDP Broker
==========
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

