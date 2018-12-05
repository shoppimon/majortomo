from majortomo import Worker


def test_zmq_mdp_worker_instantiates():
    worker = Worker(broker_url='tcp://localhost:5555', service_name='test-service')
    assert worker
