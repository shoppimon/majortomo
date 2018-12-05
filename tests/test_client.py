from majortomo import Client


def test_client_is_connected():
    client = Client(broker_url='tcp://localhost:5555')
    assert client.is_connected() is False
    client.connect()
    assert client.is_connected() is True
    client.close()
    assert client.is_connected() is False


def test_client_connection_is_context_managed():
    client = Client(broker_url='tcp://localhost:5555')
    assert client.is_connected() is False
    with client:
        assert client.is_connected() is True
    assert client.is_connected() is False
