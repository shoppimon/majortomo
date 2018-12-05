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
