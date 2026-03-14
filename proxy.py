# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project

import os
import socket
import threading
import time
import uuid
from typing import Any

import aiohttp
import msgpack
import zmq
from quart import Quart, make_response, request
import json

from dataclasses import dataclass
from typing import Optional


@dataclass
class InstanceInfo:
    http_address: str
    zmq_address: str
    stamp: float
    pd_type: str  # "p_heavy" or "d_heavy"
    num_tokens: int


@dataclass
class RequestState:
    request_id: str
    next_instance: Optional[InstanceInfo] = None
    num_left_tokens: int


count = 0
prefill_instances: dict[str, InstanceInfo] = {}  # http_address: InstanceInfo
decode_instances: dict[str, InstanceInfo] = {}  # http_address: InstanceInfo

prefill_cv = threading.Condition()
decode_cv = threading.Condition()

inflight_requests: dict[str, RequestState] = {}  # request_id: RequestState

DEFAULT_PING_SECONDS = 5


def _remove_oldest_instances(instances: dict[str, Any]) -> None:
    oldest_key = next(iter(instances), None)
    while oldest_key is not None:
        value = instances[oldest_key]
        if value.stamp > time.time():
            break
        print(f"🔴Remove [HTTP:{oldest_key}, ZMQ:{value.zmq_address}, stamp:{value.stamp}]")
        instances.pop(oldest_key, None)
        oldest_key = next(iter(instances), None)

def _get_min_instance(instances: dict[str, InstanceInfo]) -> InstanceInfo:
    min_instance = None
    for instance in instances.values():
        if min_instance is None or instance.num_tokens < min_instance.num_tokens:
            min_instance = instance
    return min_instance

def _get_remote_address(data):
    if data["src_type"] == "p_heavy":
        return _get_min_instance(decode_instances).zmq_address
    elif data["src_type"] == "d_heavy":
        return _get_min_instance(prefill_instances).zmq_address

def _listen_for_register(poller, router_socket):
    while True:
        socks = dict(poller.poll())
        if router_socket in socks:
            remote_address, message = router_socket.recv_multipart()
            # data: {"type": "P", "http_address": "ip:port",
            #        "zmq_address": "ip:port"}
            data = msgpack.loads(message)
            if data["cmd"] == "ping":
                if data["type"] == "p_heavy":
                    global prefill_instances
                    global prefill_cv
                    with prefill_cv:
                        node = prefill_instances.get(data["http_address"], None)
                        prefill_instances[data["http_address"]] = InstanceInfo(
                            zmq_address=data["zmq_address"],
                            stamp=time.time() + DEFAULT_PING_SECONDS,
                            pd_type="p_heavy",
                            num_tokens=0,
                        )
                        _remove_oldest_instances(prefill_instances)

                elif data["type"] == "d_heavy":
                    global decode_instances
                    global decode_cv
                    with decode_cv:
                        node = decode_instances.get(data["http_address"], None)
                        decode_instances[data["http_address"]] = InstanceInfo(
                            zmq_address=data["zmq_address"],
                            stamp=time.time() + DEFAULT_PING_SECONDS,
                            pd_type="d_heavy",
                            num_tokens=0,
                        )
                        _remove_oldest_instances(decode_instances)
                else:
                    print(
                        "Unexpected, Received message from %s, data: %s",
                        remote_address,
                        data,
                    )
                    return

                if node is None:
                    print(f"🔵Add [HTTP:{data['http_address']}, ZMQ:{data['zmq_address']}]")
            elif data["cmd"] == "GET_REMOTE_ADDRESS":
                response = {
                    "remote_address": _get_remote_address(data),
                }
                print(f"Get remote address for {data['src_type']}: {response['remote_address']}")
                inflight_requests[data["request_id"]].next_instance = InstanceInfo(
                    zmq_address=response["remote_address"],
                )
                router_socket.send_multipart([remote_address, msgpack.dumps(response)])
            else:
                print(
                    "Unexpected, Received message from %s, data: %s",
                    remote_address,
                    data,
                )
                return


def start_service_discovery(hostname, port):
    if not hostname:
        hostname = socket.gethostname()
    if port == 0:
        raise ValueError("Port cannot be 0")

    context = zmq.Context()
    router_socket = context.socket(zmq.ROUTER)
    router_socket.bind(f"tcp://{hostname}:{port}")

    poller = zmq.Poller()
    poller.register(router_socket, zmq.POLLIN)

    _listener_thread = threading.Thread(
        target=_listen_for_register, args=[poller, router_socket], daemon=True
    )
    _listener_thread.start()
    return _listener_thread


AIOHTTP_TIMEOUT = aiohttp.ClientTimeout(total=6 * 60 * 60)

app = Quart(__name__)


def random_uuid() -> str:
    return str(uuid.uuid4().hex)


async def forward_request(url, data, request_id, prefix=None):
    async with aiohttp.ClientSession(timeout=AIOHTTP_TIMEOUT) as session:
        headers = {
            "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}",
            "X-Request-Id": request_id,
        }
        async with session.post(url=url, json=data, headers=headers) as response:
            if response.status == 200:
                if True:
                    async for chunk_bytes in response.content:

                        # 修改字段
                        chunk_str = chunk_bytes.decode('utf-8').strip()
                        if chunk_str.startswith('data: '):
                            data_str = chunk_str[6:]
                            if data_str.strip() != '[DONE]':
                                data = json.loads(data_str)
                                new_chunk_str = "data: " + json.dumps(data, ensure_ascii=False)
                                chunk_bytes = (new_chunk_str + "\n").encode("utf-8")

                        yield chunk_bytes
                else:
                    content = await response.read()
                    yield content

def algo2():
    pass

@app.route("/v1/completions", methods=["POST"])
@app.route("/v1/chat/completions", methods=["POST"])
async def handle_request():
    try:
        original_request_data = await request.get_json()

        prefill_request = original_request_data.copy()
        # change max_tokens = 1 to let it only do prefill
        prefill_request["max_tokens"] = 1
        if "max_completion_tokens" in prefill_request:
            prefill_request["max_completion_tokens"] = 1

        # global count
        # global prefill_instances
        # global prefill_cv
        # with prefill_cv:
        #     prefill_list = list(prefill_instances.items())
        #     prefill_addr, prefill_zmq_addr = prefill_list[count % len(prefill_list)]
        #     prefill_zmq_addr = prefill_zmq_addr[0]

        # global decode_instances
        # global decode_cv
        # with decode_cv:
        #     decode_list = list(decode_instances.items())
        #     decode_addr, decode_zmq_addr = decode_list[count % len(decode_list)]
        #     decode_zmq_addr = decode_zmq_addr[0]

        prefill_instance: InstanceInfo = algo2()
        decode_instance = _get_min_instance(decode_instances)


        # print(
        #     f"handle_request count: {count}, [HTTP:{prefill_addr}, "
        #     f"ZMQ:{prefill_zmq_addr}] 👉 [HTTP:{decode_addr}, "
        #     f"ZMQ:{decode_zmq_addr}]"
        # )
        count += 1

        request_id = (
            f"___prefill_addr_{prefill_instance.zmq_address}___decode_addr_"
            f"{decode_instance.zmq_address}_{random_uuid()}"
        )
        

        # finish prefill
        async for _ in forward_request(
            f"http://{prefill_instance.http_address}{request.path}", prefill_request, request_id, "prefill"
        ):
            continue
        # return decode

        max_tokens = original_request_data.get("max_tokens", None)
        assert max_tokens is not None
        num_decode = 0
        inflight_requests[request_id] = RequestState(
            request_id=request_id,
            next_instance=decode_instance,
            num_left_tokens=max_tokens,
        )
        

        while request_id in inflight_requests:
            max_tokens = max_tokens - num_decode
            num_decode = 0
            original_request_data["max_tokens"] = max_tokens
            if max_tokens <= 0:
                break
            async for chunk in forward_request(
                f"http://{inflight_requests[request_id].next_instance.http_address}{request.path}", original_request_data, request_id, "decode"
            ):
                yield chunk
                num_decode += 1
            
        return

    except Exception as e:
        import sys
        import traceback

        exc_info = sys.exc_info()
        print("Error occurred in disagg prefill proxy server")
        print(e)
        print("".join(traceback.format_exception(*exc_info)))


if __name__ == "__main__":
    t = start_service_discovery("0.0.0.0", 30001)
    app.run(host="0.0.0.0", port=8000)
    t.join()
