import asyncio
import json
import time
from asyncua import Client
from asyncio_mqtt import Client as MqttClient, MqttError

# settings
MOCK_CONFIG_PATH = "config.json"
OPCUA_HOST = "127.0.0.1"
MQTT_HOST = "127.0.0.1"
MQTT_PORT = "1883"
MQTT_BASE_TOPIC = "opcua" # prefix for messages comming from OPC UA
MQTT_QOS = 1 # messages will arrive at least once
MQTT_RETAIN = 1 # retain last value on broker

# load targets/variables we expect from opc ua (from config.json)
def load_targets(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        json_config = json.load(f)

    namespaces = json_config["namespaces"]
    targets = []
    for server in json_config["servers"]:
        port = int(server["port"])
        path = server["path"]
        for obj in server.get("objects", []):
            obj_name = obj["name"]
            for var in obj.get("variables", []):
                ns_idx = int(var["ns"])
                targets.append({
                    "port": port,
                    "path": path,
                    "namespace_uri": namespaces[ns_idx],
                    "obj": obj_name,
                    "var": var["name"],
                })
    return targets

async def mqtt_publisher(queue):
    while True:
        try:
            async with MqttClient(MQTT_HOST, MQTT_PORT) as mqtt:
                while True:
                    topic_suffix, payload = await queue.get()
                    topic = f"{MQTT_BASE_TOPIC}/{topic_suffix}"
                    msg = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                    await mqtt.publish(topic, msg, qos=MQTT_QOS, retain=MQTT_RETAIN)
                    queue.task_done()
        except MqttError:
            await asyncio.sleep(1)


class Handler:
    def __init__(self, endpoint, nodeid_to_target, out_q):
        self.endpoint = endpoint
        self.nodeid_to_target = nodeid_to_target
        self.out_q = out_q

    # identify the change, wrap it and push to queue if possible
    def datachange_notification(self, node, val):
        t = self.nodeid_to_target.get(str(node.nodeid))
        if not t:
            return

        payload = {
            "timestamp_ms": int(time.time() * 1000),
            "source": {
                "endpoint": self.endpoint,
                "browse_path": t.browse_path(),
                "node_id": str(node.nodeid),
            },
            "value": val,
        }

        try:
            self.out_q.put_nowait((t.topic_suffix(), payload))
        except asyncio.QueueFull:
            pass

# watch a single opc ua server for changes
async def watch_server(port, path, targets, out_q):
    endpoint = f"opc.tcp://{OPCUA_HOST}:{port}{path}"
    while True:
        try:
            async with Client(url=endpoint) as client:
                resolved = []

                for target in targets:
                    ns = await client.get_namespace_index(target.namespace_uri)
                    node = await client.nodes.root.get_child([
                        "0:Objects",
                        f"{ns}:{target.obj}",
                        f"{ns}:{target.var}",
                    ])
                    resolved.append((target, node))

                nodeid_to_target = {str(node.nodeid): target for target, node in resolved}
                handler = Handler(endpoint, nodeid_to_target, out_q)
                sub = await client.create_subscription(500, handler) # 500ms publishing interval
                for _, node in resolved:
                    await sub.subscribe_data_change(node)
                while True:
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(1)

async def main():
    targets = load_targets(MOCK_CONFIG_PATH)
    by_server = {}
    for t in targets:
        by_server.setdefault((t.port, t.path), []).append(t)

    out_q = asyncio.Queue(maxsize=10000)
    tasks = [asyncio.create_task(mqtt_publisher(out_q))]
    for (port, path), ts in by_server.items():
        tasks.append(asyncio.create_task(watch_server(port, path, ts, out_q)))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
