import json
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

CONNECTOR_URL = "http://connect:8083/connectors/kilatmart-orders"


def request(suffix, data=None):
    req = Request(
        CONNECTOR_URL + suffix,
        data=data,
        headers={"Content-Type": "application/json"},
        method="PUT" if data is not None else "GET",
    )
    with urlopen(req, timeout=5) as response:
        return json.load(response)


def retry(operation, ready, label):
    deadline = time.monotonic() + 120
    last_result = None
    while time.monotonic() < deadline:
        try:
            last_result = operation()
            if ready(last_result):
                return
        except HTTPError as exc:
            body = exc.read().decode(errors="replace")
            if exc.code == 400:
                raise RuntimeError(body) from exc
            last_result = f"HTTP {exc.code}: {body}"
        except (URLError, TimeoutError) as exc:
            last_result = str(exc)
        time.sleep(2)
    raise RuntimeError(f"{label} did not become ready: {last_result}")


def running(status):
    tasks = status.get("tasks", [])
    for component in [status["connector"], *tasks]:
        if component["state"] == "FAILED":
            raise RuntimeError(component.get("trace", "Connector task failed"))
    return bool(tasks) and all(
        item["state"] == "RUNNING" for item in [status["connector"], *tasks]
    )


if __name__ == "__main__":
    config = Path(__file__).with_name("orders.json").read_bytes()
    retry(lambda: request("/config", config), bool, "Connector registration")
    retry(lambda: request("/status"), running, "Connector task")
    print("Connector and task are RUNNING", flush=True)

