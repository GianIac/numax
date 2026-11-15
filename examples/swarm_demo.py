#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import concurrent.futures
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import threading
import time
import tomllib
import sys
import urllib.error
import urllib.parse
import urllib.request


class SwarmError(RuntimeError):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


def validate_config(kind: str, config: dict) -> dict:
    defaults = dict(grid_width=12, grid_height=12, seed=1337, base_port=9000)
    if kind == "ants":
        defaults.update(hotspot_count=3, nest_x=0, nest_y=0, ticks_per_ant=40, ants=4)
        count_key, ticks_key = "ants", "ticks_per_ant"
    else:
        defaults.update(grid_width=16, grid_height=16, seed=7, base_port=9100,
                        anchor_count=3, settle_radius=1, ticks_per_particle=120,
                        particles=14, phase_count=1, polarity_enabled=True)
        count_key, ticks_key = "particles", "ticks_per_particle"
    values = defaults | config
    for key, default in defaults.items():
        if type(values[key]) is not type(default):
            raise SwarmError(f"invalid type for {key}")
    if not 1 <= values[count_key] <= 50 or not 1 <= values[ticks_key] <= 10000:
        raise SwarmError("invalid worker or tick count")
    if not all(2 <= values[key] <= 64 for key in ("grid_width", "grid_height")):
        raise SwarmError("grid dimensions must be between 2 and 64")
    if not 1 <= values["base_port"] <= 65535 - 2 * values[count_key] + 1:
        raise SwarmError("port range out of bounds")
    if not 0 <= values["seed"] < 2**64:
        raise SwarmError("seed out of bounds")
    if kind == "ants":
        if not 1 <= values["hotspot_count"] <= 64:
            raise SwarmError("invalid hotspot_count")
        if not 0 <= values["nest_x"] < values["grid_width"] or not 0 <= values["nest_y"] < values["grid_height"]:
            raise SwarmError("nest outside grid")
    else:
        if not 1 <= values["anchor_count"] <= 64 or not 0 <= values["settle_radius"] <= 64:
            raise SwarmError("invalid anchors or settle_radius")
        if not 1 <= values["phase_count"] <= 32:
            raise SwarmError("invalid phase_count")
        previous = 1
        for phase in range(2, values["phase_count"] + 1):
            tick = values.get(f"phase{phase}_at_tick")
            boosts = values.get(f"phase{phase}_boosts")
            if type(tick) is not int or not previous < tick <= values[ticks_key]:
                raise SwarmError(f"invalid phase {phase} tick")
            if not isinstance(boosts, str) or not boosts:
                raise SwarmError(f"missing phase {phase} boosts")
            for entry in boosts.split(","):
                try:
                    anchor, delta = map(int, entry.strip().split(":"))
                except ValueError as error:
                    raise SwarmError(f"invalid phase {phase} boost") from error
                if not 0 <= anchor < values["anchor_count"] or not -(2**63) < delta < 2**63 or delta == 0:
                    raise SwarmError(f"invalid phase {phase} boost")
            previous = tick
    values["workers"] = values[count_key]
    values["ticks"] = values[ticks_key]
    return values


def encoded_key(key: str) -> str:
    return base64.urlsafe_b64encode(key.encode()).decode().rstrip("=")


class Node:
    def __init__(self, index: int, directory: Path, network: int, management: int, nx: Path, peers: list[int]):
        self.index = index
        self.directory = directory
        self.directory.mkdir(mode=0o700)
        self.token = secrets.token_hex(32)
        token_path = directory / "management.token"
        descriptor = os.open(token_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            stream.write(self.token + "\n")
        self.network = network
        self.management = management
        self.nx = nx
        self.module = ""
        self.identity = ""
        self.controls: dict[bytes, str] = {}
        self.process: subprocess.Popen | None = None
        self.pids: list[int] = []
        config = f'''[network]
listen = "127.0.0.1:{network}"
peers = {json.dumps([f"127.0.0.1:{port}" for port in peers])}
[storage]
datastore_path = {json.dumps(str(directory / "data"))}
[management]
listen = "127.0.0.1:{management}"
token_file = {json.dumps(str(token_path))}
allow_non_loopback = false
'''
        (directory / "node.toml").write_text(config)

    def start(self) -> None:
        if self.process is not None:
            raise SwarmError("node already started")
        environment = {key: value for key, value in os.environ.items() if not key.startswith(("NX_", "NUMAX_"))}
        with (self.directory / "daemon.log").open("ab") as log:
            self.process = subprocess.Popen([str(self.nx), "serve", "--config", str(self.directory / "node.toml")],
                                            stdout=log, stderr=log, env=environment)
        self.pids.append(self.process.pid)

    def alive(self) -> None:
        if self.process is None or self.process.poll() is not None:
            raise SwarmError(f"node {self.index} exited; inspect its private daemon.log")

    def request(self, route: str, method: str = "GET", body: bytes | None = None,
                content_type: str = "application/octet-stream", allowed: tuple[int, ...] = (200,)) -> bytes:
        self.alive()
        request = urllib.request.Request(f"http://127.0.0.1:{self.management}/api/v1/{route}", data=body,
                                        method=method, headers={"Authorization": f"Bearer {self.token}", "Content-Type": content_type})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        try:
            with opener.open(request, timeout=5) as response:
                if response.status not in allowed:
                    raise SwarmError(f"node {self.index}: unexpected HTTP status")
                data = response.read(4 * 1024 * 1024 + 1)
                if len(data) > 4 * 1024 * 1024:
                    raise SwarmError("response budget exceeded")
                return data
        except urllib.error.HTTPError as error:
            raise SwarmError(f"node {self.index}: {method} HTTP {error.code}") from None
        except TimeoutError:
            raise SwarmError(f"node {self.index}: {method} {route} timed out") from None

    def view(self, name: str) -> list[dict]:
        items: list[dict] = []
        cursors: set[str] = set()
        cursor = None
        while True:
            query = dict(view=name, limit="100")
            if cursor is not None:
                query["cursor"] = cursor
            page = json.loads(self.request("peers?" + urllib.parse.urlencode(query)))
            if not isinstance(page.get("items"), list) or "next_cursor" not in page:
                raise SwarmError("invalid peer page")
            items.extend(page["items"])
            cursor = page["next_cursor"]
            if cursor is None:
                return items
            if not isinstance(cursor, str) or not cursor or cursor in cursors or len(cursors) >= 32:
                raise SwarmError("invalid peer cursor")
            cursors.add(cursor)

    def control(self, values: dict[str, str], clear_completed: bool = False) -> None:
        payload = {"demo:env/" + name: value for name, value in values.items()}
        if clear_completed:
            payload["demo:completed"] = ""
        generator = Path(__file__).resolve().parent / "swarm_control/target/debug/swarm_control"
        generated = subprocess.run([str(generator)], input=json.dumps(payload).encode(), capture_output=True, timeout=10)
        if generated.returncode != 0:
            raise SwarmError("control guest generation failed")
        wasm = generated.stdout
        module = self.controls.get(wasm)
        if module is None:
            if len(self.controls) >= 128:
                raise SwarmError("control module budget exceeded")
            module = json.loads(self.request("modules", "POST", wasm, "application/wasm", (200, 201)))["id"]
            self.controls[wasm] = module
        self.request(f"modules/{module}/runs", "POST", allowed=(204,))

    def run(self) -> None:
        self.request(f"modules/{self.module}/runs", "POST", allowed=(204,))

    def tick(self, tick: int, boost: str = "") -> None:
        self.control(dict(NUMAX_OBSERVE="0", NUMAX_RENDER="0", NUMAX_BOOST=boost, NUMAX_TICK="done"), True)
        self.run()
        if self.request("keys/" + encoded_key("demo:completed")) != b"done":
            raise SwarmError(f"node {self.index}: incomplete tick {tick}")

    def snapshot(self, render: bool = False, label: str = "snapshot") -> dict:
        self.control(dict(NUMAX_OBSERVE="1", NUMAX_RENDER="1" if render else "0", NUMAX_SNAPSHOT_LABEL=label))
        self.run()
        return json.loads(self.request("keys/" + encoded_key("demo:snapshot")))

    def stop(self) -> None:
        process = self.process
        if process is None:
            return
        if process.poll() is None:
            process.terminate()
        try:
            code = process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            self.process = None
            raise SwarmError(f"node {self.index}: graceful shutdown timed out") from None
        self.process = None
        if code != 0:
            raise SwarmError(f"node {self.index}: exited with status {code}")


class Fleet:
    def __init__(self, kind: str, config: dict, root: Path, nx: Path, wasm: Path, timeout: float = 60):
        self.kind = kind
        self.config = validate_config(kind, config)
        self.root = root.resolve()
        self.nx = nx.resolve()
        self.wasm = wasm.resolve()
        self.timeout = timeout
        self.nodes: list[Node] = []
        self.cancelled = threading.Event()
        if not self.nx.is_file() or not os.access(self.nx, os.X_OK) or not self.wasm.is_file():
            raise SwarmError("build the native nx binary and guest WASM first")
        self.root.parent.mkdir(parents=True, exist_ok=True)
        self.root.mkdir(mode=0o700)
        (self.root / "config.json").write_text(json.dumps(self.config, indent=2))

    def parallel(self, operation):
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(self.nodes))) as pool:
            return list(pool.map(operation, self.nodes))

    def wait(self, condition, label: str):
        deadline = time.monotonic() + self.timeout
        while True:
            for node in self.nodes:
                node.alive()
            if self.cancelled.is_set():
                raise SwarmError("interrupted")
            result = condition()
            if result:
                return result
            if time.monotonic() >= deadline:
                raise SwarmError(f"timeout: {label}")
            self.cancelled.wait(0.1)

    def ready(self, node: Node) -> None:
        def condition():
            try:
                node.request("ready")
                return True
            except urllib.error.URLError:
                return False
        self.wait(condition, "daemon readiness")

    def start(self) -> None:
        count = self.config["workers"]
        base = self.config["base_port"]
        reservations = []
        try:
            for port in range(base, base + count * 2):
                reservation = socket.socket()
                reservations.append(reservation)
                reservation.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                reservation.bind(("127.0.0.1", port))
            for index in range(count):
                neighbor_indices = sorted({(index - 1) % count, (index + 1) % count} - {index})
                self.nodes.append(Node(index, self.root / f"node-{index}", base + index, base + count + index,
                                       self.nx, [base + neighbor for neighbor in neighbor_indices]))
        finally:
            for reservation in reservations:
                reservation.close()
        for node in self.nodes:
            node.start()
        for node in self.nodes:
            self.ready(node)
            owner = [record for record in node.view("membership") if record["endpoint"] == f"127.0.0.1:{node.network}"]
            if len(owner) != 1:
                raise SwarmError("local membership owner unavailable")
            node.identity = owner[0]["node_id"]
            registered = json.loads(node.request("modules", "POST", self.wasm.read_bytes(), "application/wasm", (200, 201)))
            node.module = registered["id"]
            settings = dict(NUMAX_GRID_W=self.config["grid_width"], NUMAX_GRID_H=self.config["grid_height"], NUMAX_SEED=self.config["seed"])
            if self.kind == "ants":
                settings.update(NUMAX_HOTSPOT_COUNT=self.config["hotspot_count"], NUMAX_NEST_X=self.config["nest_x"], NUMAX_NEST_Y=self.config["nest_y"])
            else:
                settings.update(NUMAX_ANCHOR_COUNT=self.config["anchor_count"], NUMAX_SETTLE_RADIUS=self.config["settle_radius"],
                                NUMAX_PARTICLE_COUNT=count, NUMAX_PARTICLE_ID=node.index, NUMAX_POLARITY=int(self.config["polarity_enabled"]))
            node.control({name: str(value) for name, value in settings.items()})
        self.wait(self.formed, "healthy membership")

    def formed(self) -> bool:
        identities = {node.identity for node in self.nodes}
        return all({record["node_id"] for record in node.view("membership") if record["state"] == "alive" and not record["service_error"]} >= identities for node in self.nodes)

    def converge(self, label: str) -> dict:
        targets: dict[str, int] = {}
        for node in self.nodes:
            for progress in node.view("recovery"):
                origin = progress["origin"]
                targets[origin] = max(targets.get(origin, 0), progress["known_high_sequence"])

        def condition():
            for node in self.nodes:
                progress = {item["origin"]: item for item in node.view("recovery")}
                for origin, sequence in targets.items():
                    item = progress.get(origin)
                    if item is None or item["contiguous_sequence"] < sequence or item["sparse_positions"] or any(
                        item[name] for name in ("source_unavailable", "baseline_missing", "baseline_unconfirmed", "recovery_failure")
                    ):
                        return False
            snapshots = self.parallel(lambda node: node.snapshot())
            return snapshots[0] if all(snapshot == snapshots[0] for snapshot in snapshots) else False

        snapshot = self.wait(condition, "replica convergence")
        rendered = self.nodes[0].snapshot(True, label)
        if rendered != snapshot:
            raise SwarmError("snapshot changed after convergence")
        (self.root / f"snapshot-{label.split(':')[0].replace(' ', '-')}.json").write_text(json.dumps(snapshot, indent=2))
        return snapshot

    def run(self) -> dict:
        for tick in range(1, self.config["ticks"] + 1):
            if self.cancelled.is_set():
                raise SwarmError("interrupted")
            boost = ""
            if self.kind == "magnets":
                for phase in range(2, self.config["phase_count"] + 1):
                    if tick == self.config[f"phase{phase}_at_tick"]:
                        self.converge(f"phase {phase - 1}: before next boost")
                        boost = self.config[f"phase{phase}_boosts"]
            self.parallel(lambda node: node.tick(tick, boost if node.index == 0 else ""))
        label = f"phase {self.config['phase_count']}: final" if self.kind == "magnets" else "final"
        return self.converge(label)

    def stop(self) -> None:
        for node in self.nodes:
            if node.process is not None and node.process.poll() is None:
                node.process.terminate()
        failures = []
        for node in self.nodes:
            try:
                node.stop()
            except SwarmError as error:
                failures.append(str(error))
        if failures:
            raise SwarmError("; ".join(failures))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=("ants", "magnets"))
    parser.add_argument("config", type=Path)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--nx", type=Path, default=Path(__file__).resolve().parent.parent / "target/debug/nx")
    parser.add_argument("--timeout", type=float, default=60)
    arguments = parser.parse_args()
    if not 0 < arguments.timeout <= 600:
        parser.error("timeout must be between 0 and 600 seconds")
    with arguments.config.open("rb") as stream:
        config = tomllib.load(stream)
    example = Path(__file__).resolve().parent / f"distributed_{arguments.kind}"
    root = arguments.state or example / "runs" / secrets.token_hex(8)
    fleet = Fleet(arguments.kind, config, root, arguments.nx,
                  example / "target/wasm32-unknown-unknown/release" / f"distributed_{arguments.kind}.wasm", arguments.timeout)
    previous_handlers = {}
    for name in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[name] = signal.signal(name, lambda *_: fleet.cancelled.set())
    print(f"Private run directory: {fleet.root}", flush=True)
    try:
        fleet.start()
        snapshot = fleet.run()
    finally:
        try:
            fleet.stop()
        finally:
            for name, handler in previous_handlers.items():
                signal.signal(name, handler)
    report = dict(passed=True, kind=arguments.kind, workers=len(fleet.nodes), ticks=fleet.config["ticks"],
                  identities=[node.identity for node in fleet.nodes], pids=[node.pids for node in fleet.nodes], snapshot=snapshot)
    (fleet.root / "report.json").write_text(json.dumps(report, indent=2))
    renderer = "render_paths.py" if arguments.kind == "ants" else "render_topology.py"
    print(f"Render: python3 {example / renderer} {fleet.nodes[0].directory / 'daemon.log'} --grid-width {fleet.config['grid_width']} --grid-height {fleet.config['grid_height']} --out {fleet.root / 'report.html'}")


if __name__ == "__main__":
    try:
        main()
    except (SwarmError, OSError, ValueError, urllib.error.URLError) as error:
        print(f"swarm_demo: {error}", file=sys.stderr)
        raise SystemExit(1) from None