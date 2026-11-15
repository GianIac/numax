import os
import socket
import tempfile
import threading
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import Mock, patch

from swarm_demo import Fleet, Node, NoRedirect, SwarmError, validate_config


class ConfigurationTests(unittest.TestCase):
    def test_validation_and_port_budgets(self):
        self.assertEqual(validate_config("ants", {})["workers"], 4)
        for config in ({"ants": True}, {"ants": 0}, {"base_port": 65535}, {"nest_x": 100}, {"seed": -1}):
            with self.subTest(config=config), self.assertRaises(SwarmError):
                validate_config("ants", config)

    def test_invalid_phase_does_not_silently_skip_boost(self):
        for boosts in ("bad", "0:0", "3:1", "0:-9223372036854775808"):
            with self.subTest(boosts=boosts), self.assertRaises(SwarmError):
                validate_config("magnets", dict(phase_count=2, phase2_at_tick=2, phase2_boosts=boosts))

    def test_existing_run_directory_is_preserved(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            marker = root / "important"
            marker.write_text("preserve")
            with self.assertRaises(FileExistsError):
                Fleet("ants", {}, root, Path("/usr/bin/true"), marker)
            self.assertEqual(marker.read_text(), "preserve")

    def test_mutation_is_not_retried_after_uncertain_response(self):
        node = object.__new__(Node)
        with patch.object(node, "control"), patch.object(node, "run", side_effect=SwarmError("uncertain")) as run:
            with self.assertRaises(SwarmError):
                node.tick(1, "0:9")
            run.assert_called_once()

    def test_snapshot_cannot_reuse_previous_value(self):
        node = object.__new__(Node)
        node.index = 0
        stored = {"snapshot": b'{"previous":true}'}

        def control(values, clear_snapshot=False):
            if clear_snapshot:
                stored["snapshot"] = b""

        with patch.object(node, "control", side_effect=control), patch.object(node, "run") as run, patch.object(
            node, "request", side_effect=lambda route: stored["snapshot"]
        ):
            with self.assertRaisesRegex(SwarmError, "incomplete snapshot"):
                node.snapshot()
            run.assert_called_once()

    def test_snapshot_requires_fresh_object(self):
        node = object.__new__(Node)
        node.index = 0
        for data, succeeds in ((b'{"field":[1]}', True), (b"null", False), (b"[]", False)):
            with self.subTest(data=data), patch.object(node, "control") as control, patch.object(node, "run"), patch.object(
                node, "request", return_value=data
            ):
                if succeeds:
                    self.assertEqual(node.snapshot(), {"field": [1]})
                else:
                    with self.assertRaisesRegex(SwarmError, "invalid snapshot"):
                        node.snapshot()
                self.assertTrue(control.call_args.kwargs["clear_snapshot"])

    def test_failed_snapshot_invalidation_does_not_run_guest(self):
        node = object.__new__(Node)
        with patch.object(node, "control", side_effect=SwarmError("invalidation failed")), patch.object(node, "run") as run:
            with self.assertRaisesRegex(SwarmError, "invalidation failed"):
                node.snapshot()
            run.assert_not_called()

    def test_wait_checks_deadline_before_and_after_condition(self):
        fleet = object.__new__(Fleet)
        fleet.timeout = 1.0
        fleet.nodes = []
        fleet.cancelled = threading.Event()
        for times, called in (((0, 1), False), ((0, 0, 1), True), ((0, 0, 2), True)):
            with self.subTest(times=times), patch("swarm_demo.time.monotonic", side_effect=times):
                condition = Mock(return_value=True)
                with self.assertRaisesRegex(SwarmError, "timeout: observation"):
                    fleet.wait(condition, "observation")
                self.assertEqual(condition.called, called)

    def test_wait_accepts_on_time_and_rejects_cancelled_results(self):
        fleet = object.__new__(Fleet)
        fleet.timeout = 1.0
        fleet.nodes = []
        fleet.cancelled = threading.Event()
        with patch("swarm_demo.time.monotonic", side_effect=(0, 0, 0.5)):
            self.assertEqual(fleet.wait(lambda: {"fresh": True}, "observation"), {"fresh": True})

        def cancel():
            fleet.cancelled.set()
            return True

        with patch("swarm_demo.time.monotonic", side_effect=(0, 0)):
            with self.assertRaisesRegex(SwarmError, "interrupted"):
                fleet.wait(cancel, "observation")

    def test_wait_polling_does_not_exceed_remaining_budget(self):
        fleet = object.__new__(Fleet)
        fleet.timeout = 1.0
        fleet.nodes = []
        fleet.cancelled = threading.Event()
        condition = Mock(return_value=False)
        with patch("swarm_demo.time.monotonic", side_effect=(0, 0, 0.95, 1)), patch.object(fleet.cancelled, "wait") as wait:
            with self.assertRaisesRegex(SwarmError, "timeout: observation"):
                fleet.wait(condition, "observation")
            condition.assert_called_once()
            wait.assert_called_once()
            self.assertAlmostEqual(wait.call_args.args[0], 0.05)

    def test_repeated_cursor_is_rejected(self):
        node = object.__new__(Node)
        with patch.object(node, "request", return_value=b'{"items":[],"next_cursor":"same"}'):
            with self.assertRaisesRegex(SwarmError, "cursor"):
                node.view("recovery")

    def test_membership_uses_lowercase_api_states(self):
        fleet = object.__new__(Fleet)
        node = object.__new__(Node)
        node.identity = "owner"
        fleet.nodes = [node]
        with patch.object(node, "view", return_value=[dict(node_id="owner", state="alive", service_error=None)]):
            self.assertTrue(fleet.formed())
        with patch.object(node, "view", return_value=[dict(node_id="owner", state="suspect", service_error=None)]):
            self.assertFalse(fleet.formed())

    def test_authenticated_requests_refuse_redirects(self):
        node = object.__new__(Node)
        node.index = 0
        node.management = 9000
        node.token = "test-token"
        with patch.object(node, "alive"), patch("swarm_demo.urllib.request.build_opener") as builder:
            builder.return_value.open.side_effect = urllib.error.HTTPError("http://localhost", 302, "redirect", {}, None)
            with self.assertRaisesRegex(SwarmError, "HTTP 302"):
                node.request("ready")
            self.assertTrue(any(isinstance(handler, NoRedirect) for handler in builder.call_args.args))
            self.assertIsNone(NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://outside.invalid"))


@unittest.skipUnless(os.environ.get("NUMAX_SWARM_E2E") == "1", "opt-in real daemon tests")
class LifecycleTests(unittest.TestCase):
    def available_ports(self, count):
        for _ in range(32):
            reservations = []
            try:
                first = socket.socket()
                reservations.append(first)
                first.bind(("127.0.0.1", 0))
                base = first.getsockname()[1]
                if base + count > 65535:
                    continue
                for port in range(base + 1, base + count):
                    reservation = socket.socket()
                    reservations.append(reservation)
                    reservation.bind(("127.0.0.1", port))
                return base
            except OSError:
                continue
            finally:
                for reservation in reservations:
                    reservation.close()
        self.fail("cannot allocate isolated test ports")

    def fleet(self, kind, root):
        examples = Path(__file__).resolve().parent
        config = dict(grid_width=4, grid_height=4, base_port=self.available_ports(4))
        if kind == "ants":
            config.update(ants=2, ticks_per_ant=4)
        else:
            config.update(particles=2, ticks_per_particle=4, phase_count=2, phase2_at_tick=3, phase2_boosts="0:2")
        return Fleet(kind, config, root, examples.parent / "target/debug/nx",
                     examples / f"distributed_{kind}/target/wasm32-unknown-unknown/release" / f"distributed_{kind}.wasm")

    def test_ticks_read_only_observation_and_durable_restart(self):
        for kind in ("ants", "magnets"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                fleet = self.fleet(kind, Path(temporary) / "run")
                try:
                    fleet.start()
                    snapshot = fleet.run()
                    self.assertTrue(all(len(node.pids) == 1 for node in fleet.nodes))
                    node = fleet.nodes[0]
                    if kind == "magnets":
                        node.control(dict(NUMAX_BOOST="0:2"))
                    self.assertEqual(node.snapshot(), snapshot)
                    with patch.object(node, "run") as run:
                        with self.assertRaisesRegex(SwarmError, "incomplete snapshot"):
                            node.snapshot()
                        run.assert_called_once()
                    self.assertEqual(node.snapshot(), snapshot)
                    identity = node.identity
                    node.stop()
                    node.start()
                    fleet.ready(node)
                    fleet.wait(fleet.formed, "membership after restart")
                    self.assertEqual(fleet.converge("restart"), snapshot)
                    self.assertEqual(node.identity, identity)
                    self.assertTrue(any(record["node_id"] == identity for record in node.view("membership")))
                    processes = [member.process for member in fleet.nodes]
                finally:
                    fleet.stop()
                self.assertTrue(all(process.poll() == 0 for process in processes))
                for member in fleet.nodes:
                    log = (member.directory / "daemon.log").read_text()
                    self.assertNotIn(member.token, log)
                    self.assertEqual(log.count(f"distributed_{kind}: start"), fleet.config["ticks"])

    def test_cancellation_stops_owned_daemons(self):
        with tempfile.TemporaryDirectory() as temporary:
            fleet = self.fleet("ants", Path(temporary) / "run")
            try:
                fleet.start()
                processes = [node.process for node in fleet.nodes]
                fleet.cancelled.set()
                with self.assertRaisesRegex(SwarmError, "interrupted"):
                    fleet.wait(lambda: False, "cancelled scenario")
            finally:
                fleet.stop()
            self.assertTrue(all(process.poll() == 0 for process in processes))

    def test_owned_process_failure_is_not_hidden(self):
        with tempfile.TemporaryDirectory() as temporary:
            fleet = self.fleet("ants", Path(temporary) / "run")
            try:
                fleet.start()
                process = fleet.nodes[0].process
                process.kill()
                process.wait(timeout=5)
                with self.assertRaisesRegex(SwarmError, "exited"):
                    fleet.nodes[0].alive()
            finally:
                with self.assertRaisesRegex(SwarmError, "exited with status"):
                    fleet.stop()
            self.assertTrue(all(node.process is None for node in fleet.nodes))


if __name__ == "__main__":
    unittest.main()