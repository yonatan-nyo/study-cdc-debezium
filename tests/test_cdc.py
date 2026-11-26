import importlib.util
import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_consumer():
    pg = ModuleType("psycopg")
    pg_json = ModuleType("psycopg.types.json")
    pg_json.Jsonb = lambda value: value
    kafka = ModuleType("confluent_kafka")
    kafka.Consumer = object
    kafka.KafkaException = RuntimeError
    with patch.dict(sys.modules, {
        "psycopg": pg, "psycopg.types": ModuleType("psycopg.types"),
        "psycopg.types.json": pg_json, "confluent_kafka": kafka,
    }):
        return load_module("kilatmart_consumer", "consumer/consumer.py")


consumer_module = load_consumer()
demo = load_module("kilatmart_demo", "demo.py")
register = load_module("kilatmart_register", "connect/register_connector.py")


class ConsumerTest(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock()
        self.consumer = MagicMock()
        self.consumer.commit.return_value = [SimpleNamespace(error=None)]
        self.message = MagicMock()
        self.message.error.return_value = None
        self.message.topic.return_value = consumer_module.TOPIC
        self.message.partition.return_value = 0
        self.message.offset.return_value = 5
        self.message.key.return_value = b'{"id": 6}'
        self.message.value.return_value = b'{"op":"c","after":{"order_ref":"KM-1006"}}'

    def process(self):
        with redirect_stdout(io.StringIO()):
            consumer_module.process_message(self.db, self.consumer, self.message)

    def test_commits_the_database_before_advancing_kafka(self):
        calls = MagicMock()
        calls.attach_mock(self.db, "db")
        calls.attach_mock(self.consumer, "kafka")
        self.process()
        self.assertEqual(
            [call[0] for call in calls.mock_calls],
            ["db.transaction", "db.transaction().__enter__", "db.execute",
             "db.transaction().__exit__", "kafka.commit"],
        )
        self.assertEqual(
            self.db.execute.call_args.args[1][:6],
            (consumer_module.TOPIC, 0, 5, "c", 6, "KM-1006"),
        )
        self.consumer.commit.assert_called_once_with(message=self.message, asynchronous=False)

    def test_failed_database_write_does_not_commit_the_offset(self):
        self.db.execute.side_effect = RuntimeError("database unavailable")
        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            self.process()
        self.consumer.commit.assert_not_called()

    def test_tombstone_only_advances_the_offset(self):
        self.message.value.return_value = None
        self.process()
        self.db.execute.assert_not_called()
        self.consumer.commit.assert_called_once()

    def test_delete_uses_id_from_message_key(self):
        self.message.value.return_value = b'{"op":"d","after":null,"before":{"id":6}}'
        self.process()
        self.assertEqual(self.db.execute.call_args.args[1][3:6], ("d", 6, None))


class DemoTest(unittest.TestCase):
    def test_reads_offset_row_for_inactive_group(self):
        output = ("GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID\n"
                  "kilatmart-analytics kilatmart.public.orders 0 6 8 2 -")
        with patch.object(demo, "compose", return_value=output):
            self.assertEqual(
                demo.group_offsets(), {"committed_offset": 6, "log_end_offset": 8, "lag": 2}
            )

    def test_returns_no_offsets_for_uncommitted_group(self):
        with patch.object(demo, "compose", return_value=(
            "kilatmart-analytics kilatmart.public.orders 0 - 5 - consumer-1"
        )):
            self.assertIsNone(demo.group_offsets())


class RegistrationTest(unittest.TestCase):
    def test_checks_task_state_in_addition_to_connector_state(self):
        status = {"connector": {"state": "RUNNING"}, "tasks": []}
        self.assertFalse(register.running(status))
        status["tasks"] = [{"state": "RUNNING"}]
        self.assertTrue(register.running(status))
        status["tasks"] = [{"state": "FAILED", "trace": "invalid replication slot"}]
        with self.assertRaisesRegex(RuntimeError, "invalid replication slot"):
            register.running(status)


if __name__ == "__main__":
    unittest.main()
