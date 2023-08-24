import unittest
import tempfile
import os
import json
from varys import varys

DIR = os.path.dirname(__file__)
LOG_FILENAME = os.path.join(DIR, 'test.log')
TMP_HANDLE, TMP_FILENAME = tempfile.mkstemp()
TEXT = "Hello, world!"

class TestVarys(unittest.TestCase):

    def setUp(self):
        config = {
            "version": "0.1",
            "profiles": {
                "test": {
                    "username": "guest",
                    "password": "guest",
                    "amqp_url": "127.0.0.1",
                    "port": 5672
                }
            }
        }

        with open(TMP_FILENAME, 'w') as f:
            json.dump(config, f, ensure_ascii=False)

        self.v = varys('test', LOG_FILENAME, config_path=TMP_FILENAME)

    def tearDown(self):
        channels = self.v.get_channels()
        for key in channels['consumer_channels']:
            print(f"tearing down consumer {key}")
            self.v._varys__in_channels[key]["varys_obj"].close_connection()
            self.v._varys__in_channels[key]["varys_obj"].stop()

        for key in channels['producer_channels']:
            print(f"tearing down producer {key}")
            self.v._varys__out_channels[key]["varys_obj"].stop()

        os.remove(TMP_FILENAME)

    def test_send_and_receive(self):
        self.v.send(TEXT, 'basic', queue_suffix='q')
        message = self.v.receive('basic', queue_suffix='q')
        self.assertEqual(TEXT, json.loads(message.body))

    # def test_send_and_receive_batch(self):
    # hangs if in separate test
        self.v.send(TEXT, 'basic', queue_suffix='q')
        messages = self.v.receive_batch('basic', queue_suffix='q')

    # def test_receive_no_message(self):
    # hangs if in separate test
        self.assertIsNone(self.v.receive('basic', queue_suffix='q', block=False))

    def test_send_no_suffix(self):
        self.assertRaises(Exception, self.v.send, TEXT, 'basic')

    def test_receive_no_suffix(self):
        self.assertRaises(Exception, self.v.receive, 'basic', block=False)

    def test_receive_batch_no_suffix(self):
        self.assertRaises(Exception, self.v.receive_batch, 'basic')


class TestVarysConfig(unittest.TestCase):

    def tearDown(self):
        os.remove(TMP_FILENAME)

    def test_config_not_json(self):
        with open(TMP_FILENAME, 'w') as f:
            f.write("asdf9υ021ζ3;-ö×=()[]{}∇Δοo")

        # use a context manager so we can check SystemExit code
        with self.assertRaises(SystemExit) as cm:
            v = varys('test', LOG_FILENAME, config_path=TMP_FILENAME)

        self.assertEqual(cm.exception.code, 11)

    def test_config_profile_missing(self):
        config = {
            "version": "0.2",  # bad version prints warning but doesn't raise error
            "profiles": {
                "asdfadsf": {}
            }
        }

        with open(TMP_FILENAME, 'w') as f:
            json.dump(config, f, ensure_ascii=False)
            
        with self.assertRaises(SystemExit) as cm:
            v = varys('test', LOG_FILENAME, config_path=TMP_FILENAME)
            
        self.assertEqual(cm.exception.code, 2)

    def test_config_profile_incomplete(self):
        config = {
            "version": "0.1",
            "profiles": {
                "test": {
                    "username": "username",
                    "extra": "unnecessary",
                }
            }
        }

        with open(TMP_FILENAME, 'w') as f:
            json.dump(config, f, ensure_ascii=False)

        with self.assertRaises(SystemExit) as cm:
            v = varys('test', LOG_FILENAME, config_path=TMP_FILENAME)

        self.assertEqual(cm.exception.code, 11)
