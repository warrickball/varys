import unittest
import time
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
        # this seems to prevent some hanging
        # or errors related to closing connections that haven't opened yet
        # I presume because some operations are so fast
        # that we try to close the connections before they've opened
        # 0.01s seems to be sufficient; 0.1s is just a bit conservative
        time.sleep(0.1)

        channels = self.v.get_channels()
        for key in channels['consumer_channels']:
            print(f"tearing down consumer {key}")
            self.v._in_channels[key]["varys_obj"].close_connection()
            self.v._in_channels[key]["varys_obj"].stop()

        for key in channels['producer_channels']:
            print(f"tearing down producer {key}")
            self.v._out_channels[key]["varys_obj"].stop()

        os.remove(TMP_FILENAME)

    def test_send_and_receive(self):
        self.v.send(TEXT, 'basic', queue_suffix='q')
        message = self.v.receive('basic', queue_suffix='q')
        self.assertEqual(TEXT, json.loads(message.body))

    def test_send_and_receive_batch(self):
        self.v.send(TEXT, 'basic', queue_suffix='q')
        self.v.send(TEXT, 'basic', queue_suffix='q')
        # give the messages time to be received / processed by rmq
        time.sleep(1)
        messages = self.v.receive_batch('basic', queue_suffix='q')
        parsed_messages = [json.loads(m.body) for m in messages]
        self.assertListEqual([TEXT, TEXT], parsed_messages)

    def test_receive_no_message(self):
        self.assertIsNone(self.v.receive('basic', queue_suffix='q'))

    def test_send_no_suffix(self):
        self.assertRaises(Exception, self.v.send, TEXT, 'basic')

    def test_receive_no_suffix(self):
        self.assertRaises(Exception, self.v.receive, 'basic')

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
