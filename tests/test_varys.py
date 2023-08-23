import unittest
import tempfile
import os
import json
from varys import varys

DIR = os.path.dirname(__file__)
LOG_FILENAME = os.path.join(DIR, 'test.log')
TMP_HANDLE, TMP_FILENAME = tempfile.mkstemp()

class TestVarys(unittest.TestCase):

    def test_varys(self):
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

        text = "Hello, world!"

        v = varys('test', LOG_FILENAME, config_path=TMP_FILENAME)

        self.assertRaises(Exception, v.send, text, 'basic')
        self.assertRaises(Exception, v.receive, 'basic', block=False)

        v.send(text, 'basic', queue_suffix='q')
        message = v.receive('basic', queue_suffix='q')
        self.assertEqual(text, json.loads(message.body))

        message = v.receive('basic', queue_suffix='q', block=False)
        v.send(text, 'basic', queue_suffix='q')
        messages = v.receive_batch('basic', queue_suffix='q')

        self.assertRaises(Exception, v.receive_batch, 'batch', block=False)

        channels = v.get_channels()
        for key in channels['consumer_channels']:
            print(f"tearing down consumer {key}")
            v._varys__in_channels[key]["varys_obj"].close_connection()
            v._varys__in_channels[key]["varys_obj"].stop()

        for key in channels['producer_channels']:
            print(f"tearing down producer {key}")
            v._varys__out_channels[key]["varys_obj"].stop()

    def test_config_not_json(self):
        with open(TMP_FILENAME, 'w') as f:
            f.write("asdf9υ021ζ3;-ö×=()[]{}∇Δοo")
            
        self.assertRaises(SystemExit, varys, 'test', LOG_FILENAME, config_path=TMP_FILENAME)

        os.remove(TMP_FILENAME)

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
            varys('test', LOG_FILENAME, config_path=TMP_FILENAME)
            
        self.assertEqual(cm.exception.code, 2)

        os.remove(TMP_FILENAME)

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
            
        self.assertRaises(SystemExit, varys, 'test', LOG_FILENAME, config_path=TMP_FILENAME)

        os.remove(TMP_FILENAME)
