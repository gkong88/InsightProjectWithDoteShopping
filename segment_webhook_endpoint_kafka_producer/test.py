from unittest import TestCase
import KafkaRESTAPI
# from KafkaRESTAPI.py import KafkaRESTAPI

class TestTime(TestCase):
    def test_segment_timestamp_to_unix_millis(self):
        self.assertEqual(KafkaRESTAPI.segment_timestamp_to_unix_millis("2019-09-21T20:31:07.942Z"), 1569097867942)
        print("passed timestamp conversion test")

