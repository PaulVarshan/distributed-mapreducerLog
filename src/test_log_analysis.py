"""
Unit tests for log_analysis.py (small sample records, not the full Apache.log).

Run:  python -m unittest discover -s src -v
  or: python src/test_log_analysis.py
"""

import tempfile
import unittest
from pathlib import Path

import log_analysis as la

ERROR_LINE = ("[Sun Nov 27 00:50:47 2005] [error] [client 61.155.76.2] "
              "Directory index forbidden by rule: /var/www/html/")
NOTICE_LINE = "[Thu Jun 09 06:07:04 2005] [notice] LDAP: SSL support unavailable"
ERROR_NO_IP = "[Thu Jun 09 06:07:05 2005] [error] env.createBean2(): Factory error creating vm: ( vm, )"
MALFORMED_LINE = "script not found or unable to stat"

SAMPLE = [
    ERROR_LINE,
    NOTICE_LINE,
    ERROR_NO_IP,
    "[Sun Nov 27 01:00:00 2005] [error] [client 61.155.76.2] File does not exist: /var/www/html/a",
    "[Mon Nov 28 02:00:00 2005] [error] [client 10.0.0.1] File does not exist: /var/www/html/b",
    MALFORMED_LINE,
    "[Mon Nov 28 03:00:00 2005] [warn] child process 123 still did not exit, sending a SIGTERM",
]


class TestParsing(unittest.TestCase):
    def test_parse_notice(self):
        parsed = la.parse_log_line(NOTICE_LINE)
        self.assertEqual(parsed["level"], "notice")
        self.assertEqual(parsed["timestamp"], "Thu Jun 09 06:07:04 2005")
        self.assertIsNone(parsed["client_ip"])
        self.assertEqual(parsed["message"], "LDAP: SSL support unavailable")

    def test_parse_error(self):
        parsed = la.parse_log_line(ERROR_LINE)
        self.assertEqual(parsed["level"], "error")
        self.assertEqual(parsed["message"], "Directory index forbidden by rule: /var/www/html/")

    def test_extract_client_ip(self):
        self.assertEqual(la.parse_log_line(ERROR_LINE)["client_ip"], "61.155.76.2")
        self.assertIsNone(la.parse_log_line(ERROR_NO_IP)["client_ip"])

    def test_malformed_line(self):
        self.assertIsNone(la.parse_log_line(MALFORMED_LINE))

    def test_timestamp_to_date(self):
        self.assertEqual(la.timestamp_to_date("Sun Nov 27 00:50:47 2005"), "2005-11-27")


class TestClassifyError(unittest.TestCase):
    def test_known_error_types(self):
        self.assertEqual(la.classify_error("Directory index forbidden by rule: /var/www/html/"),
                         "Directory index forbidden by rule")
        self.assertEqual(la.classify_error("File does not exist: /var/www/html/blog"),
                         "File does not exist")
        self.assertEqual(la.classify_error("request failed: URI too long (longer than 8190)"),
                         "request failed: URI too long")
        self.assertEqual(la.classify_error("env.createBean2(): Factory error creating vm: ( vm, )"),
                         "Factory error creating")

    def test_dynamic_values_are_grouped(self):
        a = la.classify_error("mod_jk child workerEnv in error state 5")
        b = la.classify_error("mod_jk child workerEnv in error state 6")
        self.assertEqual(a, b)

    def test_unknown_message_hides_numbers(self):
        self.assertEqual(la.classify_error("something odd 1234: detail"), "something odd N")


if __name__ == "__main__":
    unittest.main(verbosity=2)
