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


class TestSplit(unittest.TestCase):
    def test_split_even_sizes(self):
        chunks = la.split_records(list(range(10)), 3)
        self.assertEqual([len(c) for c in chunks], [4, 3, 3])

    def test_split_keeps_all_records_in_order(self):
        records = list(range(101))
        for n in (1, 2, 3, 4):
            chunks = la.split_records(records, n)
            self.assertEqual(len(chunks), n)
            self.assertEqual([r for c in chunks for r in c], records)

    def test_split_invalid_workers(self):
        with self.assertRaises(ValueError):
            la.split_records([1, 2], 0)


class TestMapShuffleReduce(unittest.TestCase):
    def test_map_emits_pairs(self):
        pairs = la.map_logs([ERROR_LINE])
        self.assertIn((("LEVEL", "error"), 1), pairs)
        self.assertIn((("ERROR_TYPE", "Directory index forbidden by rule"), 1), pairs)
        self.assertIn((("CLIENT_IP", "61.155.76.2"), 1), pairs)
        self.assertIn((("ERROR_DATE", "2005-11-27"), 1), pairs)

    def test_notice_emits_only_level(self):
        pairs = la.map_logs([NOTICE_LINE])
        self.assertEqual(pairs, [(("RECORD", "parsed"), 1), (("LEVEL", "notice"), 1)])

    def test_shuffle_groups_by_key(self):
        pairs = [(("LEVEL", "error"), 1), (("LEVEL", "notice"), 1), (("LEVEL", "error"), 1)]
        groups = la.shuffle(pairs)
        self.assertEqual(groups[("LEVEL", "error")], [1, 1])
        self.assertEqual(groups[("LEVEL", "notice")], [1])

    def test_reduce_sums(self):
        groups = {("LEVEL", "error"): [1, 1, 1], ("CLIENT_IP", "61.155.76.2"): [1, 1, 1, 1]}
        reduced = la.reduce_counts(groups)
        self.assertEqual(reduced["LEVEL"]["error"], 3)
        self.assertEqual(reduced["CLIENT_IP"]["61.155.76.2"], 4)


class TestRunMapReduce(unittest.TestCase):
    """End-to-end with real worker processes on a tiny temporary file."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile("w", suffix=".log", delete=False, encoding="utf-8")
        self.tmp.write("\n".join(SAMPLE))
        self.tmp.close()
        self.path = Path(self.tmp.name)

    def tearDown(self):
        self.path.unlink()

    def test_counts_and_consistency_across_workers(self):
        first = None
        for n in (1, 2, 4):
            r = la.run_mapreduce(self.path, n)
            self.assertEqual(r["total_records"], 7)
            self.assertEqual(r["parsed_records"] + r["malformed_records"], 7)
            self.assertEqual(sum(r["chunks"]), 7)
            self.assertEqual(len(r["worker_info"]), n)
            self.assertEqual(r["level_counts"], {"error": 4, "notice": 1, "warn": 1})
            self.assertEqual(r["error_counts"]["File does not exist"], 2)
            self.assertEqual(r["ip_counts"], {"61.155.76.2": 2, "10.0.0.1": 1})
            self.assertEqual(r["date_counts"], {"2005-11-27": 2, "2005-06-09": 1, "2005-11-28": 1})
            if first is None:
                first = r
            for key in ("level_counts", "error_counts", "ip_counts", "date_counts"):
                self.assertEqual(r[key], first[key])


if __name__ == "__main__":
    unittest.main(verbosity=2)
