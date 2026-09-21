import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mapping_check import snapshot


class MappingCheckTests(unittest.TestCase):
    def test_mapping_scope_thp_and_unknown_numa(self):
        smaps = (
            "1000-9000 rw-p 00000000 00:00 0 [heap]\nKernelPageSize: 64 kB\nAnonHugePages: 2048 kB\nVmFlags: rd wr\n"
        )
        row = snapshot(smaps, "", 0x2000, 4096)
        self.assertEqual(row["status"], "covered")
        self.assertEqual(row["vmas"][0]["vma_bytes"], 32768)
        self.assertEqual(row["vmas"][0]["AnonHugePages_kb"], 2048)
        self.assertIsNone(row["vmas"][0]["numa_pages"])
        self.assertFalse(row["vmas"][0]["hugetlb_flag"])

    def test_hugetlb_numa_and_missing_address(self):
        smaps = (
            "200000-400000 rw-p 00000000 00:00 0\nKernelPageSize: 2048 kB\n"
            "Private_Hugetlb: 2048 kB\nVmFlags: rd wr ht\n"
        )
        row = snapshot(smaps, "200000 default huge N3=1 kernelpagesize_kB=2048\n", 0x200000, 656)
        self.assertTrue(row["vmas"][0]["hugetlb_flag"])
        self.assertEqual(row["vmas"][0]["numa_pages"], {"N3": 1})
        self.assertEqual(snapshot(smaps, "", 0x1000, 4096)["status"], "partial-or-unknown")
