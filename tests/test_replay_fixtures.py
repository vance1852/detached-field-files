"""用仓库提供的两套设备资料做端到端断言。"""

import base64
import tempfile
import unittest
from pathlib import Path

from offstore.replay import load_fixture, replay

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "fixtures"


class RenameCrossesRangeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = replay(FIXTURES / "rename-crosses-range-edits",
                          Path(self.tmp.name) / "run")

    def tearDown(self):
        self.tmp.cleanup()

    def test_trace_shape(self):
        actions = [t["action"] for t in self.out["trace"]]
        self.assertEqual(actions,
                         ["checkout", "checkout", "open", "rename", "edit", "edit",
                          "integrate", "create", "integrate", "integrate", "truncate",
                          "integrate", "edit", "integrate", "read", "resolve"])

    def test_rename_keeps_identity_and_create_reuses_path(self):
        status = self.out["status"]
        self.assertEqual(status["directory"],
                         {"banks/east-old.dat": "x7", "banks/east.dat": "x8"})
        x7 = next(v for k, v in status["writes"].items() if k == "van:1")
        self.assertEqual(x7["action"], "rename")
        self.assertEqual(x7["state"], "integrated")

    def test_disjoint_branch_edits_merged_into_one_version(self):
        status = self.out["status"]
        x7 = status["ids"]
        self.assertIn("x7", x7)
        # 两个不相交 patch（[5,7)=18, [10,12)=30）合入同一版本
        integrated = [w for w in status["writes"].values()
                      if w["state"] == "integrated" and w["action"] == "edit"]
        self.assertEqual({(w["client"], w["seq"]) for w in integrated},
                         {("van", 2), ("foot", 1)})

    def test_truncate_that_removes_other_write_is_retained(self):
        status = self.out["status"]
        cut = status["writes"]["van:4"]
        self.assertEqual(cut["state"], "retained")
        self.assertEqual(cut["reason"], "truncated_other_write")
        conflict = next(c for c in status["conflicts"]
                        if c["reason"] == "truncated_other_write")
        self.assertIn("van:4", conflict["members"])
        self.assertIn("foot:1", conflict["members"])

    def test_expired_lease_edit_retained_then_discarded(self):
        status = self.out["status"]
        late = status["writes"]["foot:2"]
        self.assertEqual(late["state"], "retained")
        self.assertEqual(late["reason"], "lease_expired")
        self.assertEqual(late["resolution"], "discard")
        self.assertTrue(all(not c["open"] for c in status["conflicts"]
                            if "foot:2" in c["members"]))

    def test_old_handle_still_reads_original_byte_range(self):
        reply = self.out["reads"]["r5"]
        self.assertEqual(reply["version"], 1)
        self.assertEqual(base64.b64decode(reply["data_base64"]), b"-leve")

    def test_edge_receipts_and_quota(self):
        van = self.out["volumes"]["van"]
        foot = self.out["volumes"]["foot"]
        # 基线 15；van 容量 36，foot 容量 32；接入后载荷全部释放
        self.assertEqual(van["remaining"], 36 - 15)
        self.assertEqual(foot["remaining"], 32 - 15)
        self.assertEqual(van["pending"], [])
        self.assertTrue(all(r["receipt"] != "local"
                            for r in van["receipts"].values()))

    def test_actual_bytes_match_fixture_hashes(self):
        record, _ = load_fixture(FIXTURES / "rename-crosses-range-edits")
        for name, meta in record["objects"].items():
            self.assertEqual(self.out["objects"][name]["sha256"], meta["sha256"])
            self.assertEqual(self.out["objects"][name]["bytes"], meta["bytes"])


class RemovedIdentityReusedPathTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = replay(FIXTURES / "removed-identity-reused-path",
                          Path(self.tmp.name) / "run")

    def tearDown(self):
        self.tmp.cleanup()

    def test_unlink_create_same_path_keeps_distinct_identities(self):
        status = self.out["status"]
        self.assertEqual(status["directory"], {"scan.dat": "x2"})
        self.assertIn("x1", status["ids"])
        self.assertTrue(status["writes"]["s1:1"]["state"], "integrated")
        self.assertEqual(status["writes"]["s1:2"]["id"], "x2")

    def test_write_to_unlinked_identity_is_retained_with_bytes(self):
        status = self.out["status"]
        rec = status["writes"]["s2:1"]
        self.assertEqual(rec["state"], "retained")
        self.assertEqual(rec["reason"], "unlinked_while_writing")
        # 真实字节仍在中心可被认领
        # （通过事件摘要与对象 edit.bin 一致验证）
        self.assertEqual(rec["sha256"], self.out["objects"]["edit.bin"]["sha256"])

    def test_repeated_integrate_is_idempotent(self):
        # t=5 与 t=6 两次 integrate：第二次没有新写入，也不产生重复记录
        status = self.out["status"]
        self.assertEqual(len(status["writes"]), 3)
        integrates = [t for t in self.out["trace"] if t["action"] == "integrate"]
        last = integrates[-1]["integrate"]
        self.assertEqual(last["integrated"], [])
        self.assertEqual(last["retained"], [])
        self.assertEqual(last["pending_local"], [])


if __name__ == "__main__":
    unittest.main()
