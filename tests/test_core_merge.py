"""纯字节合并与冲突分类的单元测试。"""

import unittest

from offstore.core import (
    OVERLAP,
    TRUNCATED_OTHER_WRITE,
    Conflict,
    lease_active,
    merge_branches,
    validate_relpath,
)


def edit(client, seq, offset, data, base=1):
    return {"client": client, "seq": seq, "action": "edit",
            "offset": offset, "data": data, "base": base}


def cut(client, seq, size, base=1):
    return {"client": client, "seq": seq, "action": "truncate",
            "size": size, "base": base}


class MergeTest(unittest.TestCase):
    def test_disjoint_ranges_merge(self):
        result = merge_branches(
            b"0123456789",
            {"a": [edit("a", 1, 2, b"AB")],
             "b": [edit("b", 1, 5, b"XY")]})
        # a 覆盖 [2,4)，b 覆盖 [5,7)，位置 4 保持基线字节
        self.assertEqual(bytes(result.data), b"01AB4XY789")
        self.assertEqual(result.applied, {("a", 1), ("b", 1)})
        self.assertEqual(result.conflicts, [])

    def test_same_range_same_bytes_dedupes_to_one(self):
        result = merge_branches(
            b"0123456789",
            {"a": [edit("a", 1, 3, b"QQ")],
             "b": [edit("b", 1, 3, b"QQ")]})
        self.assertEqual(bytes(result.data), b"012QQ56789")
        self.assertEqual(result.applied, {("a", 1), ("b", 1)})
        self.assertEqual(result.conflicts, [])

    def test_overlapping_different_bytes_is_retained(self):
        result = merge_branches(
            b"0123456789",
            {"a": [edit("a", 1, 3, b"AB")],
             "b": [edit("b", 1, 4, b"CD")]})  # 共用位置 4: B vs C
        self.assertEqual(bytes(result.data), b"0123456789")  # 冲突字节不自动拼装
        self.assertEqual(result.applied, set())
        self.assertEqual(len(result.conflicts), 1)
        self.assertEqual(result.conflicts[0].reason, OVERLAP)
        self.assertEqual({w["client"] for w in result.conflicts[0].writes}, {"a", "b"})

    def test_overlap_where_shared_positions_equal_is_clean(self):
        # a 写 [2,4)=AB，b 写 [3,5)=BZ；位置 3 两边都是 B，仅位置 4 来自 b
        result = merge_branches(
            b"0123456789",
            {"a": [edit("a", 1, 2, b"AB")],
             "b": [edit("b", 1, 3, b"BZ")]})
        self.assertEqual(bytes(result.data), b"01ABZ56789")
        self.assertEqual(result.conflicts, [])

    def test_divergent_base_is_evolution_not_conflict(self):
        # a 从基线 1 分叉，b 从更新的基线 2 分叉：b 已看到 a 的发布结果，
        # 即使在重叠区间给不同字节也是正常演进，不构成离线冲突。
        result = merge_branches(
            b"AB23456789",
            {"a": [edit("a", 1, 0, b"AB", base=1)],
             "b": [edit("b", 1, 0, b"CD", base=2)]})
        self.assertEqual(result.conflicts, [])
        self.assertEqual(bytes(result.data), b"CD23456789")

    def test_missing_base_does_not_fabricate_conflict(self):
        # 没有基线信息时不能凭空判冲突，保守按可合并处理
        result = merge_branches(
            b"0123456789",
            {"a": [edit("a", 1, 0, b"AB", base=None)],
             "b": [edit("b", 1, 0, b"CD", base=None)]})
        self.assertEqual(result.conflicts, [])

    def test_truncate_removing_other_write_is_conflict(self):
        result = merge_branches(
            b"0123456789",
            {"a": [cut("a", 1, 6)],
             "b": [edit("b", 1, 7, b"Z")]})
        self.assertEqual(result.conflicts[0].reason, TRUNCATED_OTHER_WRITE)
        # 冲突截断不改变公共长度，冲突编辑不拼装
        self.assertEqual(len(result.data), 10)
        self.assertEqual(bytes(result.data), b"0123456789")

    def test_truncate_before_other_write_start_is_clean(self):
        result = merge_branches(
            b"0123456789",
            {"a": [cut("a", 1, 6)],
             "b": [edit("b", 1, 2, b"Q")]})
        self.assertEqual(result.conflicts, [])
        self.assertEqual(len(result.data), 6)
        self.assertEqual(bytes(result.data), b"01Q345")

    def test_same_branch_sequential_edits_never_conflict(self):
        result = merge_branches(
            b"0123456789",
            {"a": [edit("a", 1, 0, b"X"), edit("a", 2, 0, b"Y")]})
        self.assertEqual(result.conflicts, [])
        self.assertEqual(bytes(result.data), b"Y123456789")

    def test_edit_past_end_zero_fills_gap(self):
        result = merge_branches(b"abc", {"a": [edit("a", 1, 5, b"Z")]})
        self.assertEqual(bytes(result.data), b"abc\x00\x00Z")

    def test_truncate_then_edit_in_same_branch(self):
        result = merge_branches(
            b"0123456789",
            {"a": [cut("a", 1, 4), edit("a", 2, 3, b"Z")]})
        self.assertEqual(bytes(result.data), b"012Z")


class RuleTest(unittest.TestCase):
    def test_lease_right_endpoint_exclusive(self):
        self.assertTrue(lease_active(22, 21))
        self.assertFalse(lease_active(22, 22))
        self.assertFalse(lease_active(22, 23))

    def test_relpath_rules(self):
        for good in ("a", "banks/east.dat", "a/b/c.bin", "A", "x.y/..z"):
            validate_relpath(good)
        for bad in ("", "/abs", "a/../b", "a/./b", "a//b", "a/b/", ".."):
            with self.assertRaises(ValueError):
                validate_relpath(bad)

    def test_case_sensitive_paths_are_distinct(self):
        from offstore.store import CentralStore
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            store = CentralStore(d, 100)
            store.seed_baseline([{"id": "i1", "path": "Scan.dat", "file": "o"}],
                                {"o": b"AAAA"})
            store.issue_lease("c1", 1, 10, 0)
            # 小写路径与大写路径是两个目录项，不构成 path_taken
            record = store.ingest({"client": "c1", "seq": 1, "action": "create",
                                   "id": "i2", "path": "scan.dat", "at": 1,
                                   "sha256": __import__("hashlib").sha256(b"B").hexdigest(),
                                   "data": b"B"})
            self.assertEqual(record["state"], "integrated")
            self.assertEqual(store.directory(), {"Scan.dat": "i1", "scan.dat": "i2"})


if __name__ == "__main__":
    unittest.main()
