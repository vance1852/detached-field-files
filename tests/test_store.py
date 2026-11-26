"""中心接合服务的行为测试。"""

import hashlib
import tempfile
import unittest
from pathlib import Path

from offstore.core import sha256_hex
from offstore.edge import EdgeError, EdgeVolume
from offstore.store import CentralError, CentralStore


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = CentralStore(self.root / "central", central_capacity=100)
        self.store.seed_baseline([{"id": "f1", "path": "a.dat", "file": "base"}],
                                 {"base": b"0123456789"})
        self.store.issue_lease("van", 1, until=20, at=0)
        self.store.issue_lease("foot", 1, until=10, at=0)

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, client, seq, action, at, **kw):
        data = kw.pop("data", b"")
        payload = {"client": client, "seq": seq, "action": action, "id": kw.pop("id", "f1"),
                   "at": at, "sha256": digest(data), "data": data, **kw}
        return self.store.ingest(payload)

    # ----------------------------------------------------------- 身份语义
    def test_rename_keeps_identity_recreate_is_new_identity(self):
        self.write("van", 1, "rename", 1, path="b.dat")
        self.assertEqual(self.store.directory(), {"b.dat": "f1"})
        self.assertEqual(self.store.file_status("f1")["path"], "b.dat")
        rec = self.write("van", 2, "create", 2, id="f2", path="a.dat", data=b"NEW")
        self.assertEqual(rec["state"], "integrated")
        self.assertEqual(set(self.store.directory().values()), {"f1", "f2"})
        # create 必须给新身份：复用既有 id 被协议拒绝，不能挤进 f2
        rec = self.write("van", 3, "create", 3, id="f2", path="c.dat", data=b"X")
        self.assertEqual(rec["state"], "rejected")
        self.assertEqual(rec["reason"], "identity_exists")
        self.assertEqual(self.store.file_status("f2")["current_version"], 1)
        # 同名字的恢复请求也不能刷进另一个文件身份
        with self.assertRaises(CentralError) as ctx:
            self.store.ingest({"client": "van", "seq": 2, "action": "create",
                               "id": "f9", "path": "a.dat", "at": 4,
                               "sha256": digest(b"NEW"), "data": b"NEW"})
        self.assertEqual(ctx.exception.code, "write_key_reused_differently")
        self.assertNotIn("f9", self.store.state["ids"])

    def test_two_identities_one_path_is_retained_with_bytes(self):
        self.write("van", 1, "unlink", 1)
        # f1 撤下后 van 建 f2 占住原路径
        self.write("van", 2, "create", 2, id="f2", path="a.dat", data=b"NEW")
        # foot 在断网期间也向同路径建身份 f3
        rec = self.write("foot", 1, "create", 3, id="f3", path="a.dat", data=b"OTHER")
        self.assertEqual(rec["state"], "retained")
        self.assertEqual(rec["reason"], "path_taken")
        # 待认领的真实字节仍在中心
        self.assertEqual(self.store._get_blob(rec["blob"]), b"OTHER")

    # ----------------------------------------------------------- 幂等
    def test_duplicate_submission_same_local_write(self):
        first = self.write("van", 5, "edit", 2, offset=0, data=b"QQ")
        again = self.write("van", 5, "edit", 2, offset=0, data=b"QQ")
        self.assertEqual(again["echo"], "duplicate")
        self.assertEqual(again["blob"], first["blob"])
        self.assertEqual(len(self.store.state["writes"]), 1)

    def test_same_seq_different_request_is_refused_not_rerouted(self):
        self.write("van", 7, "edit", 2, offset=0, data=b"QQ")
        with self.assertRaises(CentralError) as ctx:
            # 同名恢复请求想刷进另一个文件身份
            self.store.ingest({"client": "van", "seq": 7, "action": "create",
                               "id": "f9", "path": "x", "at": 3,
                               "sha256": digest(b"QQ"), "data": b"QQ"})
        self.assertEqual(ctx.exception.code, "write_key_reused_differently")
        self.assertNotIn("f9", self.store.state["ids"])

    # ----------------------------------------------------------- 合并与冲突
    def test_disjoint_and_equal_overlap_merge_to_one_version(self):
        self.write("van", 1, "edit", 2, offset=0, data=b"AB")
        rec = self.write("foot", 1, "edit", 3, offset=2, data=b"CD")
        self.assertEqual(rec["state"], "integrated")
        meta = self.store.file_status("f1")
        self.assertGreater(meta["current_version"], 1)
        top = str(meta["current_version"])
        self.assertEqual(self.store._get_blob(meta["versions"][top]["sha256"]),
                         b"ABCD456789")
        # 同区间同字节再来一笔，拼装结果不变，不产生新版本
        before = meta["current_version"]
        rec2 = self.write("foot", 2, "edit", 4, offset=0, data=b"AB")
        self.assertEqual(rec2["state"], "integrated")
        self.assertEqual(self.store.file_status("f1")["current_version"], before)
        # 字节虽未变，第二笔写入的来源必须并入当前版本账本（同一份结果两个出处）
        meta = self.store.file_status("f1")
        sources = {tuple(src) for src in meta["versions"][str(before)]["sources"]}
        self.assertIn(("foot", 2), sources)

    def test_overlap_conflict_preserves_both_bytes_and_excludes_auto_merge(self):
        self.write("van", 1, "edit", 2, offset=3, data=b"XX")
        rec = self.write("foot", 1, "edit", 3, offset=4, data=b"YY")
        self.assertEqual(rec["state"], "retained")
        self.assertEqual(rec["reason"], "overlap")
        conflict = self.store.conflicts()[0]
        self.assertTrue(conflict["open"])
        self.assertEqual(set(conflict["members"]), {"van:1", "foot:1"})
        meta = self.store.file_status("f1")
        # 公共版本只含 van 的干净写入，foot 字节未被冒充合并
        self.assertEqual(self.store._get_blob(meta["versions"][str(
            meta["current_version"])]["sha256"]), b"012XX56789")
        self.assertEqual(self.store._get_blob(rec["blob"]), b"YY")

    def test_resolve_accept_and_discard(self):
        self.write("van", 1, "edit", 2, offset=3, data=b"XX")
        foot = self.write("foot", 1, "edit", 3, offset=4, data=b"YY")
        # 冲突中 van 的写入已在公共结果，确认 accept；foot 明确丢弃
        self.store.resolve("van", 1, "accept", 5)
        self.store.resolve("foot", 1, "discard", 6)
        self.assertEqual(self.store.conflicts(), [])
        self.assertEqual(self.store.state["writes"]["foot:1"]["resolution"], "discard")
        self.assertTrue(Path(self.store.blobs_dir, foot["blob"]).exists())  # gc 前仍在
        self.store.gc()
        self.assertFalse(Path(self.store.blobs_dir, foot["blob"]).exists())

    def test_cannot_accept_two_mutually_exclusive_writes(self):
        self.write("van", 1, "edit", 2, offset=3, data=b"XX")
        self.write("foot", 1, "edit", 3, offset=4, data=b"YY")
        # van 已隐式接受于公共结果；与之互斥的 foot 不能再被 accept
        with self.assertRaises(CentralError) as ctx:
            self.store.resolve("foot", 1, "accept", 6)
        self.assertEqual(ctx.exception.code, "conflict_still_open")

    def test_truncate_that_cuts_other_write_is_retained(self):
        self.write("foot", 1, "edit", 2, offset=8, data=b"ZZ")  # [8,10)
        rec = self.write("van", 1, "truncate", 3, **{"size": 6})
        self.assertEqual(rec["state"], "retained")
        self.assertEqual(rec["reason"], "truncated_other_write")
        meta = self.store.file_status("f1")
        # 公共长度保持 10，foot 的改写仍在
        self.assertEqual(meta["versions"][str(meta["current_version"])]["size"], 10)

    def test_accept_truncate_removes_cut_published_write(self):
        self.write("foot", 1, "edit", 2, offset=8, data=b"ZZ")  # 已发布
        self.write("van", 1, "truncate", 3, **{"size": 6})       # 冲突留存
        # 接受截断：被切掉的 foot 写入随之撤销
        self.store.resolve("van", 1, "accept", 4)
        self.assertEqual(self.store.conflicts(), [])
        meta = self.store.file_status("f1")
        top = str(meta["current_version"])
        self.assertEqual(self.store._get_blob(meta["versions"][top]["sha256"]),
                         b"012345")

    def test_discard_truncate_keeps_other_write(self):
        self.write("foot", 1, "edit", 2, offset=8, data=b"ZZ")
        self.write("van", 1, "truncate", 3, **{"size": 6})
        self.store.resolve("van", 1, "discard", 4)
        self.store.resolve("foot", 1, "accept", 5)
        meta = self.store.file_status("f1")
        top = str(meta["current_version"])
        self.assertEqual(self.store._get_blob(meta["versions"][top]["sha256"]),
                         b"01234567ZZ")

    def test_unlink_then_other_write_is_unlinked_while_writing(self):
        self.write("van", 1, "unlink", 1)
        rec = self.write("foot", 1, "edit", 2, offset=0, data=b"Z")
        self.assertEqual(rec["state"], "retained")
        self.assertEqual(rec["reason"], "unlinked_while_writing")
        self.assertEqual(self.store._get_blob(rec["blob"]), b"Z")
        self.assertNotIn("f1", self.store.directory().values())
        # 接受后身份产生新版本，可按身份读取（无目录项）
        self.store.resolve("foot", 1, "accept", 5)
        meta = self.store.file_status("f1")
        self.assertTrue(meta["tombstoned"])
        self.assertEqual(self.store._get_blob(
            meta["versions"][str(meta["current_version"])]["sha256"]),
            b"Z123456789")

    # ----------------------------------------------------------- 租约
    def test_expired_lease_write_is_retained_never_merged(self):
        rec = self.write("foot", 1, "edit", 11, offset=0, data=b"Z")
        self.assertEqual(rec["state"], "retained")
        self.assertEqual(rec["reason"], "lease_expired")
        meta = self.store.file_status("f1")
        self.assertEqual(meta["current_version"], 1)  # 没有新版本
        self.assertEqual(self.store._get_blob(rec["blob"]), b"Z")

    def test_revocation_stops_central_commitment_but_keeps_bytes(self):
        self.store.revoke_lease("foot", 5)
        rec = self.write("foot", 1, "edit", 6, offset=0, data=b"Z")
        self.assertEqual(rec["reason"], "lease_revoked")
        self.assertEqual(self.store._get_blob(rec["blob"]), b"Z")
        self.assertEqual(self.store.file_status("f1")["current_version"], 1)

    # ----------------------------------------------------------- 句柄
    def test_handle_pinned_across_rename_merge_and_unlink(self):
        self.store.open_handle("h1", "f1", until=99, at=1)
        self.write("van", 1, "rename", 2, path="b.dat")
        self.write("van", 2, "edit", 3, offset=0, data=b"AB")
        reply = self.store.read_handle("h1", 2, 4, at=50)
        self.assertEqual(reply["version"], 1)
        self.assertEqual(reply["data_base64"], __import__("base64").b64encode(
            b"2345").decode())
        # 新句柄跟随新版本
        self.store.open_handle("h2", "f1", until=99, at=4)
        reply2 = self.store.read_handle("h2", 0, 4, at=50)
        self.assertEqual(reply2["version"], 2)
        self.assertEqual(__import__("base64").b64decode(reply2["data_base64"]), b"AB23")

    def test_expired_handle_rejected(self):
        self.store.open_handle("h1", "f1", until=10, at=1)
        with self.assertRaises(CentralError) as ctx:
            self.store.read_handle("h1", 0, 1, at=10)
        self.assertEqual(ctx.exception.code, "handle_expired")

    def test_gc_keeps_blobs_pinned_by_handle(self):
        self.store.open_handle("h1", "f1", until=99, at=1)
        self.write("van", 1, "edit", 2, offset=0, data=b"AB")  # 产生 v2
        result = self.store.gc()
        meta = self.store.file_status("f1")
        v1_blob = meta["versions"]["1"]["sha256"]
        self.assertNotIn(v1_blob, result["reclaimed"])  # h1 钉住 v1
        self.store.read_handle("h1", 0, 2, at=3)       # 仍可续读

    def test_gc_preserves_integrated_payloads_for_later_conflicts(self):
        # 已接入写入的载荷必须保留：并发未发布分支仍要据此做冲突判定
        self.write("van", 1, "edit", 2, offset=0, data=b"AB")
        self.store.gc()  # 不能把 van 的载荷回收掉
        # 与 van 同基线分叉、尚未发布的并发写入在重叠区间给不同字节 -> 冲突
        late = self.write("foot", 1, "edit", 3, offset=0, data=b"AC")
        self.assertEqual(late["state"], "retained")
        self.assertEqual(late["reason"], "overlap")
        # 仅 discard 的载荷可被回收
        self.store.resolve("foot", 1, "discard", 5)
        reclaimed = self.store.gc()["reclaimed"]
        self.assertIn(late["blob"], reclaimed)
        # 身份起点始终保留
        origin = self.store.state["ids"]["f1"]["origin"]
        self.assertNotIn(origin, reclaimed)

    def test_write_over_published_range_is_normal_evolution_not_conflict(self):
        # van 先把 [0,2) 发布到公共快照（产生 v2）
        self.write("van", 1, "edit", 2, offset=0, data=b"AB")
        new_version = self.store.file_status("f1")["current_version"]
        # 后来者基于已含 AB 的新基线签出，在同一区间改写：是演进，不是冲突
        self.store.issue_lease("late", base=new_version, until=30, at=3)
        late = self.write("late", 1, "edit", 5, offset=0, data=b"AC")
        self.assertEqual(late["state"], "integrated")
        meta = self.store.file_status("f1")
        top = str(meta["current_version"])
        self.assertEqual(self.store._get_blob(meta["versions"][top]["sha256"]),
                         b"AC23456789")
        self.assertEqual(self.store.conflicts(), [])

    # ----------------------------------------------------------- 容量
    def test_central_capacity_blocks_new_blob(self):
        small = CentralStore(self.root / "small", central_capacity=12)
        small.seed_baseline([{"id": "f1", "path": "a", "file": "b"}], {"b": b"0123456789"})
        small.issue_lease("c", 1, 20, 0)
        with self.assertRaises(CentralError) as ctx:
            small.ingest({"client": "c", "seq": 1, "action": "edit", "id": "f1",
                          "at": 1, "sha256": digest(b"ABC"), "data": b"ABC"})
        self.assertEqual(ctx.exception.code, "quota_exhausted")
        # 失败不留残骸
        self.assertNotIn(digest(b"ABC"), [p.name for p in small.blobs_dir.iterdir()])

    # ----------------------------------------------------------- 重启
    def test_restart_restores_everything(self):
        self.store.open_handle("h1", "f1", until=99, at=1)
        self.write("van", 1, "edit", 2, offset=3, data=b"XX")
        self.write("foot", 1, "edit", 3, offset=4, data=b"YY")  # 冲突留存
        revived = CentralStore(self.root / "central", central_capacity=100)
        self.assertEqual(revived.directory(), self.store.directory())
        self.assertEqual(len(revived.conflicts()), 1)
        reply = revived.read_handle("h1", 0, 3, at=4)
        self.assertEqual(__import__("base64").b64decode(reply["data_base64"]), b"012")
        self.assertEqual(revived.state["writes"]["foot:1"]["state"], "retained")
        # 重启后裁决仍有依据
        revived.resolve("foot", 1, "discard", 9)
        self.assertEqual(revived.conflicts(), [])


class EdgeQuotaTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = CentralStore(self.root / "central", central_capacity=200)
        self.store.seed_baseline([{"id": "f1", "path": "a.dat", "file": "b"}],
                                 {"b": b"01234567"})  # 8 字节基线
        self.store.issue_lease("dev", 1, 30, 0)

    def tearDown(self):
        self.tmp.cleanup()

    def test_edge_capacity_then_release_after_durable_ingest(self):
        vol = EdgeVolume(self.root / "vol", "dev", edge_capacity=10,
                         central=self.store, objects={})
        vol.checkout(1, 30, 0)
        self.assertEqual(vol.remaining(), 2)  # 10 - 8 基线
        with self.assertRaises(EdgeError) as ctx:
            vol.write("edit", 1, id="f1", offset=8, data=b"ABC")  # 需要 3
        self.assertEqual(ctx.exception.code, "edge_quota_exhausted")
        receipt = vol.write("edit", 1, id="f1", offset=8, data=b"AB")  # 恰好 2
        self.assertEqual(receipt["remaining_after"], 0)
        # 持久回执落盘
        on_disk = EdgeVolume(self.root / "vol", "dev", 10, self.store)
        self.assertEqual(on_disk.remaining(), 0)
        self.assertEqual(len(on_disk.pending_writes()), 1)
        # 接入后中心持久，本地释放，额度归还
        result = vol.integrate(2)
        self.assertEqual(len(result["integrated"]), 1)
        self.assertEqual(vol.remaining(), 2)
        self.assertEqual(vol.pending_writes(), [])

    def test_retained_write_also_frees_local_quota(self):
        self.store.issue_lease("other", 1, 30, 0)
        vol = EdgeVolume(self.root / "vol", "dev", edge_capacity=12,
                         central=self.store, objects={})
        vol.checkout(1, 30, 0)  # 剩 4
        other = EdgeVolume(self.root / "vol2", "other", 12, self.store)
        other.checkout(1, 30, 0)
        other.write("edit", 1, id="f1", offset=3, data=b"ZZ")
        other.integrate(1)
        vol.write("edit", 1, id="f1", offset=4, data=b"YY")  # 与 other 冲突
        result = vol.integrate(2)
        self.assertEqual(len(result["retained"]), 1)
        self.assertEqual(vol.remaining(), 4)  # 留存字节中心已持久，本地占量释放


if __name__ == "__main__":
    unittest.main()
