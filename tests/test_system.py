"""HTTP 后端、断网重放、多卷轮流接回与双端重启的系统测试。"""

import tempfile
import threading
import unittest
from pathlib import Path

from offstore.edge import EdgeVolume
from offstore.replay import load_fixture
from offstore.server import CentralHTTPServer, RemoteCentral, build_from_fixture
from offstore.store import CentralStore

ROOT = Path(__file__).resolve().parent.parent
FIXTURE = ROOT / "fixtures" / "rename-crosses-range-edits"


class HttpSystemTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        central, self.objects = build_from_fixture(self.root / "central", FIXTURE)
        self.server = CentralHTTPServer(("127.0.0.1", 0), central)
        self.worker = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.02}, daemon=True)
        self.worker.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.worker.join(timeout=3)
        self.server.server_close()
        self.tmp.cleanup()

    def test_full_flow_over_http_with_revocation_and_gc(self):
        remote = RemoteCentral(self.url)
        record, _ = load_fixture(FIXTURE)
        policy = record["policy"]

        van = EdgeVolume(self.root / "vol-van", "van",
                         policy["edge_capacity"]["van"], remote, self.objects)
        van.checkout(5, 31, 0)
        foot = EdgeVolume(self.root / "vol-foot", "foot",
                          policy["edge_capacity"]["foot"], remote, self.objects)
        foot.checkout(5, 22, 0)

        # 指挥员在重连前取得读句柄
        remote.open_handle("r5", "x7", 32, 1)

        van.write("rename", 2, seq=1, id="x7", path="banks/east-old.dat")
        van.write("edit", 3, seq=2, id="x7", offset=5,
                  data=self.objects["patch-a.bin"])
        foot.write("edit", 4, seq=1, id="x7", offset=10,
                   data=self.objects["patch-b.bin"])
        van.integrate(5)
        van.write("create", 6, seq=3, id="x8", path="banks/east.dat",
                  data=self.objects["replacement.bin"])
        van.integrate(7)
        foot.integrate(8)

        directory = remote.directory()
        self.assertEqual(directory,
                         {"banks/east-old.dat": "x7", "banks/east.dat": "x8"})

        # 收走过期编辑许可：之后 foot 的写入只能留存，不能冒充合并
        remote.revoke_lease("foot", 9)
        foot.write("edit", 11, seq=2, id="x7", offset=11,
                   data=self.objects["late.bin"])
        result = foot.integrate(12)
        self.assertEqual(result["retained"][0]["central"]["reason"], "lease_revoked")

        # 旧句柄按原字节范围续读，与之后的改名/合并无关
        reply = remote.read_handle("r5", 8, 5, 24)
        import base64
        self.assertEqual(base64.b64decode(reply["data_base64"]), b"-leve")

        # 公共入库的载荷可被中心回收，句柄钉住的快照保留
        gc = remote.gc()
        self.assertIsInstance(gc["reclaimed"], list)
        reply_again = remote.read_handle("r5", 0, 3, 25)
        self.assertEqual(base64.b64decode(reply_again["data_base64"]), b"eas")

    def test_offline_then_reconnect_replays_pending_writes_once(self):
        # 先不启动网络：本地卷照常编辑，接入失败时回执保持 local
        dead = RemoteCentral("http://127.0.0.1:1", timeout=0.5)
        van = EdgeVolume(self.root / "vol-van2", "van", 64, dead, self.objects)
        with self.assertRaises(ConnectionError):
            van.checkout(5, 31, 0)
        # 离线状态直接在已签发租约的中心（同身份）上构造卷继续业务
        central = self.server.central
        van = EdgeVolume(self.root / "vol-van2", "van", 64, central, self.objects)
        van.checkout(5, 31, 0)
        van.write("edit", 2, seq=1, id="x7", offset=0,
                  data=self.objects["patch-a.bin"])
        # 模拟断网接入
        van.central = RemoteCentral("http://127.0.0.1:1", timeout=0.5)
        failed = van.integrate(3)
        self.assertEqual(len(failed["pending_local"]), 1)
        self.assertEqual(van.pending_writes()[0]["status"], "local")
        # 重启本地端进程：待处理写入仍有依据
        revived = EdgeVolume(self.root / "vol-van2", "van", 64,
                             RemoteCentral(self.url), self.objects)
        self.assertEqual(len(revived.pending_writes()), 1)
        ok = revived.integrate(4)
        self.assertEqual(len(ok["integrated"]), 1)
        # 重复接入同一笔不会产生第二条中心记录
        again = revived.integrate(5)
        self.assertEqual(again["integrated"], [])
        self.assertEqual(
            sum(1 for w in central.state["writes"].values()
                if w["client"] == "van"), 1)


class MultiVolumeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.record, self.objects = load_fixture(FIXTURE)
        self.central = CentralStore(self.root / "central",
                                    self.record["policy"]["central_capacity"])
        self.central.seed_baseline(self.record["policy"]["entries"], self.objects)

    def tearDown(self):
        self.tmp.cleanup()

    def test_two_volumes_from_same_public_directory_round_robin(self):
        cap = self.record["policy"]["edge_capacity"]
        first = EdgeVolume(self.root / "a", "van", cap["van"],
                           self.central, self.objects)
        first.checkout(1, 40, 0)
        first.write("edit", 1, seq=1, id="x7", offset=0,
                    data=self.objects["patch-a.bin"])
        first.integrate(2)

        # 第二张离线卷从同一公共目录取基线，应看到第一次接入的真实数据
        second = EdgeVolume(self.root / "b", "foot", cap["foot"],
                            self.central, self.objects)
        second.checkout(2, 40, 3)
        meta = self.central.file_status("x7")
        baseline = second.state["baseline"]["x7"]
        self.assertEqual(baseline["base_version"], meta["current_version"])

        second.write("edit", 4, seq=1, id="x7", offset=12,
                     data=self.objects["late.bin"])
        second.integrate(5)
        # 再轮流接回第一张卷：查询得到路径冲突视图与已接入数据
        view = first.readable()
        ids = {item["id"] for item in view}
        self.assertIn("x7", ids)
        self.assertEqual(first.central.directory(), self.central.directory())

    def test_restart_keeps_commitments_and_pending_conflicts(self):
        cap = self.record["policy"]["edge_capacity"]
        van = EdgeVolume(self.root / "v", "van", cap["van"],
                         self.central, self.objects)
        van.checkout(1, 40, 0)
        foot = EdgeVolume(self.root / "f", "foot", cap["foot"],
                          self.central, self.objects)
        foot.checkout(1, 40, 0)
        self.central.open_handle("r9", "x7", 99, 1)
        van.write("edit", 2, seq=1, id="x7", offset=3,
                  data=self.objects["patch-a.bin"])
        foot.write("edit", 2, seq=1, id="x7", offset=4,
                   data=self.objects["patch-b.bin"])
        van.integrate(3)
        foot.integrate(4)  # 相交冲突，留存待认领
        self.assertEqual(len(self.central.conflicts()), 1)

        # 中心与本地端同时重启
        restarted_central = CentralStore(self.root / "central",
                                         self.record["policy"]["central_capacity"])
        self.assertEqual(len(restarted_central.conflicts()), 1)
        import base64
        reply = restarted_central.read_handle("r9", 0, 4, 5)
        # 句柄钉在基线 v1（base.bin 以 east 开头），冲突字节不影响旧句柄
        self.assertEqual(base64.b64decode(reply["data_base64"]), b"east")
        self.assertEqual(reply["length"], 4)
        restarted_van = EdgeVolume(self.root / "v", "van", cap["van"],
                                   restarted_central, self.objects)
        self.assertEqual(restarted_van.pending_writes(), [])
        self.assertIn("van:1", restarted_van.receipts())
        # 冲突仍可被处理
        restarted_central.resolve("foot", 1, "discard", 6)
        self.assertEqual(restarted_central.conflicts(), [])


if __name__ == "__main__":
    unittest.main()
