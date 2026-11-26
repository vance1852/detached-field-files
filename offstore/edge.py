"""本地写入端（离线卷）。

每个巡堤班组持有一个 :class:`EdgeVolume`，目录即持久状态：

    volume/state.json   租约、基线、写入流水与本地回执（原子落盘）
    volume/local/<sha>  尚未确认被中心可靠持久的写入载荷与基线副本

回执生命周期与占量释放规则：

* ``write`` 成功即生成 ``local`` 持久回执并计入占用；额度不足直接拒绝；
* ``integrate`` 把待传流水接入中心；无论结果是 ``integrated`` 还是
  ``retained``，真实字节都已在中心持久，本地载荷即可释放并归还额度；
* 重复接入同一 ``(client, seq)`` 仍返回同一回执（中心幂等，本地不重复计费）；
* 断网（transport_failed）时状态保持 ``local``，重启后仍可重放。
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path

from .core import merge_branches, sha256_hex, validate_relpath
from .store import _atomic_write


class EdgeError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class EdgeVolume:
    def __init__(self, state_dir: str | os.PathLike, client: str, edge_capacity: int,
                 central, objects: dict[str, bytes] | None = None):
        self.dir = Path(state_dir)
        self.local_dir = self.dir / "local"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.local_dir.mkdir(exist_ok=True)
        self.central = central
        self.objects = dict(objects or {})
        snapshot = self.dir / "state.json"
        if snapshot.exists():
            self.state = json.loads(snapshot.read_text(encoding="utf-8"))
            if self.state["client"] != client or self.state["edge_capacity"] != edge_capacity:
                raise EdgeError("volume_identity_mismatch",
                                f"{self.dir} belongs to {self.state['client']}")
        else:
            self.state = {"client": client, "edge_capacity": edge_capacity,
                          "lease": None, "baseline": {}, "writes": [],
                          "receipts": {}, "usage": 0}
            self._persist()

    # ------------------------------------------------------------ 持久化
    def _persist(self) -> None:
        _atomic_write(self.dir / "state.json",
                      json.dumps(self.state, ensure_ascii=False, indent=1).encode("utf-8"))

    def _store_local(self, data: bytes) -> str:
        digest = sha256_hex(data)
        target = self.local_dir / digest
        if not target.exists():
            _atomic_write(target, data)
        return digest

    def _delete_local(self, digest: str) -> None:
        target = self.local_dir / digest
        if target.exists():
            target.unlink()

    # ------------------------------------------------------------ 容量
    def remaining(self) -> int:
        return max(0, self.state["edge_capacity"] - self.state["usage"])

    def _charge(self, amount: int) -> None:
        if self.state["usage"] + amount > self.state["edge_capacity"]:
            raise EdgeError(
                "edge_quota_exhausted",
                f"{self.state['client']} capacity {self.state['edge_capacity']} "
                f"would be exceeded: used={self.state['usage']} need={amount} "
                f"remaining={self.remaining()}")
        self.state["usage"] += amount

    # ------------------------------------------------------------ 签发
    def checkout(self, base: int, until: int, at: int) -> dict:
        """从公共目录签发离线卷：取走基线字节并登记离线租约。"""
        lease = self.central.issue_lease(self.state["client"], base, until, at)
        self.state["lease"] = lease
        for path, file_id in self.central.directory().items():
            meta = self.central.file_status(file_id)
            version = str(meta["current_version"])
            blob = meta["versions"][version]["sha256"]
            data = self._baseline_bytes(meta, blob)
            digest = self._store_local(data)
            self.state["baseline"][file_id] = {
                "path": path, "blob": digest, "size": len(data),
                "base_version": meta["current_version"], "tombstoned": False,
            }
            self._charge(len(data))
        self._persist()
        return {"lease": lease, "baseline": dict(self.state["baseline"]),
                "remaining": self.remaining()}

    def _baseline_bytes(self, meta: dict, blob: str) -> bytes:
        # 优先用资料端字节目录，其次向中心句柄式读取兜底。
        for data in self.objects.values():
            if sha256_hex(data) == blob:
                return data
        return self.central._get_blob(blob)  # 进程内中心可直接取快照字节

    # ------------------------------------------------------------ 本地写入
    def _next_seq(self) -> int:
        return max((w["seq"] for w in self.state["writes"]), default=0) + 1

    def write(self, action: str, at: int, **fields) -> dict:
        """在离线卷上执行一次本地写入并立即给出持久回执。"""
        client = self.state["client"]
        seq = fields.pop("seq", None) or self._next_seq()
        key = f"{client}:{seq}"
        if any(w["seq"] == seq for w in self.state["writes"]):
            raise EdgeError("duplicate_local_seq", f"seq {seq} already used")

        data = bytes(fields.pop("data", b"") or b"")
        entry = {"client": client, "seq": seq, "action": action, "at": at,
                 "id": fields.get("id"), "status": "local", "central": None,
                 "bytes": len(data), "sha256": sha256_hex(data)}
        if action == "edit":
            entry["offset"] = fields["offset"]
        if action == "truncate":
            entry["size"] = fields["size"]
        if action in ("rename", "create"):
            validate_relpath(fields["path"])
            entry["path"] = fields["path"]
        if action == "create" and not data:
            raise EdgeError("empty_create", "create requires payload bytes")

        if action in ("edit", "create"):
            self._charge(len(data))  # 先判剩余额度：不够就拒绝，不留半条记录
            digest = self._store_local(data)
            entry["local_blob"] = digest
        self.state["writes"].append(entry)
        receipt = dict(entry)
        receipt["receipt"] = "local"
        receipt["remaining_after"] = self.remaining()
        self.state["receipts"][key] = receipt
        self._persist()
        return dict(receipt)

    # ------------------------------------------------------------ 接入
    def integrate(self, at: int) -> dict:
        """把所有尚未可靠接入的本地写入重放到中心，返回逐条归属与冲突视图。"""
        results = []
        for entry in self.state["writes"]:
            if entry["status"] != "local":
                continue
            payload = b""
            if "local_blob" in entry:
                payload = (self.local_dir / entry["local_blob"]).read_bytes()
            request = {"client": entry["client"], "seq": entry["seq"],
                       "action": entry["action"], "id": entry["id"], "at": at,
                       "sha256": entry["sha256"],
                       "data_base64": base64.b64encode(payload).decode()}
            if "offset" in entry:
                request["offset"] = entry["offset"]
            if "size" in entry:
                request["size"] = entry["size"]
            if "path" in entry:
                request["path"] = entry["path"]
            try:
                central_record = self.central.ingest(request)
            except Exception as exc:  # 断网/服务不可用：回执保持 local，下次重放
                entry["status"] = "local"
                entry["transport_error"] = str(exc)
                results.append({"key": f"{entry['client']}:{entry['seq']}",
                                "status": "local", "error": str(exc)})
                self._persist()
                continue

            entry["transport_error"] = None
            entry["central"] = {k: central_record.get(k)
                                for k in ("state", "reason", "resolution", "commit")}
            entry["status"] = central_record["state"]
            if central_record.get("echo") == "duplicate":
                entry["central"]["echo"] = "duplicate"
            # 中心已持久真实字节（含留存冲突），本地载荷可释放并归还占量。
            if "local_blob" in entry:
                self._delete_local(entry["local_blob"])
                self.state["usage"] -= entry["bytes"]
                del entry["local_blob"]
            receipt = dict(entry)
            receipt["receipt"] = central_record["state"]
            self.state["receipts"][f"{entry['client']}:{entry['seq']}"] = receipt
            results.append(dict(receipt))
        self._persist()
        view_error = None
        try:
            open_conflicts = self.central.conflicts()
            directory = self.central.directory()
        except Exception as exc:  # 断网时写入已逐条落账，视图查询可稍后再取
            open_conflicts = []
            directory = {}
            view_error = str(exc)
        return {
            "client": self.state["client"],
            "integrated": [r for r in results if r.get("status") == "integrated"],
            "retained": [r for r in results if r.get("status") == "retained"],
            "rejected": [r for r in results if r.get("status") == "rejected"],
            "pending_local": [r for r in results if r.get("status") == "local"],
            "view_error": view_error,
            "remaining": self.remaining(),
            "open_conflicts": open_conflicts,
            "directory": directory,
        }

    # ------------------------------------------------------------ 查询/本地读
    def receipts(self) -> dict:
        return dict(self.state["receipts"])

    def pending_writes(self) -> list[dict]:
        return [dict(w) for w in self.state["writes"] if w["status"] == "local"]

    def local_view(self) -> dict:
        """本卷当前可见的身份→路径视图（跟随本地改名，撤下的标记墓碑）。"""
        view = {file_id: dict(meta) for file_id, meta in self.state["baseline"].items()}
        for write in self.state["writes"]:
            file_id = write["id"]
            if write["action"] == "rename" and file_id in view:
                view[file_id]["path"] = write["path"]
            if write["action"] == "unlink" and file_id in view:
                view[file_id]["tombstoned"] = True
            if write["action"] == "create":
                view.setdefault(file_id, {"path": write["path"], "blob": None,
                                          "size": 0, "base_version": 0,
                                          "tombstoned": False})
        return view

    def _local_or_central(self, digest: str) -> bytes:
        target = self.local_dir / digest
        if target.exists():
            return target.read_bytes()
        return self.central._get_blob(digest)

    def read_local(self, file_id: str) -> bytes:
        """按文件身份读取本卷本地字节：基线副本叠加尚未接入的区间写入。

        已确认接入并释放了本地副本的内容，回退到中心的公共版本读取。
        """
        base_meta = self.state["baseline"].get(file_id)
        if base_meta is None:
            creates = [w for w in self.state["writes"]
                       if w["action"] == "create" and w["id"] == file_id]
            if not creates:
                raise EdgeError("unknown_identity", file_id)
            create = creates[-1]
            if "local_blob" in create:
                data = (self.local_dir / create["local_blob"]).read_bytes()
            else:
                meta = self.central.file_status(file_id)
                version = str(meta["current_version"])
                data = self.central._get_blob(meta["versions"][version]["sha256"])
        else:
            data = self._local_or_central(base_meta["blob"])
        branches: dict[str, list[dict]] = {}
        for write in self.state["writes"]:
            if write["id"] != file_id or write["status"] != "local":
                continue
            if write["action"] == "edit":
                op = {"client": self.state["client"], "seq": write["seq"],
                      "action": "edit", "offset": write["offset"],
                      "data": (self.local_dir / write["local_blob"]).read_bytes()}
                branches.setdefault(self.state["client"], []).append(op)
            elif write["action"] == "truncate":
                op = {"client": self.state["client"], "seq": write["seq"],
                      "action": "truncate", "size": write["size"]}
                branches.setdefault(self.state["client"], []).append(op)
        if branches:
            result = merge_branches(data, branches)
            return bytes(result.data)
        return data

    def readable(self) -> list[dict]:
        """查询本卷各自可读的内容、路径冲突及已接入的真实数据。"""
        out = []
        central_dir = self.central.directory()
        for file_id, meta in self.local_view().items():
            item = {"id": file_id, "local_path": meta["path"],
                    "tombstoned": meta["tombstoned"],
                    "central_path": next((p for p, owner in central_dir.items()
                                          if owner == file_id), None)}
            try:
                item["local_bytes"] = len(self.read_local(file_id))
            except EdgeError:
                item["local_bytes"] = None
            out.append(item)
        return out
