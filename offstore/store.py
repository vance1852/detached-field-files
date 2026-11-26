"""中心接合服务：文件身份、公共版本、冲突留存、读句柄与中心容量。

持久化布局（``state_dir``）::

    state.json    每次变更后原子落盘的完整快照（fsync）
    blobs/<sha>   内容寻址的实际字节（基线、写入载荷、版本快照）
    journal.jsonl 仅追加的审计流水（接入/留存/裁决/句柄）

关键不变量：
* 文件身份 ``id`` 不可变；rename 只改路径绑定，create 必须换新身份；
* 所有写入先落 blob 再改状态，崩溃后要么完整可见要么完全不见；
* 冲突写入的真实字节始终保留，自动合并不触及它们，等待 accept/discard；
* 读句柄钉住签发时刻的版本号，改名、撤下、合并后续读都返回同一字节范围。
"""

from __future__ import annotations

import base64
import json
import os
import threading
from pathlib import Path

from .core import (
    DUPLICATE,
    INTEGRATED,
    LEASE_EXPIRED,
    OVERLAP,
    PATH_TAKEN,
    REJECTED,
    RETAINED,
    TRUNCATED_OTHER_WRITE,
    UNLINKED_WHILE_WRITING,
    lease_active,
    merge_branches,
    sha256_hex,
    validate_relpath,
)

LEASE_REVOKED = "lease_revoked"
IDENTITY_EXISTS = "identity_exists"
RENAME_DIVERGED = "rename_diverged"


class CentralError(Exception):
    """协议级错误（未知身份、容量耗尽、重复键内容不一致等）。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class QuotaExhausted(CentralError):
    def __init__(self, message: str):
        super().__init__("quota_exhausted", message)


def _atomic_write(path: Path, data: bytes) -> None:
    # 临时名必须唯一：内容寻址目录下多个写入并发时，固定 .tmp 会互相替换，
    # 使最终文件名（sha）与内容不一致。
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{id(data)}.tmp")
    with open(tmp, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    parent_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)


class CentralStore:
    """线程安全的中心接合服务，所有变更立即持久化。"""

    def __init__(self, state_dir: str | os.PathLike, central_capacity: int):
        self.dir = Path(state_dir)
        self.blobs_dir = self.dir / "blobs"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.blobs_dir.mkdir(exist_ok=True)
        self.lock = threading.RLock()
        snapshot = self.dir / "state.json"
        if snapshot.exists():
            self.state = json.loads(snapshot.read_text(encoding="utf-8"))
        else:
            self.state = {
                "central_capacity": central_capacity,
                "next_commit": 2,  # v1 保留给基线/新建身份的首个版本
                "ids": {},        # id -> 身份记录（版本链、路径绑定、墓碑）
                "paths": {},      # 当前公共路径 -> id
                "leases": {},     # client -> 离线租约
                "writes": {},     # "client:seq" -> 写入记录（含状态与裁决）
                "conflicts": [],  # 冲突清单（含已裁决历史）
                "handles": {},    # 句柄 -> (id, version, until)
            }
            self._persist()

    # ------------------------------------------------------------------ 底层
    def _persist(self) -> None:
        _atomic_write(self.dir / "state.json",
                      json.dumps(self.state, ensure_ascii=False, indent=1).encode("utf-8"))

    def _audit(self, event: dict) -> None:
        with open(self.dir / "journal.jsonl", "ab") as handle:
            handle.write(json.dumps(event, ensure_ascii=False).encode("utf-8") + b"\n")
            handle.flush()
            os.fsync(handle.fileno())

    def _put_blob(self, data: bytes) -> str:
        """落盘一个内容寻址字节；超中心容量时拒绝且不留残骸。"""
        digest = sha256_hex(data)
        target = self.blobs_dir / digest
        if target.exists():
            return digest
        if self.bytes_used() + len(data) > self.state["central_capacity"]:
            raise QuotaExhausted(
                f"central capacity {self.state['central_capacity']} exhausted; "
                f"need {len(data)} bytes, {self.bytes_used()} in use")
        _atomic_write(target, data)
        return digest

    def _get_blob(self, digest: str) -> bytes:
        return (self.blobs_dir / digest).read_bytes()

    def bytes_used(self) -> int:
        """中心副本当前占用（内容寻址，去重后求和）。"""
        return sum(path.stat().st_size for path in self.blobs_dir.iterdir()
                   if path.is_file() and not path.name.endswith(".tmp"))

    def capacity(self) -> dict:
        cap = self.state["central_capacity"]
        used = self.bytes_used()
        return {"capacity": cap, "used": used, "remaining": max(0, cap - used)}

    # ------------------------------------------------------------ 基线/租约
    def seed_baseline(self, entries: list[dict], objects: dict[str, bytes], at: int = 0) -> None:
        """用基线 ``entries`` 初始化公共目录（仅空库可调用）。"""
        with self.lock:
            if self.state["ids"]:
                raise CentralError("already_seeded", "baseline already installed")
            for entry in entries:
                data = objects[entry["file"]]
                digest = self._put_blob(data)
                self.state["ids"][entry["id"]] = {
                    "origin": digest,
                    "path": entry["path"],
                    "tombstoned": False,
                    "current_version": 1,
                    "versions": {"1": {
                        "blob": digest, "size": len(data), "committed_at": at,
                        "basis": 0, "sources": [], "kind": "baseline",
                    }},
                }
                self.state["paths"][entry["path"]] = entry["id"]
            self._persist()
            self._audit({"at": at, "event": "seed_baseline",
                         "entries": [e["id"] for e in entries]})

    def issue_lease(self, client: str, base: int, until: int, at: int) -> dict:
        with self.lock:
            lease = {"client": client, "base": base, "until": until,
                     "issued_at": at, "revoked": False}
            self.state["leases"][client] = lease
            self._persist()
            self._audit({"at": at, "event": "checkout", **lease})
            return dict(lease)

    def revoke_lease(self, client: str, at: int) -> dict:
        """收走编辑许可：之后该客户端的写入只能留存，不能再占中心承诺。"""
        with self.lock:
            lease = self.state["leases"].get(client)
            if lease is None:
                raise CentralError("unknown_client", f"no lease for {client}")
            lease["revoked"] = True
            self._persist()
            self._audit({"at": at, "event": "revoke", "client": client})
            return dict(lease)

    # --------------------------------------------------------------- 写入
    @staticmethod
    def _write_key(write: dict) -> str:
        return f"{write['client']}:{write['seq']}"

    def _record_conflict(self, file_id: str, reason: str, members: list[str], detail: str,
                         at: int) -> dict:
        for conflict in self.state["conflicts"]:
            if conflict["id"] == file_id and conflict["reason"] == reason \
                    and set(conflict["members"]) == set(members):
                return conflict
        # 已在公共结果中的成员视为隐式 accept；只有留存成员需要人工裁决。
        resolved = {}
        for member in members:
            record = self.state["writes"].get(member)
            if record is not None and record["state"] == INTEGRATED:
                resolved[member] = "accept"
        conflict = {"id": file_id, "reason": reason, "members": members,
                    "detail": detail, "open": True, "created_at": at,
                    "resolved_members": resolved}
        self.state["conflicts"].append(conflict)
        return conflict

    def _active_range_writes(self, file_id: str) -> dict[str, list[dict]]:
        """按客户端分组当前参与自动拼装的 edit/truncate 写入。

        已被 accept 的留存写入纳入拼装；被 discard 的排除；卷入仍开放冲突、
        尚未裁决的写入保持在公共结果之外，直到处理人明确表态。
        """
        conflicted_members: set[str] = set()
        for conflict in self.state["conflicts"]:
            if conflict["id"] != file_id or not conflict["open"]:
                continue
            for member, decision in conflict["resolved_members"].items():
                if decision == "accept":
                    continue  # 已明确接受的成员重新参与拼装
                conflicted_members.add(member)
        branches: dict[str, list[dict]] = {}
        for key, record in self.state["writes"].items():
            if record["id"] != file_id or record["action"] not in ("edit", "truncate"):
                continue
            if record.get("resolution") == "discard" or key in conflicted_members:
                continue
            op = {"client": record["client"], "seq": record["seq"],
                  "action": record["action"], "base": record.get("base")}
            if record["action"] == "edit":
                op["offset"] = record["offset"]
                op["data"] = self._get_blob(record["blob"])
            else:
                op["size"] = record["new_size"]
            branches.setdefault(record["client"], []).append(op)
        for ops in branches.values():
            ops.sort(key=lambda op: op["seq"])
        return branches

    def _rebuild(self, file_id: str, at: int, extra: dict | None = None) -> None:
        """在当前公共版本快照上增量应用有效区间写入，产出新的公共版本。

        活跃写入集合中的成员要么此前尚未发布，要么是刚被 accept 的留存写入；
        已经在当前版本中的写入不会重复作用（按 (client, seq) 去重）。
        ``extra`` 是尚未落入 writes 表、本次正在接入的新写入。
        """
        identity = self.state["ids"][file_id]
        current = identity["versions"][str(identity["current_version"])]
        already: set[tuple[str, int]] = {
            (source[0], source[1]) for source in current.get("sources", [])}
        branches = self._active_range_writes(file_id)
        if extra is not None:
            branches.setdefault(extra["client"], []).append(extra)
            branches[extra["client"]].sort(key=lambda op: op["seq"])
        fresh = {client: [op for op in ops
                          if (client, op["seq"]) not in already]
                 for client, ops in branches.items()}
        fresh = {client: ops for client, ops in fresh.items() if ops}
        if not fresh:
            return
        result = merge_branches(self._get_blob(current["blob"]), fresh)
        digest = sha256_hex(bytes(result.data))
        new_sources = sorted(set(already) | result.applied)
        if digest == current["blob"]:
            # 字节未变（典型：另一分支同区间同字节），不新建快照，
            # 但新接入的写入来源必须并入当前版本的账本。
            current["sources"] = [list(source) for source in new_sources]
            return
        self._put_blob(bytes(result.data))
        version = self.state["next_commit"]
        self.state["next_commit"] += 1
        identity["versions"][str(version)] = {
            "blob": digest, "size": result.size, "committed_at": at,
            "basis": identity["current_version"],
            "sources": [list(source) for source in new_sources], "kind": "merge",
        }
        identity["current_version"] = version

    def _rebuild_after_resolution(self, file_id: str, at: int) -> None:
        """裁决改变了有效写入集合后，从身份起点重算公共版本快照。"""
        identity = self.state["ids"][file_id]
        branches = self._active_range_writes(file_id)
        if not branches:
            return
        result = merge_branches(self._get_blob(identity["origin"]), branches)
        digest = sha256_hex(bytes(result.data))
        current = identity["versions"][str(identity["current_version"])]
        if digest == current["blob"]:
            current["sources"] = [list(s) for s in sorted(result.applied)]
            return
        self._put_blob(bytes(result.data))
        version = self.state["next_commit"]
        self.state["next_commit"] += 1
        identity["versions"][str(version)] = {
            "blob": digest, "size": result.size, "committed_at": at,
            "basis": identity["current_version"],
            "sources": [list(s) for s in sorted(result.applied)],
            "kind": "resolve",
        }
        identity["current_version"] = version

    def _mark_retained(self, record: dict, reason: str, detail: str, at: int) -> dict:
        record["state"] = RETAINED
        record["reason"] = reason
        members = [self._write_key(record)]
        self._record_conflict(record["id"], reason, members, detail, at)
        self._audit({"at": at, "event": "retained", "write": self._write_key(record),
                     "reason": reason, "detail": detail})
        return record

    def ingest(self, write: dict) -> dict:
        """接入一笔离线写入。重复 (client, seq) 永远返回同一条本地写入记录。"""
        with self.lock:
            return self._ingest_locked(write)

    def _ingest_locked(self, write: dict) -> dict:
        client = write["client"]
        seq = write["seq"]
        at = write["at"]
        key = self._write_key(write)
        action = write["action"]
        file_id = write["id"]

        # 幂等：同一 (client, seq) 必须仍是同一次本地写入。
        if key in self.state["writes"]:
            stored = self.state["writes"][key]
            same_request = (
                stored["id"] == file_id and stored["action"] == action
                and stored["sha256"] == write["sha256"]
            )
            if not same_request:
                raise CentralError(
                    "write_key_reused_differently",
                    f"{key} already identifies a different write; "
                    "a same-named recovery request cannot enter another file")
            echo = dict(stored)
            echo["echo"] = DUPLICATE
            return echo

        lease = self.state["leases"].get(client)
        expired = lease is None or lease["revoked"] or not lease_active(lease["until"], at)
        expired_reason = LEASE_REVOKED if (lease and lease["revoked"]) else LEASE_EXPIRED

        payload = write.get("data")
        if payload is None and write.get("data_base64") is not None:
            payload = base64.b64decode(write["data_base64"])
        payload = bytes(payload or b"")
        if sha256_hex(payload) != write["sha256"]:
            raise CentralError("digest_mismatch", f"payload sha256 mismatch for {key}")
        if action in ("rename",):
            validate_relpath(write["path"])
        if action == "create":
            validate_relpath(write["path"])

        digest = self._put_blob(payload)  # 实际字节先可靠落盘
        record = {
            "client": client, "seq": seq, "id": file_id, "action": action, "at": at,
            "blob": digest, "sha256": write["sha256"], "size": len(payload),
            "state": None, "reason": None, "resolution": None, "resolved_at": None,
            "commit": None, "base": lease["base"] if lease else None,
        }
        if action == "edit":
            record["offset"] = write["offset"]
        if action == "truncate":
            record["new_size"] = write["size"]
        if action in ("rename", "create"):
            record["path"] = write["path"]
        self.state["writes"][key] = record

        # 租约失效：字节持久留存，但绝不自动合并、不冒充已接入。
        if expired:
            self._mark_retained(record, expired_reason,
                                f"lease not active at t={at}", at)
            self._persist()
            return dict(record)

        identity = self.state["ids"].get(file_id)

        if action == "create":
            if identity is not None:
                record["state"] = REJECTED
                record["reason"] = IDENTITY_EXISTS
                self._audit({"at": at, "event": "rejected", "write": key,
                             "reason": IDENTITY_EXISTS})
                self._persist()
                return dict(record)
            if write["path"] in self.state["paths"]:
                self._mark_retained(
                    record, PATH_TAKEN,
                    f"path {write['path']!r} already bound to "
                    f"{self.state['paths'][write['path']]}", at)
                self._persist()
                return dict(record)
            record["state"] = INTEGRATED
            self.state["ids"][file_id] = {
                "origin": digest, "path": write["path"], "tombstoned": False,
                "current_version": 1,
                "versions": {"1": {"blob": digest, "size": len(payload),
                                   "committed_at": at, "basis": 0,
                                   "sources": [[client, seq]], "kind": "create"}},
            }
            self.state["paths"][write["path"]] = file_id
            record["commit"] = 1

        elif action == "rename":
            if identity is None:
                raise CentralError("unknown_identity", f"no such file id {file_id}")
            target = write["path"]
            if identity["tombstoned"]:
                self._mark_retained(record, UNLINKED_WHILE_WRITING,
                                    f"id {file_id} is unlinked", at)
            elif target in self.state["paths"] and self.state["paths"][target] != file_id:
                self._mark_retained(
                    record, PATH_TAKEN,
                    f"path {target!r} already bound to {self.state['paths'][target]}", at)
            elif identity["path"] and identity["path"] != target \
                    and self._binding_owner(file_id) not in (None, client):
                self._mark_retained(
                    record, RENAME_DIVERGED,
                    f"id {file_id} already renamed to {identity['path']!r} "
                    f"by {self._binding_owner(file_id)}", at)
            else:
                old_path = identity["path"]
                if old_path:
                    self.state["paths"].pop(old_path, None)
                identity["path"] = target
                identity["binding_owner"] = key
                self.state["paths"][target] = file_id
                record["state"] = INTEGRATED

        elif action == "unlink":
            if identity is None:
                raise CentralError("unknown_identity", f"no such file id {file_id}")
            others = [k for k, r in self.state["writes"].items()
                      if r["id"] == file_id and r["client"] != client
                      and r["action"] in ("edit", "truncate")
                      and r["state"] == INTEGRATED
                      and r.get("resolution") != "discard"]
            if identity["tombstoned"]:
                record["state"] = INTEGRATED  # 重复撤下，幂等无操作
            elif others:
                self._mark_retained(
                    record, UNLINKED_WHILE_WRITING,
                    f"other clients still have live writes: {sorted(others)}", at)
            else:
                identity["tombstoned"] = True
                if identity["path"]:
                    self.state["paths"].pop(identity["path"], None)
                identity["path"] = None
                record["state"] = INTEGRATED

        elif action in ("edit", "truncate"):
            if identity is None:
                raise CentralError("unknown_identity", f"no such file id {file_id}")
            if identity["tombstoned"]:
                self._mark_retained(record, UNLINKED_WHILE_WRITING,
                                    f"id {file_id} was unlinked; write kept for claim", at)
                self._persist()
                return dict(record)
            self._apply_range_write(record, at)

        else:
            raise CentralError("unknown_action", f"unsupported action {action}")

        if record["state"] is None:
            record["state"] = INTEGRATED
        self._persist()
        self._audit({"at": at, "event": "ingest", "write": key,
                     "state": record["state"], "reason": record["reason"]})
        return dict(record)

    def _binding_owner(self, file_id: str) -> str | None:
        owner = self.state["ids"][file_id].get("binding_owner")
        return owner.split(":")[0] if owner else None

    def _apply_range_write(self, record: dict, at: int) -> None:
        """把新区间写入并入计算，区分自动接入与待认领冲突。

        判定基准是当前公共版本快照（后来者看到的已发布状态）：

        * 同一客户端对自己已发布区间的改写是正常演进，永不自相冲突；
        * 不同客户端在重叠区间给出与已发布字节不同的内容，是在同一份公共
          基线上分叉的离线冲突，两边字节都留存待认领；
        * 与尚未发布的并发活跃写入重叠且不一致，同样是冲突。
        """
        file_id = record["id"]
        identity = self.state["ids"][file_id]
        current = identity["versions"][str(identity["current_version"])]
        # 探测分支包含所有有效写入（含已发布的对端写入），由 merge_branches
        # 按“重叠位置字节不同”给出冲突；基准用当前快照而非身份起点。
        probe = {k: [dict(op) for op in ops]
                 for k, ops in self._active_range_writes(file_id).items()}
        op = {"client": record["client"], "seq": record["seq"],
              "action": record["action"], "base": record.get("base")}
        if record["action"] == "edit":
            op["offset"] = record["offset"]
            op["data"] = self._get_blob(record["blob"])
        else:
            op["size"] = record["new_size"]
        probe.setdefault(record["client"], []).append(op)
        probe[record["client"]].sort(key=lambda item: item["seq"])

        result = merge_branches(self._get_blob(current["blob"]), probe)
        hit = [c for c in result.conflicts
               if any((w["client"], w["seq"]) == (record["client"], record["seq"])
                      for w in c.writes)]
        if hit:
            reason = hit[0].reason
            members = sorted({f"{w['client']}:{w['seq']}"
                              for conflict in hit for w in conflict.writes})
            self._record_conflict(file_id, reason, members,
                                  "; ".join(c.detail for c in hit), at)
            record["state"] = RETAINED
            record["reason"] = reason
            self._audit({"at": at, "event": "retained", "write": self._write_key(record),
                         "reason": reason, "detail": hit[0].detail})
            return
        record["state"] = INTEGRATED
        self._rebuild(file_id, at, extra=op)
        record["commit"] = identity["current_version"]

    # --------------------------------------------------------------- 裁决
    def resolve(self, client: str, seq: int, decision: str, at: int) -> dict:
        """管理员对一笔冲突写入做 accept/discard，冲突成员全部裁决后才关闭。"""
        with self.lock:
            key = f"{client}:{seq}"
            record = self.state["writes"].get(key)
            if record is None:
                raise CentralError("unknown_write", f"no retained write {key}")
            if decision not in ("accept", "discard"):
                raise CentralError("bad_decision", decision)
            relevant = [c for c in self.state["conflicts"] if key in c["members"]]
            if not relevant:
                raise CentralError("not_retained", f"{key} has no open conflict")
            if record["state"] == INTEGRATED:
                # 已在公共结果中的成员：仅确认选择，不允许反向 discard 已发布字节。
                if decision != "accept":
                    raise CentralError(
                        "already_published",
                        f"{key} is already public; cannot discard published bytes")
                for conflict in relevant:
                    conflict["resolved_members"].setdefault(key, "accept")
            else:
                if record.get("resolution") is not None:
                    raise CentralError("already_resolved",
                                       f"{key} resolved as {record['resolution']}")
                if decision == "accept":
                    # 普通字节重叠：两边互斥，不能同时接受。
                    # 截断/撤下类冲突：接受操作方意味着撤销被切掉/被撤下的对端写入，
                    # 因此允许接受，并在下方把对端已发布写入一并标记丢弃后重算。
                    for conflict in relevant:
                        if conflict["reason"] in (TRUNCATED_OTHER_WRITE,
                                                  UNLINKED_WHILE_WRITING):
                            continue
                        for member in conflict["members"]:
                            if member == key:
                                continue
                            other = self.state["writes"][member]
                            other_accepted = (
                                other.get("resolution") == "accept"
                                or conflict["resolved_members"].get(member) == "accept")
                            if other_accepted and conflict["reason"] == OVERLAP:
                                raise CentralError(
                                    "conflict_still_open",
                                    f"cannot accept {key}: {member} already accepted "
                                    f"for {conflict['reason']}")
                    record["resolution"] = "accept"
                    record["resolved_at"] = at
                    if record["action"] == "create":
                        self._accept_create(record, at)
                    elif record["action"] == "rename":
                        self._accept_rename(record, at)
                    elif record["action"] == "unlink":
                        identity = self.state["ids"][record["id"]]
                        identity["tombstoned"] = True
                        if identity["path"]:
                            self.state["paths"].pop(identity["path"], None)
                        identity["path"] = None
                    else:
                        self._rebuild(record["id"], at)
                        record["commit"] = \
                            self.state["ids"][record["id"]]["current_version"]
                    # 接受截断：被该截断切掉的对端已发布写入随之撤销，重算公共版本。
                    if record["action"] == "truncate":
                        for conflict in relevant:
                            if conflict["reason"] != TRUNCATED_OTHER_WRITE:
                                continue
                            for member in conflict["members"]:
                                if member == key:
                                    continue
                                other = self.state["writes"][member]
                                if other["state"] == INTEGRATED:
                                    other["resolution"] = "discard"
                                    other["resolved_at"] = at
                                    conflict["resolved_members"][member] = "discard"
                        self._rebuild_after_resolution(record["id"], at)
                else:
                    record["resolution"] = "discard"
                    record["resolved_at"] = at

            for conflict in self.state["conflicts"]:
                if key in conflict["members"]:
                    conflict["resolved_members"][key] = decision
                    if set(conflict["resolved_members"]) == set(conflict["members"]):
                        conflict["open"] = False
            self._persist()
            self._audit({"at": at, "event": "resolve", "write": key, "decision": decision})
            return dict(record)

    def _accept_create(self, record: dict, at: int) -> None:
        file_id = record["id"]
        target = record["path"]
        if file_id in self.state["ids"]:
            return
        if target in self.state["paths"]:
            # 路径仍被占：身份与版本可用，但不进公共目录，继续挂路径冲突。
            record["reason"] = PATH_TAKEN
            return
        payload = self._get_blob(record["blob"])
        self.state["ids"][file_id] = {
            "origin": record["blob"], "path": target, "tombstoned": False,
            "current_version": 1,
            "versions": {"1": {"blob": record["blob"], "size": len(payload),
                               "committed_at": at, "basis": 0,
                               "sources": [[record["client"], record["seq"]]],
                               "kind": "create"}},
        }
        self.state["paths"][target] = file_id
        record["state"] = INTEGRATED
        record["reason"] = None
        record["commit"] = 1

    def _accept_rename(self, record: dict, at: int) -> None:
        identity = self.state["ids"].get(record["id"])
        target = record["path"]
        if identity is None:
            return
        if target in self.state["paths"] and self.state["paths"][target] != record["id"]:
            record["reason"] = PATH_TAKEN
            return
        if identity["tombstoned"]:
            record["reason"] = UNLINKED_WHILE_WRITING
            return
        old_path = identity["path"]
        if old_path:
            self.state["paths"].pop(old_path, None)
        identity["path"] = target
        identity["binding_owner"] = self._write_key(record)
        self.state["paths"][target] = record["id"]
        record["state"] = INTEGRATED
        record["reason"] = None

    # --------------------------------------------------------------- 句柄
    def open_handle(self, handle: str, file_id: str, until: int, at: int) -> dict:
        """签发钉在当前公共版本上的读句柄；后续改名/合并不影响其字节范围。"""
        with self.lock:
            identity = self.state["ids"].get(file_id)
            if identity is None:
                raise CentralError("unknown_identity", f"no such file id {file_id}")
            version = identity["current_version"]
            self.state["handles"][handle] = {
                "id": file_id, "version": version, "until": until,
                "created_at": at,
            }
            self._persist()
            self._audit({"at": at, "event": "open", "handle": handle,
                         "id": file_id, "version": version})
            return dict(self.state["handles"][handle])

    def read_handle(self, handle: str, offset: int, length: int, at: int) -> dict:
        with self.lock:
            pinned = self.state["handles"].get(handle)
            if pinned is None:
                raise CentralError("unknown_handle", handle)
            if not lease_active(pinned["until"], at):
                raise CentralError("handle_expired", f"{handle} expired at {pinned['until']}")
            identity = self.state["ids"][pinned["id"]]
            version = identity["versions"][str(pinned["version"])]
            data = self._get_blob(version["blob"])
            if offset < 0 or length < 0:
                raise CentralError("bad_range", "offset/length must be non-negative")
            end = min(len(data), offset + length)
            chunk = data[offset:end]
            return {"handle": handle, "id": pinned["id"], "version": pinned["version"],
                    "offset": offset, "length": len(chunk),
                    "eof": end >= len(data), "data_base64": base64.b64encode(chunk).decode()}

    # --------------------------------------------------------------- 查询
    def directory(self) -> dict:
        """公共目录：只含已明确身份且有可用版本的路径绑定。"""
        with self.lock:
            return dict(self.state["paths"])

    def file_status(self, file_id: str) -> dict:
        with self.lock:
            identity = self.state["ids"].get(file_id)
            if identity is None:
                raise CentralError("unknown_identity", file_id)
            versions = {version: {"size": meta["size"], "sha256": meta["blob"],
                                  "committed_at": meta["committed_at"],
                                  "sources": meta["sources"]}
                        for version, meta in identity["versions"].items()}
            return {"id": file_id, "path": identity["path"],
                    "tombstoned": identity["tombstoned"],
                    "current_version": identity["current_version"], "versions": versions}

    def conflicts(self, include_closed: bool = False) -> list[dict]:
        with self.lock:
            return [dict(c) for c in self.state["conflicts"]
                    if include_closed or c["open"]]

    def write_records(self) -> dict:
        with self.lock:
            return {key: dict(record) for key, record in self.state["writes"].items()}

    def status(self) -> dict:
        with self.lock:
            return {
                "capacity": self.capacity(),
                "directory": dict(self.state["paths"]),
                "leases": {k: dict(v) for k, v in self.state["leases"].items()},
                "handles": {k: dict(v) for k, v in self.state["handles"].items()},
                "conflicts": [dict(c) for c in self.state["conflicts"]],
                "writes": {k: self._public_write(r)
                           for k, r in self.state["writes"].items()},
                "ids": sorted(self.state["ids"]),
            }

    @staticmethod
    def _public_write(record: dict) -> dict:
        return {k: v for k, v in record.items() if k != "blob"}

    # --------------------------------------------------------------- 回收
    def gc(self) -> dict:
        """依据三类承诺决定中心副本能否回收。

        * 离线回执：本地占量在中心确认持久（含留存冲突）后即可释放，与本回收无关；
        * 公共入库：身份起点（origin）与已接入的区间写入载荷仍要参与后续冲突
          判定与重算，必须保留；只有明确 discard 的载荷无人认领，可回收；
        * 新旧句柄：版本快照仍被有效句柄钉住、或仍是身份当前版本时保留，
          其余过期快照可回收。
        """
        with self.lock:
            pinned_versions: set[tuple[str, str]] = set()
            for pinned in self.state["handles"].values():
                pinned_versions.add((pinned["id"], str(pinned["version"])))
            keep: set[str] = set()
            # 身份起点与当前/钉住的版本快照
            for file_id, identity in self.state["ids"].items():
                keep.add(identity["origin"])
                for version, meta in identity["versions"].items():
                    if (file_id, version) in pinned_versions \
                            or version == str(identity["current_version"]):
                        keep.add(meta["blob"])
            # 写入载荷：除明确 discard 外都保留（接入的要参与冲突判定，留存的待认领）
            for record in self.state["writes"].values():
                if record.get("resolution") == "discard":
                    continue
                keep.add(record["blob"])
            removed = []
            for blob in list(self.blobs_dir.iterdir()):
                if blob.is_file() and blob.name not in keep:
                    removed.append(blob.name)
                    blob.unlink()
            self._audit({"at": -1, "event": "gc", "reclaimed": len(removed)})
            return {"reclaimed": removed, "capacity": self.capacity()}
