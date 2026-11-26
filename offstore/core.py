"""无状态的纯逻辑：区间合并、冲突分类、路径校验、哈希与租约判定。

这里的函数不接触磁盘和网络，便于对合并语义做独立单元测试。
中心服务 :mod:`offstore.store` 与本地端 :mod:`offstore.edge` 都基于这些原语。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Iterable

# 冲突原因码（对外稳定的英文标识）
OVERLAP = "overlap"                          # 相交且字节不一致
TRUNCATED_OTHER_WRITE = "truncated_other_write"  # 截断删掉了另一方的改写
UNLINKED_WHILE_WRITING = "unlinked_while_writing"  # 文件已撤下，另一台仍在续写
LEASE_EXPIRED = "lease_expired"              # 离线租约到期后才接入
PATH_TAKEN = "path_taken"                    # 两个身份占了同一个路径
IDENTITY_EXISTS = "identity_exists"          # create 复用了既有身份
RENAME_DIVERGED = "rename_diverged"          # 同一身份被改名到不同路径

# 接入状态
INTEGRATED = "integrated"        # 已可靠接入公共结果
RETAINED = "retained"            # 实际字节已持久，留给人工认领
DUPLICATE = "duplicate"          # 重复提交，仍对应同一次本地写入
REJECTED = "rejected"            # 未被接受（语义错误，无留存字节）


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def validate_relpath(path: str) -> None:
    """区分大小写的 POSIX 相对路径：不能为空、不能绝对、不能含 ``..``。"""
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    if "\x00" in path:
        raise ValueError("path contains NUL")
    if "//" in path:
        raise ValueError("path must not contain empty segments")
    if any(segment in ("", ".", "..") for segment in path.split("/")):
        raise ValueError("path must not contain empty, '.' or '..' segments")
    pure = PurePosixPath(path)
    if pure.is_absolute():
        raise ValueError("path must be relative")
    if path in (".", "..") or path.endswith("/"):
        raise ValueError("path must be a normal relative path")


def lease_active(until: int, now: int) -> bool:
    """租约右端点不含：``now < until`` 才有效。"""
    return now < until


@dataclass
class EditOp:
    client: str
    seq: int
    offset: int
    data: bytes
    base: int | None = None

    @property
    def end(self) -> int:
        return self.offset + len(self.data)

    @property
    def span(self) -> tuple[int, int]:
        return self.offset, self.end


@dataclass
class TruncateOp:
    client: str
    seq: int
    size: int
    base: int | None = None


@dataclass
class Conflict:
    """一笔需要人工认领的冲突，保留全部相关写入的实际字节引用。"""

    reason: str
    writes: list[dict] = field(default_factory=list)
    detail: str = ""

    def key(self) -> tuple[str, frozenset]:
        return self.reason, frozenset((w["client"], w["seq"]) for w in self.writes)


@dataclass
class MergeResult:
    data: bytearray                       # 自动合并后的公共字节
    size: int                             # 公共长度
    applied: set[tuple[str, int]]         # 已自动接入的 (client, seq)
    conflicts: list[Conflict]             # 待认领冲突（写入字节已另行持久）


def _branch_intent(
    ops: Iterable[dict],
) -> tuple[dict[int, int], int, list[EditOp], list[TruncateOp]]:
    """按顺序回放单个分支的 edit/truncate，得到稀疏字节视图与最终长度。

    编辑超出当前长度时按零填充缺口；截断丢弃位于新长度及之后的所有字节。
    """
    cells: dict[int, int] = {}
    size = 0
    edits: list[EditOp] = []
    truncates: list[TruncateOp] = []
    for op in ops:
        kind = op["action"]
        if kind == "edit":
            edit = EditOp(op["client"], op["seq"], op["offset"],
                          bytes(op["data"]), op.get("base"))
            if edit.offset < 0:
                raise ValueError("negative offset")
            for index, byte in enumerate(edit.data):
                cells[edit.offset + index] = byte
            size = max(size, edit.end)
            edits.append(edit)
        elif kind == "truncate":
            new_size = op["size"]
            if new_size < 0:
                raise ValueError("negative truncate size")
            for pos in [p for p in cells if p >= new_size]:
                del cells[pos]
            size = min(size, new_size)
            truncates.append(TruncateOp(op["client"], op["seq"], new_size,
                                        op.get("base")))
        else:  # pragma: no cover - 调用方负责只传 edit/truncate
            raise ValueError(f"not a range write: {kind}")
    return cells, size, edits, truncates


def merge_branches(base: bytes, branches: dict[str, list[dict]]) -> MergeResult:
    """把多个离线分支对同一文件身份的 edit/truncate 合并回基线。

    ``branches`` 键为客户端身份，值为该分支按本地顺序排列的写入。
    语义：

    * 不重叠区间自动合并；同区间同字节只有一份结果；
    * 相交且任一共用位置字节不同 → ``overlap``，两边字节都留存；
    * 截断会切掉另一分支写入的区间（编辑末端超过截断长度）→
      ``truncated_other_write``；
    * 冲突写入不参与自动拼装，其真实字节由中心持久层保留待认领。
    """
    intents = {client: _branch_intent(ops) for client, ops in branches.items()}
    all_edits = [edit for _, _, edits, _ in intents.values() for edit in edits]
    all_truncates = [cut for _, _, _, cuts in intents.values() for cut in cuts]

    conflicted: set[tuple[str, int]] = set()
    conflicts: list[Conflict] = []

    def retain(reason: str, writes: list[dict], detail: str) -> None:
        item = Conflict(reason, writes, detail)
        if not any(existing.key() == item.key() for existing in conflicts):
            conflicts.append(item)
        for write in writes:
            conflicted.add((write["client"], write["seq"]))

    # 1) 截断 vs 另一分支的编辑：编辑末端越过截断长度即可能删掉对方改写。
    #    同样要求两笔写入从同一离线基线分叉，否则视为后来者已知情的演进。
    for cut in all_truncates:
        for edit in all_edits:
            if edit.client == cut.client:
                continue
            if edit.end <= cut.size:
                continue
            if cut.base is not None and edit.base is not None \
                    and cut.base != edit.base:
                continue
            retain(
                TRUNCATED_OTHER_WRITE,
                [
                    {"client": cut.client, "seq": cut.seq, "action": "truncate",
                     "size": cut.size},
                    {"client": edit.client, "seq": edit.seq, "action": "edit",
                     "offset": edit.offset, "length": len(edit.data)},
                ],
                f"truncate to {cut.size} removes write at [{edit.offset},{edit.end})",
            )

    # 2) 不同分支的编辑两两比较：重叠位置出现不同字节即冲突；
    #    共用位置全部相同则视为同一份内容，允许合并。
    #    跨客户端仅当两笔写入从同一离线基线分叉（base 相同）才算冲突；
    #    基线不同说明后者已看到前者发布的结果，属于正常演进。
    for index, left in enumerate(all_edits):
        for right in all_edits[index + 1:]:
            if left.client == right.client:
                continue
            lo = max(left.offset, right.offset)
            hi = min(left.end, right.end)
            if lo >= hi:
                continue  # 不重叠，可合并
            divergent = False
            for pos in range(lo, hi):
                if left.data[pos - left.offset] != right.data[pos - right.offset]:
                    divergent = True
                    break
            if not divergent:
                continue
            same_base = (
                left.base is not None and right.base is not None
                and left.base == right.base)
            if not same_base:
                continue  # 基线不同的跨客户端改写：后来者的正常演进，不冲突
            retain(
                OVERLAP,
                [
                    {"client": left.client, "seq": left.seq, "action": "edit",
                     "offset": left.offset, "length": len(left.data)},
                    {"client": right.client, "seq": right.seq, "action": "edit",
                     "offset": right.offset, "length": len(right.data)},
                ],
                f"divergent bytes inside [{lo},{hi})",
            )

    # 3) 用所有干净写入在基线副本上按 (客户端, seq) 确定顺序拼装。
    result = bytearray(base)
    applied: set[tuple[str, int]] = set()
    for edit in sorted(all_edits, key=lambda e: (e.client, e.seq)):
        token = (edit.client, edit.seq)
        if token in conflicted:
            continue
        if edit.end > len(result):
            result.extend(b"\x00" * (edit.end - len(result)))
        result[edit.offset:edit.end] = edit.data
        applied.add(token)

    # 4) 干净截断按顺序作用；涉及冲突的截断不自动改变公共长度。
    final_size = len(result)
    for cut in sorted(all_truncates, key=lambda c: (c.client, c.seq)):
        if (cut.client, cut.seq) in conflicted:
            continue
        if cut.size < len(result):
            del result[cut.size:]
        elif cut.size > len(result):
            result.extend(b"\x00" * (cut.size - len(result)))
        final_size = len(result)

    return MergeResult(result, final_size, applied, conflicts)
