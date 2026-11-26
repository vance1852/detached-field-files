"""按设备资料 ``events.json`` 驱动端到端重放。

资料端只负责提供身份、区间记录与原始字节；本模块把这些事实喂给真实的
中心服务与本地写入端，产出每个时间点的接入/留存/读取结果，供断言与复盘。

支持两种后端：

* ``inproc``：中心与本地卷同进程（默认，单测使用）；
* ``http``：中心经 :mod:`offstore.server` 的 HTTP 暴露，本地卷持
  :class:`~offstore.server.RemoteCentral`，可验证断网与服务端重启。
"""

from __future__ import annotations

import json
from pathlib import Path

from .edge import EdgeVolume
from .store import CentralStore
from .core import sha256_hex


def load_fixture(fixture_dir: str | Path) -> tuple[dict, dict[str, bytes]]:
    fixture_dir = Path(fixture_dir)
    record = json.loads((fixture_dir / "events.json").read_text(encoding="utf-8"))
    objects = {name: (fixture_dir / "objects" / name).read_bytes()
               for name in record["objects"]}
    return record, objects


def replay(fixture_dir: str | Path, state_root: str | Path,
           central: CentralStore | None = None, remote_factory=None) -> dict:
    """重放整套设备记录，返回每个事件的处理结果与最终状态快照。"""
    state_root = Path(state_root)
    state_root.mkdir(parents=True, exist_ok=True)
    record, objects = load_fixture(fixture_dir)
    policy = record["policy"]

    if central is None:
        central = CentralStore(state_root / "central", policy["central_capacity"])
        central.seed_baseline(policy["entries"], objects)
    backend = remote_factory() if remote_factory else central

    volumes: dict[str, EdgeVolume] = {}
    trace: list[dict] = []
    reads: dict[str, dict] = {}

    def backend_for(client: str):
        return remote_factory() if remote_factory else central

    for event in record["events"]:
        action = event["action"]
        at = event["at"]
        result = {"at": at, "action": action, "event": event}

        if action == "checkout":
            client = event["client"]
            volume = EdgeVolume(state_root / f"vol-{client}", client,
                                policy["edge_capacity"][client],
                                backend_for(client), objects)
            volume.checkout(event["base"], event["until"], at)
            volumes[client] = volume
            result["remaining"] = volume.remaining()

        elif action == "open":
            result["handle"] = backend_for("commander").open_handle(
                event["handle"], event["id"], event["until"], at)

        elif action == "read":
            reply = backend_for("commander").read_handle(
                event["handle"], event["offset"], event["length"], at)
            reads[event["handle"]] = reply
            result["read"] = reply

        elif action == "integrate":
            client = event["client"]
            result["integrate"] = volumes[client].integrate(at)

        elif action == "resolve":
            result["resolve"] = backend_for("handler").resolve(
                event["client"], event["seq"], event["action_result"], at)

        else:  # edit / truncate / rename / create / unlink：本地写入
            client = event["client"]
            fields = {k: v for k, v in event.items()
                      if k not in ("at", "action", "client")}
            if "file" in fields:
                fields["data"] = objects[fields.pop("file")]
            result["receipt"] = volumes[client].write(action, at, **fields)

        trace.append(result)

    return {
        "trace": trace,
        "reads": reads,
        "status": central.status(),
        "volumes": {client: {"remaining": vol.remaining(),
                             "receipts": vol.receipts(),
                             "pending": vol.pending_writes(),
                             "readable": vol.readable(),
                             "local_view": vol.local_view()}
                    for client, vol in volumes.items()},
        "objects": {name: {"bytes": len(data), "sha256": sha256_hex(data)}
                    for name, data in objects.items()},
    }
