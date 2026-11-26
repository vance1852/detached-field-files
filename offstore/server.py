"""中心服务的 HTTP 包装，以及给本地端使用的远程客户端。

默认仅监听 127.0.0.1，与设备资料端保持同样的暴露范围。
远程客户端 :class:`RemoteCentral` 的方法形状与 :class:`~offstore.store.CentralStore`
一致，因此 :class:`~offstore.edge.EdgeVolume` 可以无缝切换进程内/网络后端，
用于验证断网、丢包后重放以及服务端重启恢复。
"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib import error as urlerror
from urllib import request as urlrequest
from urllib.parse import parse_qs, urlsplit

from .store import CentralError, CentralStore


class CentralHandler(BaseHTTPRequestHandler):
    def _send(self, status: int, payload) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict:
        size = int(self.headers.get("Content-Length", "0"))
        if not size:
            return {}
        return json.loads(self.rfile.read(size).decode("utf-8"))

    def _handle(self, method: str, path: str) -> None:
        store: CentralStore = self.server.central  # type: ignore[attr-defined]
        body = self._body() if method == "POST" else {}
        query = parse_qs(urlsplit(path).query)
        path = urlsplit(path).path
        try:
            if method == "GET" and path == "/status":
                return self._send(200, store.status())
            if method == "GET" and path == "/directory":
                return self._send(200, store.directory())
            if method == "GET" and path == "/file":
                return self._send(200, store.file_status(query["id"][0]))
            if method == "GET" and path == "/conflicts":
                include_closed = query.get("include_closed", ["false"])[0] == "true"
                return self._send(200, store.conflicts(include_closed))
            if method == "POST" and path == "/checkout":
                return self._send(200, store.issue_lease(
                    body["client"], body["base"], body["until"], body["at"]))
            if method == "POST" and path == "/revoke":
                return self._send(200, store.revoke_lease(body["client"], body["at"]))
            if method == "POST" and path == "/ingest":
                return self._send(200, store.ingest(body))
            if method == "POST" and path == "/resolve":
                return self._send(200, store.resolve(
                    body["client"], body["seq"], body["decision"], body["at"]))
            if method == "POST" and path == "/open":
                return self._send(200, store.open_handle(
                    body["handle"], body["id"], body["until"], body["at"]))
            if method == "POST" and path == "/read":
                return self._send(200, store.read_handle(
                    body["handle"], body["offset"], body["length"], body["at"]))
            if method == "POST" and path == "/gc":
                return self._send(200, store.gc())
            self._send(404, {"state": "unmatched"})
        except CentralError as exc:
            self._send(409, {"code": exc.code, "error": str(exc)})
        except (KeyError, ValueError, TypeError) as exc:
            self._send(400, {"code": "bad_request", "error": str(exc)})

    def do_GET(self):  # noqa: N802
        self._handle("GET", self.path)

    def do_POST(self):  # noqa: N802
        self._handle("POST", self.path)

    def log_message(self, *_):  # 静默，避免污染测试输出
        pass


class CentralHTTPServer(ThreadingHTTPServer):
    def __init__(self, address, central: CentralStore):
        super().__init__(address, CentralHandler)
        self.central = central


class RemoteCentral:
    """CentralStore 的 HTTP 镜像；连接失败向上抛出由本地端保留 local 回执。"""

    def __init__(self, base_url: str, timeout: float = 5.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _call(self, path: str, payload: dict | None = None):
        data = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8")
        req = urlrequest.Request(
            self.base_url + path, data=data,
            headers={"Content-Type": "application/json; charset=utf-8"}, method="POST") \
            if payload is not None else urlrequest.Request(self.base_url + path)
        try:
            with urlrequest.urlopen(req, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urlerror.HTTPError as exc:
            detail = json.loads(exc.read().decode("utf-8"))
            raise CentralError(detail.get("code", "http_error"),
                               detail.get("error", str(exc))) from None
        except (urlerror.URLError, TimeoutError, OSError) as exc:
            raise ConnectionError(f"central unreachable: {exc}") from None

    def issue_lease(self, client, base, until, at):
        return self._call("/checkout", {"client": client, "base": base,
                                        "until": until, "at": at})

    def revoke_lease(self, client, at):
        return self._call("/revoke", {"client": client, "at": at})

    def ingest(self, write):
        return self._call("/ingest", write)

    def resolve(self, client, seq, decision, at):
        return self._call("/resolve", {"client": client, "seq": seq,
                                       "decision": decision, "at": at})

    def open_handle(self, handle, file_id, until, at):
        return self._call("/open", {"handle": handle, "id": file_id,
                                    "until": until, "at": at})

    def read_handle(self, handle, offset, length, at):
        return self._call("/read", {"handle": handle, "offset": offset,
                                    "length": length, "at": at})

    def directory(self):
        return self._call("/directory")

    def file_status(self, file_id):
        return self._call(f"/file?id={file_id}")

    def conflicts(self, include_closed=False):
        return self._call(f"/conflicts?include_closed={'true' if include_closed else 'false'}")

    def status(self):
        return self._call("/status")

    def gc(self):
        return self._call("/gc", {})


def build_from_fixture(state_dir: str, fixture_dir: str | Path) -> CentralStore:
    """按设备资料的 policy 初始化中心：基线身份、字节与中心容量。"""
    record = json.loads(Path(fixture_dir, "events.json").read_bytes())
    policy = record["policy"]
    objects = {name: (Path(fixture_dir) / "objects" / name).read_bytes()
               for name in record["objects"]}
    store = CentralStore(state_dir, policy["central_capacity"])
    if not store.state["ids"]:
        store.seed_baseline(policy["entries"], objects)
    return store, objects


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path, help="设备记录目录（提供基线与容量）")
    parser.add_argument("--state", type=Path, default=Path(".central-state"),
                        help="中心持久化目录")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8091)
    args = parser.parse_args()
    central, _ = build_from_fixture(args.state, args.fixture)
    with CentralHTTPServer((args.host, args.port), central) as server:
        print(f"central store on http://{args.host}:{server.server_port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
