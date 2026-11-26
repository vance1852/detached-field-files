# 离线巡堤文件卷的对接与冲突留存

存储联调使用合成设备、时间轴和原始字节。设备记录由各客户端及介质的输入组成，每份记录可独立使用。

## 业务协议

基线 `entries` 每项含不可变文件身份 `id`、路径与字节来源 `file`。客户写入以离线许可内的 `seq` 识别，`edit` 的偏移从零开始，`file` 为新写入范围的完整字节。`truncate` 的 `size` 为更改后的长度，`rename` 不修改身份。`create` 需要新身份。`unlink` 是撤下目录项。离线租约到期字段为 `until`，时间轴右端点不含。
`open` 可提供固定版本的读句柄。`resolve` 由管理员处理冲突，动作 `accept` 或 `discard` 指向一笔具体离线写入。路径采用区分大小写的 POSIX 相对路径。`edge_capacity` 是每个客户端可实际保存的字节上限。

## 本地设备资料

运行环境为 Python 3.11.2，使用标准库。设备资料端默认仅在 127.0.0.1:8090 运行，使用 --port 可另设端口。

~~~sh
python3 device_fixture.py fixtures/rename-crosses-range-edits
~~~

GET /fixture/events 返回设备参数、字节目录和按到达顺序记录的输入。同一时刻按文件先后排列。GET /fixture/objects/文件名 取原始字节。GET /fixture/journal 查看资料端已收到的设备请求，每个请求记录实际正文长度和摘要。

events.json 中的 packets 是可重放的设备响应。资料端按实际 HTTP 方法、路径和完整原始请求正文匹配第一条尚未使用的记录。没有匹配时返回 404。lag_ms 是回包延迟，drop 代表设备收到请求后直接断开连接，接收方需要以其他报告确认结果。没有 HTTP 交互的设备和客户端动作直接取 events。重新启动资料端会回到记录开头。

在项目根目录运行 python3 -m unittest -v 检查事件、对应字节、设备响应及接收日志。

## 接合服务与本地写入端（offstore）

`offstore/` 在设备资料提供的身份、区间记录和原始字节之上，实现了中心接合服务
与本地写入端，全部使用标准库：

| 模块 | 职责 |
| --- | --- |
| `offstore/core.py` | 无状态纯逻辑：区间合并、冲突分类、POSIX 相对路径校验、哈希、租约右端点判定 |
| `offstore/store.py` | 中心接合服务：文件身份与版本链、冲突留存、读句柄、中心容量、原子持久化、回收 |
| `offstore/edge.py` | 本地写入端（离线卷）：本地持久回执、剩余额度、断网重放、按身份本地读 |
| `offstore/server.py` | 中心服务的 HTTP 包装（默认 127.0.0.1）与 `RemoteCentral` 客户端 |
| `offstore/replay.py` | 按 `events.json` 驱动的端到端重放，产出每个时间点的接入/留存/读取结果 |

### 语义对照

* **身份 vs 路径**：`rename` 只改路径绑定，身份 `id` 不变；同路径重建必须 `create`
  新身份。两个身份争用同一路径时记 `path_taken`，实际字节留存待认领。
* **区间合并**：不重叠区间自动合并；同区间同字节只有一份结果（两个出处都进版本
  来源账本）；相交且字节不一致记 `overlap`，两边字节都保留且不进公共结果。
* **截断与撤下**：截断会删掉另一分支改写区间时记 `truncated_other_write`；
  文件已 `unlink` 而另一台仍续写记 `unlinked_while_writing`。
* **租约**：`until` 右端点不含。过期接入记 `lease_expired`、被收走许可后记
  `lease_revoked`：字节可靠持久但绝不自动合并、不冒充已接入。
* **幂等**：写入以 `(client, seq)` 识别。重复提交回显同一条本地写入（`duplicate`）；
  同一键携带不同请求会被 `write_key_reused_differently` 拒绝，不能刷进另一个文件。
* **读句柄**：`open` 钉住签发时刻的版本号，改名、撤下、合并之后仍按原字节范围续读；
  句柄到期后拒绝读取。
* **裁决**：处理人对具体写入 `accept`/`discard`；互斥的两边不能同时 accept；
  冲突成员全部裁决后才关闭。新的公共读写只跟随已明确的身份与可用版本。
* **容量与回收**：本地端按基线与未确认写入计占用，中心确认持久（含留存冲突）后
  释放本地占量；中心按内容寻址去重，超容量拒绝且不留残骸。`gc` 依据三类承诺决定
  副本能否回收：身份起点与已接入的区间写入载荷仍要参与后续冲突判定与重算而保留，
  仅明确 `discard` 的载荷可回收；版本快照在被有效句柄钉住或仍是当前版本时保留，
  其余过期快照可回收。
* **并发判据**：跨客户端的区间冲突只在两笔写入从同一离线基线（租约 `base`）分叉时
  成立；后来者从更新基线签出后在已发布区间上的改写属于正常演进，不误判为冲突。
  同一客户端对自己已发布区间的改写永不自相冲突。
* **重启恢复**：中心 `state.json` 与本地卷 `state.json` 每次变更原子落盘并 fsync，
  重启后已承诺的读取、待处理冲突和待重放写入都有依据。

### 运行

~~~sh
# 中心 HTTP 服务（按资料 policy 初始化基线与容量）
python3 -m offstore.server fixtures/rename-crosses-range-edits \
    --state .central-state --port 8091

# 端到端重放（进程内，便于复盘）
python3 -c "from offstore.replay import replay; \
    import json; print(json.dumps(replay('fixtures/rename-crosses-range-edits', \
    '/tmp/run')['status'], ensure_ascii=False, indent=1))"

# 测试：纯合并、中心行为、两套夹具端到端、HTTP/断网/多卷/重启
python3 -m unittest -v
~~~

HTTP 接口：`POST /checkout|/revoke|/ingest|/resolve|/open|/read|/gc`，
`GET /status|/directory|/file?id=|/conflicts`。

