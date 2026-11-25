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
