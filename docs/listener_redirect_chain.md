# 监听重定向链

`tab.listen` 与 `frame.listen` 新增**默认关闭**的重定向链采集模式。开启后，
一次 HTTP 重定向过程中的所有跳转会被归并为一条链，队列中**只入队一个终包**；
终包原有字段表示最后一次跳转，之前的各跳保存在 `packet.redirects` 中。

不开启时，监听行为与旧版本一致。

## 开启与关闭

```python
tab.listen.start('/final', redirect_chain=True)
# 或先设置再启动
tab.listen.set_redirect_chain(True)
tab.listen.start('/final')

frame.listen.start('/final', redirect_chain=True)
```

`start()` 还接受 `redirects` 作为 `redirect_chain` 的别名：

```python
tab.listen.start('/final', redirects=True)
```

读取与修改当前模式：

```python
tab.listen.redirect_chain          # bool，默认 False
tab.listen.redirect_chain = True   # 等价于 set_redirect_chain(True)
tab.listen.set_redirect_chain(True)
tab.listen.set_redirects(True)             # 别名
tab.listen.set_collect_redirects(True)     # 别名
```

`start(urls=None)` 不传 URL 时会保留已有的 URL 过滤配置，只更新重定向链模式。

## 过滤规则

过滤条件在**终跳**上判断：

- URL：只检查最后一次跳转的 URL；
- 方法：只检查终跳的请求方法；
- 资源类型：只检查终跳的 CDP resource type。

因此：

- 中间跳命中、终跳不命中：整条链不入队；
- 中间跳不命中、终跳命中：正常入队一个终包，中间跳仍完整保留在
  `packet.redirects` 中。

这避免了把一个 302 中间响应误当成最终响应，也保证每条匹配链只产生一个数据包。

## 终包字段

终包仍是 `DataPacket`，原有字段全部指向最终跳：

```python
packet.url                 # 最终跳 URL
packet.method              # 最终跳方法
packet.request             # 最终跳请求
packet.response            # 最终跳响应
packet.resourceType        # 最终跳资源类型
packet.response.body       # 只取最终跳正文
packet.is_failed           # 链是否以失败结束
packet.fail_info           # 失败原因（若失败）
```

新增链相关属性：

```python
packet.redirects      # List[RedirectHop]，终跳之前的各跳，按发生顺序
packet.redirect_count # 终跳之前的跳转数量
packet.hops           # redirects + [终跳]；终跳也是 RedirectHop
```

`redirects` 中的每一跳包含（`RedirectHop` 也可从 `DrissionPage.items` 导入）：

```python
hop.url                    # 该跳请求 URL
hop.method                 # 该跳请求方法
hop.request                # 该跳请求（与 DataPacket.request 用法一致）
hop.response               # 该跳响应；302/307 的响应同样可见
hop.status                 # 该跳响应状态码
hop.status_code            # 别名
hop.statusCode             # 别名
hop.resourceType           # 该跳资源类型
hop.request_headers        # 该跳请求头
hop.response_headers       # 该跳响应头
hop.request_extra_info     # 该跳请求 ExtraInfo
hop.response_extra_info    # 该跳响应 ExtraInfo
hop.requestExtraInfo       # 别名
hop.responseExtraInfo      # 别名
hop.body / hop.data        # 中间跳固定为 None
hop.fail_info              # 跳失败信息（若该跳失败）
hop.raw_request            # 原始 requestWillBeSent 事件
hop.raw_response           # 原始 redirectResponse / responseReceived 响应
hop.as_dict()              # 便于日志和序列化的字典
```

只有终跳通过 CDP `Network.getResponseBody` 获取正文。中间跳不会发额外 CDP
请求，其 `body` 为 `None`。失败链不会尝试获取正文，但已收集的跳、跳状态、
响应头与 `fail_info` 都会保留。

## ExtraInfo 归属

CDP 的 `requestWillBeSentExtraInfo` / `responseReceivedExtraInfo` 可能早于、
晚于对应请求事件，或因浏览器实现而缺席。链模式会按 `requestId` 隔离链状态，
并用请求头相似度和响应状态码把 ExtraInfo 归属到具体跳：

- **早到**：先暂存，等对应跳出现后再配对；
- **晚到**：即使终包已经入队，仍会补挂到对应跳，并刷新已缓存的头信息；
- **缺席**：不会阻塞终包入队；终包不会等待缺席信息。

因此可以安全调用：

```python
packet.wait_extra_info(timeout=0.1)  # 缺席时返回 False，而不是永久阻塞
```

`packet._request_extra_info` / `packet._response_extra_info`（以及请求、响应
包装对象的 `extra_info`）始终指向终跳；中间跳的 ExtraInfo 通过
`hop.request_extra_info` / `hop.response_extra_info` 读取。

## 并发与 requestId 复用

每个 `requestId` 单独维护一条链状态，因此：

- 多个并发重定向链不会串数据；
- 同一个 `requestId` 的多次 `requestWillBeSent` 不会覆盖上一跳的原始请求；
- 同一 `requestId` 在链路结束后被复用时，会新建一条链，不继承旧链。

为避免长期运行内存增长，已完成的链与迟到的孤儿 ExtraInfo 都有上限；达到上限
时按完成/到达顺序淘汰最旧记录。

## wait_silent

请求计数在**第一次** `requestWillBeSent` 时增加，在 `loadingFinished` /
`loadingFailed` 时减少；ExtraInfo 事件不改变计数。

所以：

- 重定向进行中，`wait_silent()` 不会误报静默；
- 链终结后，即使 ExtraInfo 缺席或迟到，`wait_silent()` 也会正常返回；
- `wait_silent(targets_only=True)` 按命中过滤的活跃请求判断；
- 失败的链同样会释放计数。

## stop / clear

`stop()`、`clear()` 会：

- 丢弃队列；
- 清空链、孤儿 ExtraInfo 与计数；
- 记录旧轮次的 `requestId` 为过期，防止上一轮迟到的 ExtraInfo 挂到新一轮
  同名请求上。

新一轮的第一个 `requestWillBeSent` 会清除该 `requestId` 的过期标记。

## 非 HTTP 与旧行为

以下行为不受链模式影响：

- `wait()` / `steps()` / `browser_wait()` / `browser_steps()` 的语义与队列顺序；
- 方法、资源类型、URL 过滤 setter；
- `WebSocketPacket`、`SSEPacket` 及 WebSocket/SSE 事件回调；
- `DataPacket` 的请求参数、postData、JSON/base64/SSE 正文解析等契约；
- `tab.listen` 与 `frame.listen` 各自的 frame 归属过滤逻辑。

链模式只处理普通 HTTP 资源；`EventSource` 与 `WebSocket` 仍走原有监听路径。
若资源类型过滤只包含这些非 HTTP 类型，则不会创建重定向链。

## 完整示例

```python
from DrissionPage import Chromium

browser = Chromium()
tab = browser.latest_tab

tab.listen.set_urls('/final')
tab.listen.set_method.POST()
tab.listen.start(redirect_chain=True)

tab.get('https://example.com/start')

packet = tab.listen.wait(timeout=10)
print(packet.url, packet.method, packet.redirect_count)

for hop in packet.hops:
    print(hop.order, hop.method, hop.status, hop.url)
    print('  req:', dict(hop.request_headers))
    print('  resp:', dict(hop.response_headers))

print(packet.response.body)
tab.listen.wait_silent(timeout=5)
tab.listen.stop()
```
