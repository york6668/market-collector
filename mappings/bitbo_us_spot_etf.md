# Bitbo 美国现货 BTC ETF 持仓

## 对象与来源

匿名 GET [US ETFs](https://bitbo.io/treasuries/us-etfs/)，无账户、API Key 或付费依赖。页面列出基金持有的 BTC 数量；这不是美元净流入，也不是基金净资产。`Value Today` 使用当前 BTC 价格计算市值，不能替代基金 NAV 或 AUM。

约定基金范围是 IBIT、FBTC、GBTC、BTC、BITB、ARKB、HODL、MSBT、BRRR、EZBC、BTCO、BTCW。BITA 属于不同产品范围，DEFI 不在有效现货持仓范围，均不汇总。范围调整需要重新核对产品定义与来源，不能自动把未知基金加入汇总。

## 字段映射与覆盖

| 页面字段 | 采集字段 | 含义与条件 |
|---|---|---|
| 内嵌 `const data` 数组的 `symbol` | `holdings_rows[].symbol` | 必须唯一；已约定 12 个基金全部存在 |
| 同数组的 `btc` | `holdings_rows[].holdings_btc` | BTC 数量，有限、非负；零值有效，保留原精度 |
| 12 个有效基金数量之和 | `public_records.etf.value.holdings_btc` | 单位 BTC，排除 BITA、DEFI；不读表格中的舍入数值 |
| 汇总表 `td[data-title="Last Updated"]` | `source_date.holdings` | 页面更新日，按观察日期使用；不推定每个基金的公布时刻 |
| 同来源、同范围的有效 7D 基准 | `holding_7d_pct` | 来源观察日期相差 7 个日历日；采集快照位于现有 7D 容差窗口；不足时“采集中” |

缺失或重复基金、未知基金、非法数量、日期缺失或冲突、页面结构变化、日期过期或请求失败均降级，不能把部分总量当完整持仓。旧来源基准、缓存回退或过期记录不能参与 7D 计算。

该匿名 GET 使用 3 秒连接、8 秒读取超时。读取超时后只允许一次新会话恢复，两次尝试均保留原始请求耗时及错误，恢复会话随请求结束关闭。会话是否来自池与 TCP 是否实际复用分别记录；连接池取出后记录连接状态、空闲时间、实际地址，区分 DNS/TCP、TLS、请求发送、响应头及正文阶段。观测适配器仅用于本采集客户端，不修改其他 requests 客户端。连接失败、TLS 错误、HTTP 拒绝及解析错误不走此恢复。两次均失败仍为空，不用旧持仓补齐。新连接恢复不证明历史超时由连接复用引起；缺少当时连接与服务端证据时，不把 VPN、解析地址或 TLS 版本归为已证根因。

## 流量边界与兼容

停止请求失效的 SoSoValue 旧采集接口。`flow_1d_usd`、`flow_5d_usd`、`flow_30d_usd` 独立使用 [CoinBoss 公开美元流量](coinboss_us_spot_etf_flow.md)及 [TFTC](tftc_us_spot_etf_flow.md)；Bitbo 持仓差与当前价格不能换算成真实美元净流量。流量资产比例字段及 `holdings_market_pct` 保持 null。

已有 SoSoValue 流量只放在 `flow_background`，保留各窗口原始日期，明确仅作历史背景，不进入当前测量或方向判断。持仓是否有效与流量缺失分别处理，核心完整度分别统计 ETF Holding 与 ETF Flow。

共享证据保留 `etf` dataset 和 `public_etf` 来源标识。Bitbo 分支只证明完整持仓及日期，不要求美元流量历史；校验基金范围与总量，拒绝仅由该来源证明的流量或 NAV/AUM 比例。TFTC 流量使用独立的 `public_etf_history` 来源。旧 SoSoValue current/history 来源和附件校验仍支持，5D/30D 流量仍需完整的 5/30 个交易日观测。

更新顺序为 reader 共享契约、采集端；沿用节点状态和自然小时调度。Markdown 只解释协议与边界，程序不解析本文。
