# TFTC 美国现货 BTC ETF 美元净流量

匿名 GET [公开 JSON](https://www.tftc.io/bitcoin-etf-flows/data.json)，[发布方说明](https://www.tftc.io/bitcoin-etf-flows)明确单位 USD、12 只美国现货基金范围及净申购减赎回口径。日汇总与近期逐基金流量来自 SoSoValue；早期逐基金历史来自 Farside 汇总的发行方披露。数据由 TFTC 发布，来源不能标成直接访问 SoSoValue 或 Farside。按 CC BY 4.0 在报告中注明 TFTC 来源。

## 字段和完整性

| JSON 字段 | 使用方式 |
|---|---|
| `units` | 必须为 `USD`；不乘 BTC 价格，不把 US$m 表格精度用于 JSON |
| `updatedThrough` | 最新公布交易日；必须与 `days` 最大日期一致 |
| `days[].date` | 严格 ISO 日期；日期唯一、有序，最新不完整日不能退回旧日 |
| `days[].netFlowUsd` | `history[].totalNetInflow`，有限有符号 USD 数值；真实零有效 |
| `days[].perEtfUsd` | 必须包含且仅包含 IBIT、FBTC、GBTC、BTC、BITB、ARKB、HODL、MSBT、BRRR、EZBC、BTCO、BTCW；null、缺基金或非法数值不是零 |
| 同日逐基金之和 | 与总量复算；容差仅覆盖 JSON 浮点求和残差（相对 1e-12、绝对 1e-6 USD） |
| 最近 1/5/30 个交易日 | 独立复算 `flow_1d_usd` / `flow_5d_usd` / `flow_30d_usd`；日期与现有 NYSE 已公布日历连续一致，每日逐基金完整；缺任何一天则对应窗口为空 |

日历复用 `market_assets` 的已确认 NYSE 2025–2028 范围，未知年份降级。数据截至日期与采集时刻分开保留；沿用现有 20:00 ET 发布参考及一天宽限，不保证第三方何时更新。过期流量只作带日期背景，不能改成当前值。持仓和流量来源日期不同时明确显示，发布修订随正常小时采集重新读取。

## 独立质量与共享契约

持仓继续来自 Bitbo，质量独立。`public_records.etf.value` 兼容原字段和 USD/BTC 单位，`flow_record` 保存 TFTC 来源、日期、采集时间、窗口质量及原观测；当前 value 只包含有效窗口。请求失败、最新日未完整公布、缺交易日、基金范围变化及过期均暴露原因，不能用持仓变化估算或复活旧 SoSoValue 缓存。

证据 `public_etf` 只证明 Bitbo 持仓；`public_etf_history` 增加 TFTC 公开 JSON 契约，独立证明流量。逐基金、日期、交易日窗口和汇总由 reader 重算；缺流量不拒绝有效持仓，缺持仓不拒绝有效流量。旧 SoSoValue 附件仍按原契约校验。

Bitbo 流量图使用持仓变化与当前 BTC 价格换算，不作为本字段的美元净申赎来源。真实响应只留执行节点，不收录源码。

先部署 reader 的共享契约，再启用采集端，保留既有状态、发送回执和自然小时调度。本文仅解释映射，程序不解析 Markdown。
