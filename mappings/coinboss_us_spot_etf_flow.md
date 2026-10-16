# CoinBoss 美国现货 BTC ETF 美元净流量

## 来源和对象

[CoinBoss 的公开页面](https://www.coinboss.com/bitcoin-etf)说明按美元统计美国现货基金的净申购减赎回，最新基金尚未报告时留空，不能把空白解释成零。页面公开脚本直接读取匿名 JSON `https://api.coinboss.com/api/etf/detail`，参数 `asset=BTC&days=30`；无需登录、浏览器会话、API Key 或付费账号。此路径与收费 API 产品分别核实，不依赖后者的认证配置。

发布方是 CoinBoss。页面标注 Farside，最新公开明细亦与 [Farside 原表](https://farside.co.uk/btc/)核对；报告注明 CoinBoss，不能把该接口写成直接访问 Farside。TFTC 继续作为独立候选来源。正常小时分别读取候选，按最新已公布日期选择一整段历史，同日优先 CoinBoss；不跨来源拼接窗口。某来源请求或结构校验失败时保留原因，可使用另一来源自身的有效日期及完整窗口。较新的不完整日不能跳过而用旧的完整日冒充当前。

## 字段及完整性

| JSON 字段 | 映射与条件 |
|---|---|
| `success`、`asset`、`unit` | 必须分别是 `true`、`BTC`、`million_usd` |
| `tickers[].ticker` | 唯一，必须为约定 12 只基金加 `TOTAL`；排除 DEFI 等其他产品，范围变化先降级 |
| `dates[]`、`data` 日期键 | ISO 交易日期，唯一、有序且两处集合一致；最大日期为来源截至日，不用抓取时刻代替 |
| `data[date][ticker]` | 单位百万 USD，使用十进制乘 1,000,000 得到 `history[].perEtfUsd`；缺键、null、布尔、字符串均不是零 |
| `data[date].TOTAL` | 同样换算为 USD，保留发布汇总为 `history[].totalNetInflow`；有限、有符号，真实零有效 |
| 逐基金之和与 `TOTAL` | 每日 12 只基金全部齐备。近期 Farside 行以 0.1 百万美元公布，早期行可到 0.001 百万美元；每项与总计独立舍入，最大误差为 13 个数各半个公布单位。含千美元精度的行按 1,000 USD 单位校验，其余按 100,000 USD 单位；低于已核实千美元精度时降级，不改变 TFTC 的微美元容差 |
| 最近 1/5/30 个交易日 | 分别按已核实 NYSE 日历复算 USD 总额；缺日或每日基金不完整则对应窗口为空，不采用接口其他页面的日历日汇总 |

采集时间、截至日期、来源、候选失败、窗口质量分别保留。时效沿用现有 20:00 ET 参考与一天宽限；宽限内落后日期仍显示待更新。接口没有保证发布时刻或持续可用性的 SLA，HTTP 200 也不证明最新日完整。持仓继续由 Bitbo 独立取得，禁止从 BTC 持仓变化乘价格推算美元申赎。

## 证据与消费

保留 USD/BTC 字段及 `public_etf_history` 标识，增加 `CoinBoss` + `api.coinboss.com/api/etf/detail` 的明确来源契约。Reader 按来源复核单位、12 基金范围、日期、每日汇总与 1/5/30 交易日窗口；TFTC 和旧 SoSoValue 继续按原规则校验。附件通过仍只是辅助输入，独立来源核验数不增加。

先部署 reader 兼容代码，再启用新来源的采集和证据输出；沿用原有状态与正常小时调度。实际响应和验证结果留在执行节点，不进入 Git。本文仅供模型理解，不由程序解析。
