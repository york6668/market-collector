# BTC 卖方衰竭常数：免费复算口径

## 对象、来源与适用范围

BTC 全市场日级链上/价格复合指标，采用 ARK 的“获利供应占比 × 30 日价格波动率”定义。它不是主动卖出量，也不是逐小时反转信号。仅 BTC 适用；QQQ/VOO 不套用。

用户要求免费接口，因此运行时只请求 BGeometrics 的匿名 REST API，不调用 Glassnode 付费 API、账户接口或受限图表隐藏数据。2026-10-02 在无 token 请求下确认三个接口返回历史，最新数据日为 2026-10-01；此事实不保证未来可用性。

| 输入 | 免费端点 | 实际字段与单位 |
|---|---|---|
| 日价格 | `https://bitcoin-data.com/v1/btc-price` | `btcPrice`，USD/BTC，数值字符串转为有限正数 |
| 获利供应量 | `https://bitcoin-data.com/v1/supply-profit` | `supplyProfitBtc`，BTC，非负 |
| 流通供应量 | `https://bitcoin-data.com/v1/supply-current` | `supplyCurrent`，BTC，正数 |

`d` 是 UTC 日日期，`unixTs` 是同一日时间戳，两者必须一致；`unixTs` 可为整数、小数或科学计数形式的数字字符串，按精确数值比对，不按字符串外观判断。仅纳入已完成 UTC 日；来源时间与实际抓取时间分别保存，发布时间未知不补造。获利供应基于币最后移动时成本的链上估计，不代表个人真实成本。

官方免费说明列出最近四年、每天15次、每小时8次请求限制。每个输入独立12小时刷新一次，共6次/日；异常最早6小时后重试，403后24小时，429尊重更长的 Retry-After（最大24小时）。持久 job 状态跨重启复用，同一 worker 无额外报告时取数。全量四年输入保存在本机历史库，重复观察幂等、修订保留实际首次取得时间。附件包含最近397天原料，前30天用于预热，能够复核约一年的日级结果。

## 确定性计算

- `supply_in_profit = supplyProfitBtc / supplyCurrent`，严格同日且必须在 [0,1]。
- 31个连续 UTC 日价格得到30个自然对数收益率 `ln(P[t]/P[t-1])`。
- `volatility_30d = pstdev(30个收益率)`：总体标准差，ddof=0，不年化，不平滑。
- `value = supply_in_profit * volatility_30d`。任一原料缺失、空值或价格窗口不连续则对应计算保持空缺；真实零波动允许结果为0，不填充历史缺日。
- 1/7/30日变化只比较相隔精确日数的有效结果；不拿最近可用日冒充指定日期。

方法标识 `ark_definition_bgeometrics_log_returns_30d_population_v1`；显示“卖竭常数（免费复算）”。Glassnode 的取样、UTXO处理、标准差约定与缩放一致性尚未验证，`official_equivalence=unverified`；不宣称这是官方原值，不套用官方0.01等绝对阈值。免费原料是获批替代来源；不偷偷换成短线CVD/OI评分。

注意：免费文档对 `/profit-loss` 的描述与实际字段数值不一致，实测 `profitLoss` 可大于1；不把它当获利供应比例，也不据此猜测换算。使用上表实际供应量相除。其他公开工具的原值免费预览有6个月延迟，不能用于当前回踩。

## 传递、质量与消费

V2 可选测量 `seller_exhaustion_daily` 同时携带 `inputs.price/profit/supply` 的原值、来源、抓取时间及日数据期，和 `supply_in_profit`、`volatility_30d`、`value`。三组成项来自同一 BGeometrics 来源族，不能计为三个独立来源。

编码、校验和 reader 二次投影均重算；拒绝无依据的值、错误单位/端点、时间错位、未完成日和数值篡改。局部数据不足、旧值、更新失败保留已知原料及具体限制。两日以上未有有效日值标记延迟，保持历史参考，不提升为当小时确认。旧V1/V2附件缺少本可选字段继续可读。

分析模型按回踩前后及相关场景判断，核验原料后形成观察；日值尚未覆盖回踩时仅作此前背景。完整深评与报告分别按实际证据影响判断，指数本身不形成配置决定。

## 依据

- ARK 原定义：https://research.ark-invest.com/hubfs/1_Download_Files_ARK-Invest/White_Papers/ARKInvest_123021_Whitepaper_OnChainData.pdf
- Glassnode 定义：https://docs.glassnode.com/basic-api/endpoints/indicators
- BGeometrics 免费契约：https://bitcoin-data.com/bguser/free-features.html
- API 与来源说明：https://bgeometrics.com/api/

文档供模型理解，不由程序解析或加载。
