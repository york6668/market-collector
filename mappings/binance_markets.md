# 币安 BTC、QQQ、VOO 监控口径

适用范围：`market_assets.py`、`market_history.py`、`market_public.py`、`market_streams.py`、`market_observations.py`、`btc_push.py` 的行情观察与证据，以及 `market_news.py`、`market_common.py` 的公共简报；不涉及下单或账户资产。这是业务资料，不由应用解析。公开接口于 2026-09-30 核实。新增及替换来源不使用账户、API Key、试用期或套餐。

## 产品归属与关系

| 观察对象 | 大类及价格来源 | 合约观察 | 关系和适用范围 |
| --- | --- | --- | --- |
| BTC | 加密资产；保留原 Binance Spot 展示价格、原小时线来源 | 原 Binance / Hyperliquid 模式 | 原 BTC 配置、影子与主源行为不变 |
| QQQ | 美股 ETF；Binance Stocks `EQ_QQQ` | `QQQUSDT` | 相同基金的直接衍生品参考；股票结构须使用自身股票小时线且满足复权条件 |
| VOO | 美股 ETF；Binance Stocks `EQ_VOO` | `SPYUSDT` | SPY 是另一只标普 500 ETF；仅作标普 500 衍生品参考，不参与 VOO 自身 OI/Funding 资金确认 |

QQQ、VOO 的股票价格以 **USD/股** 记录，成交量是 **股数**。页面支付/结算币种 USDC 不是股票报价币种；USDT 合约另行记录。OI 数量按合约数量记录，名义值按 USDT，不命名为 BTC 数量，也不做 USDT/USD 的隐式兑换。

VOO 支撑、阻力、趋势、量价与锚点只用 `EQ_VOO`。SPY 的基差仅由 **SPYUSDT 自身标记价与自身指数价** 计算；禁止 VOO/SPY 跨基金价格相减。`instrument / target_asset / observed_asset / relation / analysis_eligible` 保留直接、代理关系。

## 股票接口与原始字段

当前使用官方文档公开的 `wss://nbstream.binance.com/equity/ws`，通过 SUBSCRIBE 订阅。必须收到成功确认后才记录连接覆盖。对象身份不匹配不进行跨标的回退。

| 主题 | 字段 | 使用方式 |
| --- | --- | --- |
| `price` | `rates[].s/ac/p/t/mp/pc` | 校验 `QQQ/EQ_QQQ`、`VOO/EQ_VOO`；价格、源时间、前收盘价及阶段分别保存。`t` 缺失保持 null，采集时间单列。价格实时缓存、每分钟持久快照；不虚构自然 24H 成交量 |
| `QQQ/VOO@kline_5m`、`@kline_1h` | `s,k.t/ct/s/i/o/h/l/c/v/x/vw` | 只保存 `x=true` 且已结束的完整周期；复权固定 UNKNOWN。`v` 为股数，`vw` 为源加权价格，均不能推导主动成交方向 |
| `QQQ/VOO@quote` | `bp/ap/T` | 保存买卖报价和实际源时间，不当成实际成交 |
| `QQQ/VOO@tradingStatus`、`@tradability` | `symbol,status,tradability,t/time` | 保存停牌、恢复及限制事件。状态流仅在变化时推送，刚订阅没有状态不能伪造已确认状态；`NONE/OFFMARKET/DELISTING` 与停牌暂停自动报告 |
| `calendar` | `from/to/ts` | 保存市场阶段变化；不是完整历史交易日历 |

`https://www.binance.com/bapi/equity/` 的全部旧行情 REST 调用及自动回退已退出；新闻接口也继续停用。旧字段解析器仅供读取 V1 历史，不再请求这些地址。

## 交易时段、样本和复权

- 使用 `America/New_York` 时区和已公布的 2025–2028 年交易所日历处理夏令时、节假日与提前收盘，并与公开流阶段及停牌信息核对。断线或报价缓存过期停止自动股票报告；未知年份不假设开市。
- 常规周为周日 20:00 ET 至周五 20:00 ET，盘前 04:00、正常盘 09:30–16:00、盘后至 20:00、夜盘其余时段；节假日、提前收盘与源状态优先。历史规则含已核实的提前收盘日，不能只判断星期几。
- 历史连续性只要求交易日历内应有的小时线；计划休市不是采集缺口。4H/24H 对比、连续确认和按小时命名的窗口使用实际时间，不拿休市前数据补齐自然小时。复市保留冻结锚点，连续小时计数重新开始。
- 旧 ADJUSTED 历史、比较与锚点隔离到 `legacy_stock_history`；新 `binance_public_ws` 序列从实际接收积累，固定 UNKNOWN。复权条件未核实前，相关趋势、关键位及价格确认暂停，辅助报告仍可生成。宏观、投递身份不清除；旧价格版本的晚到回执不能恢复旧快照或锚点。
- 原生 UTC K 线不命名为美股常规交易日日线。WebSocket 无公开补数接口的断线时段保留缺口，不填零。
- 休市或行情流不可用时停止定时报告与旧定时报告重试；复市及行情流恢复本身不触发报告，等待下一整点，不补发错过的小时。人工查询只复用新来源的有效报告并标注原时间；只有旧 REST 缓存时返回来源已退出及积累状态，不在休市推进状态。

## 合约与适用性

`/fapi/v1/klines`、`/futures/data/openInterestHist`、`/fapi/v1/openInterest`、`/fapi/v1/premiumIndex` 请求指定合约 symbol。缓存身份包括源地址、symbol、粒度、窗口/小时；公共合约元数据与宏观数据单独共享。

`/fapi/v1/fundingRate` 的 `rateType`：

- `Regular`（旧 BTC 响应省略时按 Regular）：普通已结算 Funding。8H、24H 按真实结算时间及边界样本计算，未证明覆盖则为 null。
- `Special`：特殊股息调整，单独存入 `special_settlements`，附时间与 fraction 单位；不计入普通 Funding 的和、异常阈值或连续间隔。普通与特殊结算可以同一时间存在。
- 当前 `/fapi/v1/fundingInfo` 只说明当前周期，不能倒推整个历史；240H 历史周期变化保留“历史间隔未证实/变化”，不影响另行证明完整的当前 8H、24H。未知类型不混入两类已知结算。
- 普通结算时间单独展示与判定时效；超过已知结算周期及 5 分钟发布余量时保留历史值并取消当前确认资格。较新的 Special 不能让过期的 Regular 变成当前数据。

稳定币、加密恐慌、BTC 爆仓/Hyperliquid 只进入 BTC。QQQ/VOO 自身 ETF 申赎/持仓是“尚缺公开稳定来源”，真实股票 Spot CVD 是“缺少主动成交方向”；这些是需求缺口，不是“不适用”。合约 CVD、CFTC 仓位都不等价替代 ETF 申赎或股票主动成交。

## 持久历史与日周观察

- SQLite 默认在主 `STATE_FILE` 旁的 `.market.sqlite3`，`MARKET_HISTORY_DB` 可覆盖。JSON 保留运行状态和短缓存；SQLite 不随 240 条快照或侧车清理删除。观测保存源时间、采集时间、身份、单位、周期和来源；相同内容幂等，修订保留旧版本。
- `known_ms` 是实际取得该版本的时间。历史回填和修订不能在回放中提前出现；CFTC 星期二是持仓观察日，不是发布时间。缺少精确发布时刻时，最早可知时间保守使用首次采集时刻，不伪造星期五固定发布成功。
- Binance 现货/合约 K 线补 180 天，官方月度文件加 REST；近期三天每小时重查，旧缺口轮换重查，不让空历史阻塞新数据。2025 年后的现货档案微秒转换为毫秒。只使用闭合线，CVD = `2*taker_buy_quote - quote_volume`，单位 USDT；1/7/14 日有缺桶则净额为 null。累计曲线注明起点，遇缺口重新起算，原 `cvd_pct` 仍是净主动成交占比。
- OI 使用 `/futures/data/openInterestHist` 最近约一个月实际窗口，小时样本分段请求避免 500 条截断，并复查可取窗口修复缺口；过期历史只靠本地积累。数量和 USDT 名义值分列，名义值变化不是方向性资金流。
- DefiLlama `stablecoincharts/all` 仅取 `totalCirculating.peggedUSD` 为美元锚定稳定币供应量；`totalCirculatingUSD.peggedUSD` 另存为按市价折算的市值。1/7/14 日差使用供应量，脱锚不视为净发行，也不命名为交易所净流入。
- VOO 使用 Cboe VIX CSV；QQQ 使用 FRED `fredgraph.csv?id=VXNCLS`。保留实际日期与指数点单位，变化使用前 5/10 个有效交易日观测，非自然日。
- CFTC 匿名 Socrata 数据集 `gpe5-46if` 是 TFF Futures Only；`13874A` 为标普 E-mini，`209742` 为纳指 E-mini。资管、杠杆资金分别用 long-short，单位张，周变化要求观察日相隔七天，保留观察日与首次采集日。不是 ETF 持仓。
- 上述日/周源补三年，每六小时检查修订。普通 Funding 与 Special 分库序列，1/7/14 日普通 Funding 标为“已观察结算和”，不声称可变周期的所有历史结算都完整。
- Hyperliquid `metaAndAssetCtxs/activeAssetCtx` 当前样本随既有采样持久保存 OI（BTC）、markPx（USD/BTC）、资金费率及 dayNtlVlm（USD，滚动日成交额）。响应缺源时刻时保留 null，样本时间仅表示采集；OI 从启动积累。`fundingHistory` 分页补 180 天，1/7/14 日按小时覆盖核对；不生成历史 OI 或历史爆仓。
- Bybit `allLiquidation.BTCUSDT`：Buy 表示被爆多仓，Sell 表示被爆空仓；`v*p` 使用破产价，是 USDT 名义金额估算。重复消息去重，同一消息的相同条目仍各自保留。1/7/14 日汇总附连续订阅覆盖与缺口，日/周报告展示；无事件且已覆盖才可说明真实零，未覆盖仅给“已采集部分”。不与 Binance 采样爆仓相加为全市场。
- HTTP 按 `Retry-After` 或指数退避，任务独立失败。WebSocket 确认订阅、心跳和失联检测，连接恢复后开始新的覆盖区间；不会跨断线补零。状态分正常、积累中、窗口不完整、过期、来源暂缺和不适用。
- `python3 btc_push.py --collect-history` 只执行一次到期历史任务；`--history-status` 只读进度、失败及跨日验收计数。正常守护进程同时运行增量任务和公开流。14 个自然日、每个股票 10 个交易日和一次新 CFTC 观察日是时长条件，仍须检查真实缺口恢复与源健康；功能测试或单次连接成功不等于稳定性通过。

## 公共宏观与财政口径

- US10Y 首选 Yahoo Finance `^TNX`，缺失或过期时使用 US Treasury `daily_treasury_yield_curve/BC_10YEAR`；真实收益率使用 Treasury `daily_treasury_real_yield_curve/TC_10YEAR`，属于 10 年期 TIPS 实际收益率。DXY 使用 Yahoo `DX-Y.NYB`。利率及利率变化分别为百分比和百分点；美元指数变化为百分比，不做同一单位转换。
- `WALCL` 为 FRED 周度 Fed 总资产，`M2SL` 为月度 M2；保留原观察日期及实际采集时间。月度序列的月初日期表示观察月份，不是当天发布。FedWatch 仅在已有配置启用时取数，未配置继续明确标注未启用。
- Treasury Fiscal Data `v1/accounting/od/buybacks_operations` 同时包含预告和结果。[TreasuryDirect 定义](https://www.treasurydirect.gov/help-center/faqs/buyback-faqs/)区分预告最大面值与结果实际接受面值；`max_par_amt_redeemed` 不能代替 `total_par_amt_accepted`。最新完成操作及近期汇总只使用操作日不晚于美国东部当前日期、实际接受金额为有限非负数的结果；零金额是有效结果，空值或字符串 `null` 尚不能证明完成。有效证据历史只收录这些结果，不能把未来预告映成最近已执行回购。
- Treasury Fiscal Data `v1/accounting/mts/mts_table_1` 的同一观察日包含两个财年分组，`Year-to-Date` 名称会重复。先按观察日计算当前美国财年（10 月进入下一财年），再使用 `FY <年>` 父项的 `classification_id` 与子项 `parent_id` 选择唯一 YTD；`line_code_nbr` 只标识展示行，不能证明财年。缺失或多义时保留不可用，不回退到上一财年。依据为官方 [Monthly Treasury Statement](https://fiscal.treasury.gov/accounting/monthly-treasury-statement) 的 Table 1 及实际 API 父子关系。
- “利息/收入”采用 `v2/accounting/od/interest_expense` 的各分项总利息 FYTD 除以 MTS 当前财年财政收入 YTD，属于国债总利息口径，不能等同预算净利息占比。两源发布进度不同时，按较早截止日重新取得另一源同月数据，保存比率的财年、截止日和实际分子/分母及来源历史；最新利息补充数据另保留其日期。无法配对时不混算，保留已有且同季度的 FRED 年率比率并标注口径和缺口；FRED 两季度不一致时也不计算。
- 宏观缓存的计算版本参与共享身份；财政或回购计算修正后使旧版本失效，不将旧数值重新标成新采集。原已发送公共简报及其查询保持原文，下一份使用新结果。

## 公共事件与新闻归属

- 来源为 [华尔街见闻快讯](https://wallstreetcn.com/live) 的公开页面接口 `https://api-one-wscn.awtmt.com/apiv1/content/lives`；参数 `channel=us-stock-channel,client=pc,limit=100`。2026-09-30 实测包络 `code=20000`、`data.items` 为列表。不是推荐文章接口，未发现稳定版本承诺；失败只降级事件模块，不改用其他来源。
- 只接受 `type=live,is_priced=false` 且属于 `us-stock-channel` 的条目；`id` 对应 `https://wallstreetcn.com/livenews/<id>` 原文。`title` 是原生中文标题，缺失时用 `content_text` 首段；不生成解读。`display_time` 是**来源显示时间（Unix 秒）**，不是抓取时间或已证明的事件发生时间；只接受最近 24h、非未来、有限数值，缺失时舍弃。
- 选择政策、经济数据、财报、监管和突发事件，不要求 QQQ、VOO 或指数名称。检查标题及正文推广导流，排除荐股、收益宣传、付费、日历/综合摘要和纯行情涨跌。标题含“广告收入”“付费用户”不等于推广。仅加密主题归 BTC，不在公共事件模块重复展示。
- 原始响应使用 5 分钟进程缓存，键包含来源、接口、频道、参数和语言；每次使用仍按源时间检查 24h 时效。公共事件最多 3 条，原文链接去掉营销参数。采集失败显示“事件新闻暂不可用”，正常采集但没有新增合格事件显示“无新增事件”。
- 每次正式采集的状态、检查时间、HTTP 状态、来源包络代码、失败阶段和错误类别保存到 outbox 的 `common:news_collection`，仅保留最近一次检查及最近一次失败，并写入已有运行日志。成功不会清掉上次失败；报告投递后的 commit 清理也不删除诊断。诊断不保存响应正文或异常原文，不改变发送、去重及 UNKNOWN 恢复语义。历史未留存的失败原因保持未知，当前重试成功不能证明历史根因已解决。
- 公共去重为 48h，身份同时使用源 id 与规范化标题。只记录实际展示的事件；事件与正文在同一 outbox 入队事务中固化。`common:news_seen` 保存已确认发送记录，尚未应用的 outbox 保存待发送、发送中和 unknown 的预留身份。重试不重新取新闻；unknown 不自动释放。确认未送达且报告过期才可重新选择；晚到回执合并去重记录，不覆盖较新的公共简报。
- BTC 沿用 BlockBeats 中文快讯、原条数和 48h 去重；只保留明确加密主题，美元、利率、ETF、交易所、合约等泛词不能单独认定为加密新闻，不用无关消息补齐。QQQ、VOO 正文和附件完整文字只引用公共简报；休市查询按原时间/行情重渲染缓存，去掉旧推荐文章，不改原状态或历史已发送附件。

## 状态、参数和证据边界

- 共享采集/结构/规则/报告函数，通过 Instrument 和上下文传入身份；默认仍为 BTC。`QQQ_`、`VOO_` 前缀可独立覆盖结构、趋势、模型、Funding、基差阈值；例如 `QQQ_STRUCTURE_LOOKBACK_H`、`VOO_PRICE_TREND_BREAKDOWN_HOURS`。形态规则使用独立的 `QQQ_STATUS_RULES_FILE`、`VOO_STATUS_RULES_FILE`，缺省复用现有规则。
- 状态默认是 BTC 原路径及相邻 `.qqq.json`、`.voo.json`；可通过 `QQQ_STATE_FILE/VOO_STATE_FILE` 指定。共享 outbox 按 `asset_id` 保存、领取、应用和重试；BTC 无参命令及报告 ID 保持兼容，股票整点 ID 带标的前缀。
- “独立”指各标的数据、分析状态和报告身份分别管理。BTC、QQQ、VOO 共用每小时整点触发规则及本小时有限重试；恢复和重启不新建额外报告，已错过的调度等待下一整点。显式启动参数 `BTC_PUSH_IMMEDIATE_FIRST` 保持原有用途，默认关闭。定时报告按身份中的小时过期，不按采集完成或入队时间判断；手动命令不受此过期规则影响。旧 `:resume:` 调度意图取消，待发送正文及附件过期；历史记录、已发送回执和 unknown 结果保留，不自动重发。
- `/btc /qqq /voo` 独立查询；`/force [BTC|QQQ|VOO]`、`/repair [BTC|QQQ|VOO]` 无参数仍为 BTC。未知发送结果保留 unknown，仅已确定可重试的失败自动重试。
- 公共宏观与事件每小时独立发送一份，投递范围 `COMMON`、身份 `common:hour:<小时>`；COMMON 不是交易标的，不受股票休市影响。`/macro` 返回最近已发送公共简报及原时间，查询不重采或消费事件。重启丢弃旧小时待发公共简报，不重复发送结果未知的消息。
- 同小时宏观快照与缓存共用，在 outbox 的 `common:macro` 保存；初次运行复用现有有效宏观缓存，保留实际源日期与采集时间。各标的仍独立使用自身价格与资金判断影响。正文及完整报告文字仅引用公共简报；JSON 附件继续携带当时宏观测量。公共简报仅有文字，不参与标的最新证据选择。
- `BTC_COLLECTOR_EVIDENCE_V2` / `MARKET_COLLECTOR_EVIDENCE_V2` 增加公开历史、来源、覆盖、单位、代理关系与 UNKNOWN 复权；所有新字段为辅助观察、`analysis_eligible=false`，不改交易阈值。读取端复算日周指标，未知或身份错误的数据不进入投影。V1 历史仍可读，BTC V2 包含原 BTC V1 测量。
- 部署顺序为实际读取端先支持完整 V2，再重载采集端。新版 BTC/QQQ/VOO 统一投递 V2；`BTC_PUSH_EVIDENCE_ENABLED` 仍是显式关闭全部附件的总开关，旧 `MARKET_EVIDENCE_V2_READER_READY` 不再使股票附件静默跳过。便携读取器支持 V2 不等于外部部署已经升级。
- 股票 V2 的 `datasets` 保留原 `stock_1h / stock_quote / derivative_oi / funding / dividend_adjustments / basis / macro`，同时保留 `instrument / sources / quality / applicability / mechanical_observation`；`measurements` 补充新日周数据和公开 5 分钟行情。公开股票价格与小时行仅按 V2 的 WSS 来源、USD/股和 UNKNOWN 复权校验，不嵌成 V1 ADJUSTED。机械观察不进入测量投影，宏观保持原始数值行的既有投影口径。
- 新增日周数据只进入结构化附件，正文保留原模块与短周期内容。原始观察缓存 `latest_observation_report` 独立于价格确认资格；休市、预热或单源失败仍生成附件，缺失数据域保留状态与原因。原始观察不自动成为 `latest_valid_report` 或恢复价格确认。旧来源的行情、报告与锚点继续隔离。
- 股票预热时保留实际报价与合约观察，不重复输出没有有效结果的价格形态、当前确认和关键位段落；历史与复权限制集中说明一次，详细原因保留在附件。价格确认资格满足时按原顺序恢复这些模块，判断规则不变。
- 读取端默认只导出 BTC；股票须显式 `observations(asset_id="QQQ"/"VOO", latest_only=True)`。选定标的的最新附件无效/待下载时返回空，不能回退旧报告，也不能覆盖其他标的。
- 源码验证、实际投递与外部消费者验收分别核实；本机采集运行事实见联合链路说明。现有外部 reader 的部署路径可能随机器变化，不能把历史文档路径当成已部署事实。Git 交付按用户授权执行。

## 官方依据与尚未提供的字段

- [币安股票时段](https://www.binance.com/en/support/faq/detail/a7469c7703524024b5bc2d492b03639d)
- [币安停牌和交易状态](https://www.binance.com/en/support/faq/detail/ec85352ea8d24a3fbcf494fd1c85fd76)
- [股票合约股息调整](https://www.binance.com/en/support/faq/detail/7ced719b5e9a4859a1864c2fe657309f)
- [币安 API 变更说明：rateType](https://developers.binance.com/zh-CN/docs/products/derivatives-trading-coin-futures/change-log)
- [NYSE 交易时间与节假日](https://www.nyse.com/trade/hours-calendars)
- [股票公开 WebSocket](https://developers.binance.com/en/docs/catalog/advanced-trading-stocks-trading/api/ws-streams/market-streams)
- [Binance 公开历史文件](https://github.com/binance/binance-public-data)
- [Cboe VIX CSV](https://cdn-api.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv)、[FRED VXN](https://fred.stlouisfed.org/series/VXNCLS)
- [DefiLlama API](https://api-docs.defillama.com/)、[CFTC TFF](https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm)
- [Bybit All Liquidation](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation)、[Hyperliquid Info](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals)

官方文档或开放数据不构成可用性承诺。ETF 自身申赎/持仓、股票真实 Spot CVD、股票历史复权以及全市场爆仓仍是未完成项；任何代理指标都不改变这些缺口的名称或含义。
