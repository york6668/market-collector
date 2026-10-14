# BTC_COLLECTOR_EVIDENCE_V1

兼容说明（2026-09-30）：本文件保留 V1 历史契约。新公开历史使用 `BTC_COLLECTOR_EVIDENCE_V2` / `MARKET_COLLECTOR_EVIDENCE_V2`，实现在 `market_evidence_v2.py`，来源、启用条件与口径见[币安市场映射](../mappings/binance_markets.md)。读取器同时支持 V1/V2；外部部署确认支持 V2 前，新附件开关保持关闭。股票新公开流的复权为 UNKNOWN，不得通过 V1 的 ADJUSTED 校验器包装发送。

实现 Owner：采集端 `btc_evidence.py`；读取端消费同一版本及固定样本。与策略 revision 3 决策 Schema 分离。

可选 `collection_metrics` 保存阶段耗时、公共请求计数、去除query的endpoint和错误分类，仅用于运行测量，不进入策略事实投影。报价源时间与抓取时间分别保存。

UTF-8 JSON，无 NaN/Infinity、重复键，最大 5 MiB。根字段：`schema` 固定为 `BTC_COLLECTOR_EVIDENCE_V1`；`bundle_id` 为 `btc:` 加移除 bundle_id 后 canonical JSON 的 SHA-256；`report_id` 为稳定整点或命令身份；`generated_at` 为带时区 ISO 8601；`text_sha256` 为实际 Telegram 正文 UTF-8 字节的 SHA-256；`role` 固定 `auxiliary_observation`；`sources`、`datasets`、`quality`、`mechanical_observation`、`full_report_text`。

解码只接受严格 UTF-8，不接受 UTF-8 BOM、UTF-16/32。递归过深、巨大数值、非字符串 URL 等坏附件产生单条拒绝/降级收据，不抛穿消息批次。

Canonical JSON 使用 Python `json.dumps(ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)`。哈希只保护内容关联，不认证发布者；读取端仍使用现有频道和发送者来源限制。

`sources` 按 source_id 索引，包含 source_id/label/venue/endpoint_url/canonical_origin/access_scope/retrieved_at/as_of/source_dates/requests。未知源时间为 null，不以抓取时间替代；日期精度资料保留字段日期。来源 API、镜像和频道转发不增加独立核验来源。

`datasets` 按数据域索引，包含 source_ids、rows 或 values、units、formula、quality 和可选 window。小时窗口包含 start_ms/end_ms/timestamp_field/interval_ms/expected_count/actual_count/contiguous/complete；读取端重新计算窗口而非信任 complete。现货和合约主动成交仅为 taker-flow proxy；OI数量与名义值分离；ETF仅日级背景；稳定币字段代表供给变化。

`futures_1h`、`oi_1h`、`spot_cvd` 固定 240 根、1 小时、`timestamp_field=close_time`；所有 window 字段必须齐全。OI 观测时间另行校验与收盘时间的偏差：Binance 上限 300 秒，Hyperliquid 接收样本上限 90 秒且不得晚于收盘。真实零成交量可保留；主动买卖金额须非负、买入不超过总量，卖出和净额由读取端重算。

三核心域均为完整证据的必需数据域，缺少任何一个产生该域的 `missing_dataset`。小时开盘须为 UTC 整点；闭合时间可采用小时末毫秒 H-1 或右边界 H，校验时将两者归到同一小时边界。连续性和三域共同覆盖的 240 个小时按该边界比较，原始 close_time、OI 实际 timestamp 和 drift_ms 不改写。半小时偏移或三域覆盖小时不同均降级，缺失域不补造空数据。

OHLC 须为有限正数且满足 low ≤ open/close ≤ high；基础成交量须为有限非负数。OI 数量及名义金额须非负，新旧名义金额同时存在时必须一致，notional_currency 须符合所引用 Binance/Hyperliquid 接口的计价币种。布尔值只用于明确的标记字段，不能充当价格、成交量或金额。校验和投影共用测量约束；非法字段从辅助投影剔除，关系冲突时剔除对应 OHLC 或名义金额字段组，其他可用测量保留。原附件保留原值供审计；原始 null 在投影中仍表示缺失，不能作为零参与计算。具体失败原因保留在 validation.dataset_reasons，不改写采集端 quality。

Binance BTCUSDT 的价格及 quote/taker 数额为 USDT。OI 同时保留 `notional_value` 与 `notional_currency`；`notional_usd` 是兼容别名，Binance 时其 units 明确为 USDT，未经外汇转换。Hyperliquid 按协议 USD 报价。既有市场 `oi_usd` 接口保留同样兼容含义。

来源的字典键必须等于 `source_id`；endpoint 必须为 HTTPS 非 IP 主机，`canonical_origin` 必须等于该主机，`access_scope=public`。`as_of`、`retrieved_at`、`generated_at` 都须有时区，且按此顺序不递减。日期精度观测只保留真实观察日期；未来会议/计划日期可单列，但不能充当 source_dates 来解除源时间缺失。宏观复合数据展开各系列的真实 endpoint、字段映射和样本日期；publication 未知时仍降级，不能使用抓取时刻代替。

来源身份还须匹配实现内固定的 `source_id + venue + hostname + path` 注册表；支持当前 Binance、Hyperliquid、CoinGecko、Alternative.me、Bitbo、SoSoValue、DefiLlama、FRED、Yahoo 和 Treasury API。未知来源、任意镜像或不匹配路径降级，不能因自填 Binance 标签被当作对应 API。注册表仅验证协议身份一致性，不认证实际取数，独立核验来源仍为 0。新增 API/镜像须显式更新契约及正向样本。行观测时间不得晚于被引用来源的 retrieved_at，as_of 或日期精度 source_dates 必须覆盖对应行；OI比较实际观测 timestamp，宏观系列和 ETF 历史按各自来源比较真实样本日期，计划时间不参与此比较。

Binance Funding 的旧字段 `funding_8h_pct` 表示实际过去 8 小时已结算费率累计；8H/24H 必须有窗口开始边界样本。`fundingInfo` 仅证明当前周期，不倒推历史；混合周期、缺失费率、缺失边界均显式降级。Hyperliquid 保留实际每小时结算记录及既有 8/24 条累计口径；预测费率独立保存。

Funding 验证不能接受附件自报任意长度：Binance 历史须覆盖 240 小时及开始边界，按实际 1/2/4/8 小时间距核验，分别重算 8H/24H 窗口和累计；Hyperliquid 固定 24 条连续小时结算，重算 1H/8H/24H。Binance shadow 使用同一契约；primary 别名按所引用 API 区分协议。Basis 是单点测量，禁止附任意 window，必须由 mark_price/index_price 重算 basis_pct。未知 dataset 显式降级。

结算时标允许距离最近 UTC 整点最多 100 ms 的发布漂移。生产与便携校验共用 `btc_evidence.settlement_window`，只以对齐桶比较周期、连续性和选择边界；`rows.time`、窗口 `start_ms/end_ms`、`latest_funding_time_ms` 与来源时间都保留原值。超过容差、半小时错位、重复桶、丢桶、周期变化均不完整；原始时标和对齐桶都必须不晚于采集或生成截止。Binance 开始边界只证明覆盖，不计入累计；HL 继续使用 24 笔小时结算，不额外要求第 25 笔。当前周期元数据不能证明历史周期。

Treasury Auctions 历史只收录 `auction_date` 不晚于该响应检索 UTC 日期、且 `bid_to_cover_ratio` 为有限非负数的已完成结果，真实 0 保留；发行额合计也仅使用这些结果行。历史观测字段仅为拍卖日期与结果数值，已公告未来拍卖、issue/maturity/付款等计划或合同日期不充当观测时间，原接口返回可在独立审计样本中保留。通用未来观测日期规则不放宽，publication 未知仍降级。

ETF 旧 SoSoValue 附件按历史接口真实 `date` 排序、去重，冲突同日值视为缺失；5D/30D 分别要求 5/30 个完整的交易日观测，不能把不足样本的部分和当作完整窗口。窗口日期来自历史末样本，不能使用 current metrics 日期代替。稳定币每资产 current/day/week 样本保留在 `value.history`，原 flow 字段表示供给差；缺源时间保持缺失。

ETF 当前匿名持仓使用 `Bitbo + bitbo.io + /treasuries/us-etfs/`，沿用 `etf` dataset 与 `public_etf` source。此分支只要求有效的 12 基金范围、逐基金 BTC 数量及总量、页面更新日，`source_ids=[public_etf]`，流量历史为空。缺少美元流量不拒绝有效持仓；该来源不能提供 flow 或 NAV/AUM 比例。7D 基准须证明同来源、同范围、相差 7 个日历日及计算结果。旧流量背景不导出为当前测量。完整定义见 [Bitbo mapping](../mappings/bitbo_us_spot_etf.md)。

ETF 采集记录的 `flow_1d_source=current|history` 指明日流量实际取值接口，真实零仍属于 current；`current_retrieved_at` 与 `history_retrieved_at` 分别记录响应完成时间。证据 `field_sources.flow_1d_usd` 和该字段的 source_dates 跟随实际来源，历史回退值还须等于历史末样本。旧记录缺少此标记时保持来源未知并产生 `daily_flow_source_unknown`，不得根据数值或日期猜测来源。

`market_current` 保存当前报价，`market_cap` 保存市值，`fear_greed` 保存 Alternative.me 时间戳及历史，均与上一完整小时数据分开。CoinGecko 旧适配器未保留源时间，因此市值源时间为 null 并降级；不得使用 `current_quote_at_ms` 代替。宏观、ETF、稳定币与 Funding 原始序列统一放在采集 record 的 `value.history`，便于状态历史定点瘦身。

当前 Hyperliquid 分钟样本保留 75 小时，primary 重建更早的 OI 无完整覆盖时，240H 证据会降级；不补造历史，也不改变当前 shadow 配置或阈值。旧宏观缓存 revision 9 不具备新 provenance，revision 10 的拍卖 raw history 混有计划/合同日期；revision 11 要求下一次正常采集刷新，不手动修改正式状态。

正向回放样本由 `tests/collector_replay_fixture.py` 在测试临时目录构造为 `valid.json` 和原样 `valid.txt`：三核心数据集各 240 根，可验证为 valid_auxiliary；这是合成协议样本，独立核验来源数仍为 0。空 datasets 及仅有 complete=true 的伪完整窗口必须显式降级。

文字先发送；JSON使用 `sendDocument` multipart 并通过 `reply_parameters.message_id` 回复该文字。文字与附件分别记录 message_id；附件失败不重发文字。读取按 chat_id/message_id/编辑版本记录关联；同 bundle_id 重复到达幂等；先到附件等待正文；正文编辑后重新检验，哈希不符不沿用旧关联。无附件的旧文字保持现有解析。

公共宏观与事件简报是独立文字消息，投递范围 COMMON，不创建交易标的证据。各标的摘要和附件完整报告文字引用公共简报的小时，JSON 的 macro 数据集仍保存本报告使用的测量、原始来源和时间。公共事件只在 COMMON 展示，QQQ、VOO 移除独立新闻段；BTC 保留加密专属新闻。休市查询从原 payload 重渲染，保留原行情和生成时间，去除旧新闻文字。公共消息与标的消息交错，不改变各标的最新附件选择；无需依赖公共消息先到达。

校验状态为 rejected、pending_text、valid_auxiliary 或 degraded_auxiliary；未知 Schema/哈希不符/根结构错误 rejected，窗口或来源不完整按数据域降级，不阻断其他消息。所有状态均不表示市场事实 VERIFIED。读取端调用 `auxiliary_observation(bundle, report_text)`，必须传关联消息的当前正文；函数内部重新验证正文及附件哈希，旧 validation 字典不能替代当前正文，正文编辑或附件变更后不得复用旧投影。附件尚未匹配正文时只调用 validate_bundle 等待关联。

实际策略导出使用 `EvidenceReader.observations(latest_only=True, asset_id="BTC")`，返回指定标的最新关联文字报告当前附件版本的至多一份快照。缺省 `asset_id` 仍为 BTC。先按正文标的（缺正文时用股票附件文件名）分组，再按同频道正文 message_id、附件 message_id 降序定位最新和校验；晚到旧附件不改变报告顺序。该标的最新附件待下载、无效、未知版本或正文编辑不匹配时返回空数组，不用历史有效报告回填当前数据，也不影响其他标的。无参 `observations()` 保留 BTC 历史诊断兼容，不能作为当前覆盖快照。快照按实际 CLI 的 UTF-8 JSON 格式及结尾换行限制为 4 MiB，超过上限明确失败，不截断测量；`max_output_bytes` 可显式收紧或配置诊断输出上限。

QQQ、VOO 使用 `MARKET_COLLECTOR_EVIDENCE_V1`，实现位于 `market_evidence.py`，具体来源及单位见 [币安市场映射](../mappings/binance_markets.md)。根节点包含 `asset_id`、完整 `instrument` 身份；数据集包含 `instrument/relation`，股票小时线为 USD/share 与 shares，合约数量/名义值为 contracts/USDT。VOO 的 SPYUSDT 始终是 proxy，基差仅为 SPY 自身标记价/指数价。普通 Funding 与 Special 股息调整是不同数据集，可有相同结算时刻。源报价时间缺失必须保留 null；股票日历凭据用于核对预期休市间隔。字段、单位、时间或来源不合法的测量不进入投影。两种 schema 均保持辅助观察和独立已验证来源计数 0；旧 BTC 附件缺 `asset_id` 时仍按 BTC 校验。

策略投影仅深拷贝已知数据域的白名单测量字段、数值/时间类型及固定来源元数据，未知 dataset、自由文本、嵌套策略对象、`*_rule`/decision/regime/status 不进入投影。宏观投影保留原始日期与数值序列；复合策略结论留在原附件供审计。来源日期规范化成日期列表；源身份不匹配的元数据不投影。独立来源数固定为0，不带因果命题、策略决定或资产事实；由策略既有独立核验后另行形成自身证据。F&G 不可用或过期记录仍保留原数据及缺失质量收据，保持降级。
