# 采集端契约与本机运行

通用 Codex 规则和方法从 [codex-framework](https://github.com/york6668/codex-framework) 的 `york-worktree/Codex框架共享` 分支读取。本文件仅维护采集职责、数据契约和本机操作；具体采集代码以 `york6668/market-collector` 的指定提交为准。

## 采集职责与数据契约

采集端取得公开行情和事件，计算机械指标，记录来源、历史及质量，并生成原有行情简报与证据附件。独立研究、组合判断、账户事实及策略结论由分析端维护。

- BTC、QQQ、VOO 的状态、调度、报告和投递身份独立，COMMON 维护共享宏观和事件简报。VOO 股票身份为 `EQ_VOO`，SPY 衍生数据保持 `proxy`。具体来源、单位、时段与缺口见 [市场 mapping](../mappings/binance_markets.md)。
- 保留来源发布时间、观察窗口、抓取时间和质量原因。抓取或恢复时间不能替代来源时间；休市、预热、过期、缺失、复权 UNKNOWN 如实传递。
- 保留 Telegram 正文、证据 V1/V2 附件及其回复关系；正文、附件 message_id 和版本共同确定身份。V2 原始数据域及测量均保留。
- `valid_auxiliary` 只代表附件校验通过，独立来源计数仍为 0。重复、乱序和重放按既有身份去重；最新附件无效或待下载时不以旧报告冒充最新。
- 保持现有小时节奏：重启不补发错过小时，复市不额外触发行情简报。生成、入队、发送和实际送达分别记录；UNKNOWN 投递保留原状态，不自动重发。

## 本机配置与运行

复用现有 `btc_push.py` 入口和本机 Python。`roles/collector.example.json` 使用占位路径；创建机器配置时填写实际代码目录、解释器、日志、状态、写入锁和服务标签。状态路径可以继续使用原位置，不随源码仓库搬迁。

```sh
python3 roles/role_support.py check --config /absolute/collector.local.json
python3 roles/role_support.py render-service --config /absolute/collector.local.json /absolute/candidate.plist
python3 roles/role_support.py status --config /absolute/collector.local.json
```

`render-service` 只生成候选 plist，不安装服务。正式启动入口 `run --config ...` 才执行配置中的采集程序。现有 LaunchAgent 保持本机常驻方式，采集线程持续运行、程序内部按小时调度；不要另加每小时启动一次的竞争实例。

凭据沿用本机现有方式。新配置的 `private_environment_file` 指向仅本机所有者可读写（0600）的 JSON，存放 `BOT_TOKEN`、`CHAT_ID` 等环境值；公开配置只保存引用。渲染的 plist 不嵌入这些值。机器配置、私有文件、状态及备份均放在源码目录之外。

未来需要启用新版本时，在目标机器先导出指定提交、验证依赖和配置，再检查原服务及实际进程。保存原代码/plist，停止原服务并等待进程退出、写入锁释放，随后切换代码并启动唯一实例。沿用原数据、调度、频道及凭据；等待下一轮自然执行验证采集和投递。本次源码抽离本身不执行这些服务操作。

## 本地状态与恢复

完整恢复集合包括 BTC/QQQ/VOO 主 JSON、liquidation 和 Hyperliquid 侧车、delivery SQLite、market history SQLite，以及仍被引用的历史归档。COMMON 的快照、去重与调度记录保存在 delivery 库。归档若放在额外目录，将其加入本机配置的 `state_paths`，保留可解析引用。

备份前停止所有相关写入者。helper 取得配置中的 writer lock，检查复制期间文件是否改变，在临时副本中恢复 SQLite WAL，生成独立可读的本地备份；并发写入或未恢复的 rollback journal 会明确失败。

```sh
python3 roles/role_support.py snapshot --config /absolute/collector.local.json /absolute/private-backup
python3 roles/role_support.py restore --config /absolute/new-collector.local.json /absolute/private-backup
```

恢复配置保留全部逻辑状态名，目标必须是未占用位置。恢复不覆盖或合并活动数据，不启动服务；历史不能由模型补造。恢复后核对已发送、待发和 UNKNOWN 的原身份及内容。回滚代码时保留期间新增的业务状态；如确需回滚数据，先单独保存新结果并核对投递差异，避免旧快照造成重复发送。

## 验证边界

`python3 -B roles/validate_repository.py` 从显式源码清单重建采集目录并执行全部离线测试，覆盖数据质量、证据兼容、独立标的、状态恢复、重放及 UNKNOWN 不重发。它不读取正式状态、不调用真实来源或 Telegram。分析端接入和模型判断不属于本仓的验证范围；源码测试、Git 发布、本机启用及自然运行分别确认。
