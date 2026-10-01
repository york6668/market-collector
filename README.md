# market-collector

BTC、QQQ、VOO 和 COMMON 的独立采集仓库。负责公开数据采集、机械指标、质量记录、行情简报、证据附件、投递去重及状态恢复。数据和定时任务运行在本机，Codex 负责本仓的工程维护。

初始采集源码提取自 [market-roles 的 b66bbe0](https://github.com/york6668/market-roles/commit/b66bbe04877f298078099f2bcce1f0a1015d5660)。分析端和策略 Skill 由其原任务维护；本仓无需分析目录即可安装、测试和运行。

## 入口与归属

- [采集契约、配置与恢复](roles/COLLECTOR_DEPLOYMENT.md)、[机器配置样例](roles/collector.example.json)。
- [市场来源和产品语义](mappings/binance_markets.md)、[证据 V1](docs/BTC_COLLECTOR_EVIDENCE_V1.md)。V2 的格式、校验和投影由 `market_evidence.py`、`market_evidence_v2.py` 维护。
- [codex-framework](https://github.com/york6668/codex-framework) 的 `york-worktree/Codex框架共享` 分支维护通用偏好和 York Skills。从该仓库 `AGENTS.md` 读取具体框架，本仓不保存其正文副本。

源码、mappings、合成测试和配置样例随 Git 维护。行情历史、JSON/SQLite 状态、日志、发送回执、凭据及实际机器配置留在本机。`btc_push.py` 是稳定运行入口；源码检出、导出和测试均不会安装或重启正式服务。

## 安装与离线验证

支持 macOS/Linux、Python 3.9 及以上。已有兼容环境可直接复用；新环境使用：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -B roles/validate_repository.py
```

验证入口仅按 `roles/collector-source-files.json` 导出源码到新的临时目录，然后在那里运行全部采集测试。测试屏蔽网络、发送凭据和外部状态路径，不依赖分析目录或原机器的源码。已安装依赖的环境可直接使用 `python3 -B roles/validate_repository.py`。

修改后的针对性测试入口：

```sh
python3 -B roles/offline_tests.py test_role_export test_role_support test_btc_joint_replay
```

云端准备可运行 `sh roles/cloud-setup.sh`；需要选择解释器时用 `COLLECTOR_PYTHON=python3.11 sh roles/cloud-setup.sh`。脚本只准备本仓测试环境，不接入本机业务数据和服务。通用框架由该环境从 `codex-framework` 读取。

## 导出与引用

```sh
python3 -B roles/export_role.py collector /absolute/new/collector-source
```

目的目录必须不存在。唯一源码清单同时供目录导出和备用 JSON 源码包使用；导出中包含测试、依赖和自身的重建入口，可以再次独立验证。`roles/role_support.py` 保留原有配置、源码包及状态备份接口，备份只写本地。

消费端引用 `york6668/market-collector` 的完整提交，保留现有顶层模块名，例如 `btc_evidence`、`btc_evidence_reader`、`market_evidence_v2`、`collector_v2_reader` 和 `market_reader`。这些模块提供证据编解码、校验和辅助观察投影，不启动采集服务或新的 Telegram 更新消费者。取得指定源码后的具体消费端接入由分析任务维护；本次抽离不改变证据 V1/V2 或导出语义。

正式节点使用经过验证的明确提交和自己的配置。安装步骤见采集部署说明；本地运行版本、源码发布和自然调度验证分别确认。
