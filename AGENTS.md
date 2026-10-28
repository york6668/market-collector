# market-collector

本仓维护行情采集、机械指标、质量记录、简报、证据附件、投递和状态恢复。先读 [README.md](README.md)，具体采集契约和本机运行说明见 [roles/COLLECTOR_DEPLOYMENT.md](roles/COLLECTOR_DEPLOYMENT.md)。

- 通用用户偏好和 York Skills 统一读取共享仓库 [codex-framework](https://github.com/york6668/codex-framework) 的 `york-worktree/Codex框架共享` 发布分支，从其 `AGENTS.md` 进入。本仓只引用该框架；按当前环境发现已安装或检出的框架位置，不假定另一台设备的路径可访问。
- 采集专属契约保留在本仓的 `mappings/`、`docs/` 和实际代码中；Markdown 供模型理解，程序不解析这些说明。
- 保留 BTC/QQQ/VOO 独立身份、COMMON 规则、证据 V1/V2、来源代理与质量降级语义。附件校验通过不代表独立来源已核验，最新附件无效不能用旧报告冒充最新。
- `UNKNOWN` 投递不自动重发。恢复保留历史、游标、已完成报告和未知回执；数据、凭据及实际机器配置留在本机。
- 采集端不维护分析策略、个人账户事实或分析任务配置。共享证据模块保留现有模块名与接口，消费端以明确提交引用。
- 使用 Python 3.9 及以上；独立重建和离线验证入口为 `python3 -B roles/validate_repository.py`。测试使用临时状态、空发送凭据和阻断网络，不改 HOME。
- 本仓由 Codex 自主维护、验证、提交和推送，无需逐次人工审核；未指定其他交付分支时沿用 `york-worktree/采集端独立维护`。源码发布和本机服务启用分别处理。

- 遵守 codex-framework 的源码与数据边界：合成或脱敏测试数据也只保留在本机；测试程序与构造逻辑可提交，数据文件不进入 Git 或源码导出。
