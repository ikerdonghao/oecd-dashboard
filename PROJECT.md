# OECD Infographic Dashboard — 项目说明书

任何人（包括 AI 编程工具）动手改这个仓库之前，先读完这份文件。

## 这个项目是什么

一个公开的静态网页，把若干个 OECD 宏观/数字经济指标做成一张张 infographic 卡片：
每张卡 = 一个指标的大数字 + 趋势箭头 + 迷你走势图 + 一句话解读。
目的：练一条真实的外部数据管线（ETL + 定时更新 + 静态发布），并作为数据分析作品展示。

## 架构（单向数据流，不要改成双向）

```
OECD SDMX API (sdmx.oecd.org, 免费, 无需 key)
  │  GitHub Actions 每天跑 etl/fetch_data.py
  ▼
data/*.json + data/meta.json  (提交回仓库)
  │  GitHub Pages 托管
  ▼
index.html 读 JSON 渲染卡片
```

为什么不用"打开网页时实时调 API"：OECD 数据是月度/季度发布，实时拉取没有收益；
且前端直连慢、无缓存。定时 ETL 是主干。页面必须显示 meta.json 里的数据更新时间。

## 目录约定

- `config/datasets.json` — 数据集注册表（唯一事实源）。加新卡先在这里加一条。
- `etl/fetch_data.py` — ETL 脚本，只用 Python 标准库，不许引入 pip 依赖（保持 Actions 简单）。
- `data/` — ETL 的输出目录，人不要手改里面的 JSON。
- `index.html` — 前端单页（MVP 阶段保持单文件，图表用 ECharts CDN）。

## ETL 的安全规则（不许破坏）

1. 某个数据集拉取失败时，保留它上一份 data/*.json，绝不写空文件覆盖。
2. 查询必须带收窄的 filter（见 config），不许发无过滤的大查询（会超时）。
3. URL 里不许出现空的 `startPeriod=` 参数——OECD 的接口对空参数返回 403
   （2026-11-09 实测）。要限制历史长度就在本地截断（脚本已如此处理）。

## 数据集清单（第一批）

| id | 指标 | 状态 |
|---|---|---|
| cpi_can | 加拿大 CPI 同比 | ✅ 查询已验证 |
| cli_can | 加拿大综合领先指标 CLI | ✅ 查询已验证 |
| unemployment_can | 加拿大失业率 | ✅ 查询已验证（DF_KEI） |
| retail_can | 加拿大零售贸易量 | ✅ 查询已验证（DF_KEI） |
| sahm_can | 加拿大萨姆规则衰退指标 | ✅ 查询已验证（DF_SAHM） |

加新数据集的流程：在 Data Explorer 网页里筛好 → Developer API 面板复制 query →
先用小脚本验证能拉到数 → 再写进 config/datasets.json。

## 工作纪律

- 同一时间只有一个写手（人或 AI 工具）在改代码；切换前先 commit。
- 每个小任务一个 commit，commit message 说清做了什么。
- workflow 文件（.github/workflows/*）受 GitHub 保护，只能由仓库主人在网页端创建/修改。
- 每张卡角落标注来源 "Source: OECD Data Explorer"（OECD 使用条款要求）。

## 里程碑

- [x] Spike 验证（2026-11-09，见 oecd-dashboard-spike 仓库）：拉数 / CORS / Actions→Pages 链路全通
- [ ] MVP：5 张卡 + 每日自动更新 + Pages 上线
- [ ] V1：国家切换（加/美/OECD）、解读文案半自动、10–12 张卡
- [ ] V2：CLI 与 IAB 广告市场数据对齐分析图
