# QBot 分析平台总体架构设计

> Review 决议：[QBot 分析平台 Review 决议](../../reviews/2026-07-10-analysis-platform-review-resolution.md)
>
> 市场事件调研：[市场事件采集与分析框架调研](../../research/2026-07-10-market-event-framework-research.md)

## 1. 目标

在不破坏现有扫描、报告和模拟交易链路的前提下，建设一个可验证、可回放、可审计的分析平台。

平台包含四个深模块：

```text
MarketSnapshotModule
PatternEngine
EventIntelligence
DecisionSupport
```

设计目标：

- 所有研究输入满足 point-in-time 语义。
- 研究环境可以快速迭代，但线上结果必须确定、可复现。
- 大模型不直接决定选股分数或交易动作。
- 研究和事件任务失败不能影响现有扫描、报告和模拟账户。
- 新功能先 shadow 运行，达到发布门槛后再成为默认入口。
- 新分析结果在现有自动交易 P0 风险修复前不得接入交易账户。

## 2. 架构决策

采用：

> Rust 生产内核 + 独立 Python 研究 Worker + PostgreSQL 版本化合同 + Parquet 历史训练矩阵。

### 2.1 Rust 生产进程负责

- 交易日任务编排。
- point-in-time 数据完整性检查。
- 最近在线特征和市场快照。
- 已发布模式加载和当日匹配。
- 事件证据、事实图和市场观察。
- API、Telegram、审计和降级。
- 幂等、任务状态和运行锁。

### 2.2 Python Worker 负责

- 历史训练矩阵构造。
- 强势股标签和匹配对照组。
- 模式原型发现。
- 判别能力验证。
- Purged Walk-forward 和 Embargo。
- 模式解释、基线比较和候选版本导出。

Python Worker 由独立 systemd timer、cron 或容器任务启动。Rust 主进程不得直接启动或托管 Python 子进程。

### 2.3 PostgreSQL 负责

- 现有业务表继续保存最新/current-state 数据，保持扫描和图表兼容。
- 新增 append-only 版本表保存每次数据观测及其 `available_at`。
- 研究、回放和模型训练只读取版本表，不读取会被覆盖的 current-state 表。
- 证券状态、行业归属和市场快照。
- 任务运行状态。
- 模型注册和发布状态。
- 最近在线特征和候选结果。
- 事件证据、修订和审计。

### 2.4 Parquet 负责

- 五年以上历史特征矩阵。
- 标签和匹配对照组。
- Walk-forward 数据切片。
- 训练中间产物。

每个数据集必须有 manifest：

```text
dataset_version
schema_version
feature_version
created_at
data_cutoff
available_at_cutoff
row_count
date_range
file_checksums
input_fingerprint
```

### 2.5 大模型负责

- 新闻或公告的结构化候选抽取。
- 已生成事实图和影响假设图的文字表达。

大模型不得：

- 生成无来源事实。
- 自动列出非直接实体的“受益股名单”。
- 修改模式得分。
- 直接决定交易动作。
- 在观察市场后回写最初影响假设。

## 3. 顶层数据流

```text
Raw market data
      |
      v
MarketSnapshotModule
      |
      +------> recent online features ----------+
      |                                          |
      +------> versioned Parquet datasets        |
                                                 v
                                      independent Python worker
                                                 |
                         archetypes + controls + validation
                                                 |
                                      candidate model versions
                                                 |
                                       manual publish gate
                                                 |
                                                 v
                                          PatternEngine
                                                 |
                                                 |
Official source + Manual input                   |
              |                                  |
              v                                  |
       EventIntelligence                         |
 evidence -> claims -> deltas -> hypotheses      |
              |                                  |
              +---------- event context ----------+
                                                 |
                                                 v
                                         DecisionSupport
                                    shadow candidates + brief
```

## 4. 深模块

### 4.1 `MarketSnapshotModule`

外部接口：

```text
build_trade_date(trade_date) -> MarketSnapshotBuildResult
get_snapshot(trade_date) -> MarketSnapshot
data_status(trade_date) -> MarketDataStatus
```

内部隐藏：

- 复权处理。
- 历史证券状态。
- 历史行业归属。
- 指数和市场宽度。
- `available_at` 对齐。
- 数据完整性和输入指纹。

线上调用方不得直接拼接多个历史表构造研究输入。独立 Python Worker 通过版本表和固定 SQL 合同生成 Parquet 数据集，并保存 manifest。

### 4.2 `PatternEngine`

内部包含：

```text
PatternArchetypeDiscovery
PatternDiscriminativeValidator
PatternMatcher
CandidateRanker
```

线上外部接口：

```text
match_market(trade_date, pattern_set_version) -> Vec<PatternCandidate>
explain_candidate(candidate_id) -> CandidateExplanation
```

第一版只发布一周和一月 shadow 候选。

### 4.3 `EventIntelligence`

外部接口：

```text
ingest(source_item) -> IngestResult
submit_manual_event(manual_input) -> EventEvidence
process_pending(cutoff) -> EventProcessingSummary
build_daily_brief(trade_date) -> DailyEventBrief
get_event_detail(event_id) -> EventDetail
```

内部隐藏：

- 精确和近重复。
- 事件提及和事件簇。
- 公告与一般新闻双轨抽取。
- ClaimGraph。
- EventDelta。
- ImpactHypothesisGraph。
- 实体映射。
- 市场观察和事件类型历史基线。

第一版只实现证据、重复、直接实体、ClaimGraph 和事实简报。

### 4.4 `DecisionSupport`

外部接口：

```text
build_daily_decision_support(trade_date) -> DailyDecisionSupport
get_candidate_detail(candidate_id) -> DecisionCandidateDetail
```

职责：

- 组合现有 `scan_ranker` 基线、新模式 shadow 候选和事件上下文。
- 输出候选分层、风险和失效条件。
- 明确每项信息属于事实、计算、推断或未知。
- 不直接操作模拟账户或交易账户。

第一版事件评分权重为 0，仅用于解释。

## 5. 目录结构

```text
src/
├── analysis/
│   ├── mod.rs
│   ├── contracts.rs
│   ├── orchestration.rs
│   ├── market_snapshot/
│   │   ├── mod.rs
│   │   ├── builder.rs
│   │   ├── adjustment.rs
│   │   ├── availability.rs
│   │   └── export.rs
│   ├── patterns/
│   │   ├── mod.rs
│   │   ├── model.rs
│   │   ├── repository.rs
│   │   ├── matcher.rs
│   │   ├── ranking.rs
│   │   └── explanation.rs
│   ├── events/
│   │   ├── mod.rs
│   │   ├── evidence.rs
│   │   ├── dedup.rs
│   │   ├── claims.rs
│   │   ├── deltas.rs
│   │   ├── hypotheses.rs
│   │   ├── entity_linking.rs
│   │   ├── market_observation.rs
│   │   └── reporting.rs
│   └── decision_support/
│       ├── mod.rs
│       ├── builder.rs
│       └── explanation.rs
├── api/
│   ├── mod.rs
│   ├── routes.rs
│   ├── pattern_routes.rs
│   └── event_routes.rs
└── storage/
    ├── mod.rs
    ├── postgres.rs
    ├── market_repository.rs
    ├── pattern_repository.rs
    └── event_repository.rs

research/
├── pyproject.toml
├── qbot_research/
│   ├── cli.py
│   ├── datasets.py
│   ├── labels.py
│   ├── controls.py
│   ├── archetypes.py
│   ├── validation.py
│   ├── baselines.py
│   ├── export.py
│   └── contracts.py
└── tests/
```

外部只暴露四个深模块。文件划分是内部实现，不要求调用方理解去重、聚类或特征细节。

## 6. Point-in-time 时间模型

所有可用于研究或回放的数据必须具备：

```text
occurred_at
published_at
first_seen_at
ingested_at
available_at
effective_trade_date
source_updated_at
```

规则：

- `available_at` 决定回测是否可使用该数据。
- `ingested_at` 记录 QBot 实际抓取或回补时间，不能替代历史 `available_at`。
- 后续修订必须追加版本，不能回写成历史时点已知。
- 收盘后发布的信息只能用于下一交易日。
- 非交易日发布的信息映射到下一可交易日。
- 历史行业归属按有效期读取，不能使用当前归属解释过去。
- 证券状态按交易日读取，不能只使用当前 ST 或退市状态。

## 7. 数据前置条件

Phase 1 前必须具备：

- append-only 日行情版本。
- 每日市值、流通市值、换手和估值版本。
- 历史证券主数据、上市和退市日期。
- 复权因子和公司行动。
- 每日 ST、停牌和价格限制。
- 历史行业/概念成员。
- 主要指数和行业指数历史。
- 每日市场宽度。
- 数据可用时间和来源。
- 数据完整性结果。

数据缺失时，系统必须拒绝发布新模型，不允许以默认值静默填充关键条件。

价格特征统一使用明确版本的前复权序列；成交模拟仍使用未复权真实价格。复权公式、因子来源和价格口径必须写入 `feature_version`。

## 8. 模型和事件状态

### 8.1 模式版本

```text
draft -> validated -> approved -> published -> retired
```

训练完成后禁止自动发布。

### 8.2 事件处理

```text
collected -> deduplicated -> extracted -> published
                                \-> rejected
```

后续阶段扩展为：

```text
collected -> deduplicated -> clustered -> extracted -> reasoned -> published
```

### 8.3 事件生命周期

```text
emerging -> active -> cooling -> closed
              \-> contradicted
```

市场状态单独保存：

```text
not_observed
market_aligned
market_contradicted
ambiguous
confounded
expired
```

## 9. 任务编排

### 9.1 每日生产链路

```text
17:00 现有行情更新
17:20 数据完整性检查和 MarketSnapshot
17:30 现有扫描
17:40 已发布模式 shadow 匹配
17:45 事件证据处理
17:50 事实简报和市场观察更新
18:00 DecisionSupport 日报
20:05 归档
```

任一新分析任务失败时：

- 不影响旧扫描和旧日报。
- 继续使用最后一个有效模型。
- 报告明确显示缺失模块和最后成功日期。

### 9.2 独立研究任务

Python Worker：

```text
每周构造训练数据
-> 一周/月度模式训练
-> Purged Walk-forward
-> 基线比较
-> 保存 draft/validated
```

第一版禁止 Rust 调度器直接调用 Python。

## 10. 分阶段实施

### Phase 0：Point-in-time 数据和合同

不产生新候选。

### Phase 1：强势股模式 shadow 引擎

- 一周和一月。
- 连续趋势、波动收缩突破、超跌反转。
- 原型发现 + 对照判别。
- 与简单基线和 `scan_ranker` 比较。
- 只输出 shadow 候选。

### Phase 2：事件证据 MVP

- Telegram/REST 人工输入。
- 一个官方事实源。
- 原始证据。
- 精确和近重复。
- 固定事件 Schema。
- 直接实体。
- ClaimGraph。
- 每日事实简报。

### Phase 3：事件演化和市场观察

- GDELT。
- 事件提及和事件簇。
- 两阶段聚类。
- EventDelta。
- ImpactHypothesisGraph。
- 市场异常收益和状态。
- 事件类型历史基线。

### Phase 4：有限融合

- `DecisionSupport` 统一展示。
- 事件最初只作为解释。
- 验证后最高 `±5` 分。
- 不接自动交易。

## 11. 现有系统迁移

### 11.1 `scan_ranker`

- 保留现有输出。
- 作为模式引擎基线。
- 新模式达到发布门槛后，再决定替换哪些旧票池。
- 不长期维护两套同义默认排序。

### 11.2 `AiAnalysisService`

迁移路径：

```text
旧接口
-> 转发到 DailyDecisionSupport / DailyEventBrief
-> 保持一个兼容周期
-> 删除旧自由 Prompt 分析
```

禁止继续在该文件加入事件表、聚类或因果逻辑。

### 11.3 自动交易

新分析结果在以下问题修复前不得接入：

- 满仓探索。
- 手续费和滑点缺失。
- 行情新鲜度。
- 交易日历。
- 组合风险。
- 涨跌停和不可成交。

## 12. 测试策略

### Rust

- point-in-time 查询测试。
- `available_at` 边界测试。
- 复权和证券状态测试。
- 模型合同和 matcher golden tests。
- 事件证据和 ClaimGraph 测试。
- 市场状态不被误写为因果确认。
- 幂等和降级测试。

### Python

- 标签无未来数据。
- 匹配对照组。
- Purge/Embargo。
- 基线比较。
- Lift 和覆盖率。
- 模式跨窗口稳定性。
- 模型导出合同。

### 跨语言

- 固定 fixture。
- 相同特征名、类型和缺失值规则。
- Python 导出模型，Rust 加载并产生固定匹配结果。
- Schema 不兼容时 Rust 必须拒绝加载。

## 13. 发布门槛

### 模式

- 有足够独立验证样本。
- 多数窗口具有正 Lift。
- 成本后优于至少一个简单基线。
- 回撤、换手和容量满足配置。
- 不由少数股票或市场阶段驱动。

### 事件

- 发布事实证据覆盖率 100%。
- 无证据事实数量为 0。
- 非法 Schema 进入发布层数量为 0。
- 直接实体映射精确率目标不低于 95%。
- 保守自动合并精确率目标不低于 90%。

## 14. 非目标

第一版不做：

- 年度预测模型。
- 事件直接选股。
- 新模型自动发布。
- Rust 托管 Python Worker。
- Kafka、Flink 或图数据库。
- 深度学习价格预测。
- 券商实盘下单。
- 一次性实现完整平台。
