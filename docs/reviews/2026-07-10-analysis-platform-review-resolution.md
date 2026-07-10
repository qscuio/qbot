# QBot 分析平台 Review 决议

> 决议日期：2026-07-10
> 适用范围：分析平台总体架构、强势股模式引擎、市场事件智能和后续实施计划

## 1. 决议目的

本文记录对现有分析平台设计进行评审后的正式调整。后续规格和实施计划均以本文为约束；出现冲突时，以更新后的专项规格为准。

## 2. 总体判断

现有方向正确，但第一版范围过大，统计验证和 point-in-time 数据准备不足。当前阶段不进入完整平台实现，先建设可验证的数据和研究闭环。

实际收益价值仍为未知。所有新模型先以 shadow 模式运行，不接自动模拟交易和实盘。

## 3. 已接受的架构调整

### 3.1 收敛为四个深模块

```text
MarketSnapshotModule
PatternEngine
EventIntelligence
DecisionSupport
```

- `MarketSnapshotModule` 提供 point-in-time 行情、证券状态、行业归属和市场宽度。
- `PatternEngine` 负责模式原型发现、对照判别、样本外验证和在线匹配。
- `EventIntelligence` 负责证据、事件演化、事实图、影响假设和市场观察。
- `DecisionSupport` 只组合候选与解释，不直接操作交易账户。

### 3.2 Python Worker 与 Rust 进程解耦

- Rust 生产进程不直接启动或托管 Python 子进程。
- Python 研究 Worker 由独立 systemd timer、cron 或容器批任务启动。
- PostgreSQL 保存任务、版本和在线结果。
- Parquet 保存历史训练矩阵和研究产物。
- Worker 失败时继续使用最后一个已发布模型版本。

### 3.3 现有模块定位

- `scan_ranker` 成为新模式引擎的基线与兼容适配器。
- 新模式候选先 shadow 运行，不与旧 A/B 票池争夺默认入口。
- `AiAnalysisService` 不再扩展；后续只做兼容转发并最终删除。
- 新分析结果在现有自动交易 P0 风险修复前不得接入交易账户。

## 4. 强势股模式引擎调整

### 4.1 第一版周期

| 周期 | 第一版状态 |
|---|---|
| 一周，5 个交易日 | 正式研究和 shadow 匹配 |
| 一月，20 个交易日 | 正式研究和 shadow 匹配 |
| 一季度，60 个交易日 | 实验，不发布在线模型 |
| 一年，250 个交易日 | 描述性画像，不作为预测模型 |

五年数据可用于一周和一月初版。季度模型需要更长历史；年度模型至少需要 10-15 年且应处理高度重叠标签。

### 4.2 模块分解

```text
PatternArchetypeDiscovery
-> PatternDiscriminativeValidator
-> PatternMatcher
-> CandidateRanker
```

聚类只回答赢家原型，不能直接证明预测能力。每个模式必须和日期、行业、规模、价格及流动性匹配的对照组比较。

### 4.3 必备统计

每个模式至少保存：

- 正样本数与对照样本数
- 基础成功率
- Precision
- Lift over base rate
- Coverage
- False positive rate
- 成本后收益
- 最大回撤
- 不同 Walk-forward 窗口稳定性
- 单一时期和单一股票贡献占比

### 4.4 验证方法

必须使用：

- 时间顺序 Walk-forward
- Purge
- Embargo
- 同一股票相邻重叠样本隔离
- 训练窗口内拟合缩放器和聚类参数
- 最终不可调参保留期

### 4.5 第一版模式范围

第一版只研究：

- 连续趋势型
- 波动收缩突破型
- 超跌反转型

板块龙头作为解释和验证特征，不先作为独立聚类类型。事件驱动型在事件历史数据稳定前不进入模式训练。

### 4.6 简单基线

新模型必须与以下基线比较：

- 20/60 日相对强度
- MA20/MA60 趋势
- 波动收缩突破规则
- 现有 `scan_ranker` A/B 票池

不能稳定优于基线的复杂模型不得发布。

## 5. Point-in-time 数据要求

任何研究实现前必须补齐：

- append-only 日行情版本
- 每日市值、流通市值、换手和估值版本
- 历史证券主数据、上市和退市状态
- 复权因子和公司行动
- ST、停牌和当日价格限制
- 历史行业和概念成员有效期
- 主要指数和行业指数历史
- 每日市场宽度
- `occurred_at`
- `published_at`
- `first_seen_at`
- `ingested_at`
- `available_at`
- `effective_trade_date`

回测能否使用数据由 `available_at` 决定；`ingested_at` 只记录 QBot 实际抓取时间，不能替代历史可用时点。后续修订不能覆盖历史认知。

## 6. 特征和研究存储调整

- PostgreSQL 保存原始数据、最近在线特征、模型注册、运行状态和审计。
- 历史训练矩阵按 `dataset_version` 写入分区 Parquet。
- Parquet manifest 保存 Schema、特征版本、日期范围、行数和校验和。
- Python 使用 Polars/DuckDB 或 PyArrow 读取训练数据。
- JSONB 仅用于低频扩展字段、解释和模型配置。
- 高频过滤及查询字段使用类型化列。

## 7. 市场事件智能调整

### 7.1 事实和推断分离

原 `EventLogicGraph` 拆分为：

```text
ClaimGraph
ImpactHypothesisGraph
```

- `ClaimGraph` 只保存有来源支持的事实节点和关系。
- `ImpactHypothesisGraph` 保存系统推断、产业传导、成立前提、反向情景和失效条件。

### 7.2 事件增量

增加 `EventDelta`：

- 新增事实
- 重复信息
- 数字修订
- 状态变化
- 相对市场预期差

每日简报优先展示“今天相对昨天新增了什么”。

### 7.3 市场观察而非因果确认

状态改为：

```text
not_observed
market_aligned
market_contradicted
ambiguous
confounded
expired
```

单独保存 `causal_confidence`。价格与逻辑一致不等于事件导致价格变化。

事件初始假设必须在观察市场反应前冻结。后续只能追加观察和修订，不能回写最初假设。

### 7.4 第一版范围

事件 MVP 只包含：

- Telegram/REST 人工输入
- 一个官方事实源
- 原始证据
- 精确与近重复
- 固定事件 Schema
- 直接实体映射
- `ClaimGraph`
- 每日事实简报

GDELT、两阶段事件聚类、`ImpactHypothesisGraph` 和异常收益验证进入后续阶段。

### 7.5 股票映射限制

第一版事件分析只输出：

- 直接涉及的上市公司
- 一级受影响行业
- 上下游方向
- 股票类型

不得由大模型自动生成“受益股名单”。非直接实体股票必须由确定性模式和相对强度筛选。

### 7.6 事件因子权重

- 第一阶段权重为 0，只用于解释。
- 完成历史验证后，最高从 `±5` 分开始。
- 事件不能绕过流动性、可交易性、价格结构和风险硬过滤。

## 8. 实施阶段

### Phase 0：Point-in-time 数据和合同

不产生新选股结果，只建设可信研究基础。

### Phase 1：强势股模式 shadow 引擎

只做一周和一月、三个模式类型、对照判别和基线比较。

### Phase 2：事件证据 MVP

人工输入 + 一个官方源 + 证据/重复/事实图/事实简报。

### Phase 3：事件演化和市场观察

增加事件簇、`EventDelta`、GDELT、影响假设和异常收益观察。

### Phase 4：DecisionSupport 有限融合

统一输出候选和事件解释；事件初始权重为 0，验证后最多 `±5`。

## 9. 发布门槛

### 模式模型

- 有足够独立验证样本
- 多数 Walk-forward 窗口具有正 Lift
- 成本后优于至少一个简单基线
- 回撤和换手受控
- 结果不由少数极端样本驱动

### 事件分析

- 已发布事实的证据引用覆盖率 100%
- 无证据事实进入日报数量为 0
- 非法 Schema 进入发布层数量为 0
- 直接实体映射精确率目标不低于 95%
- 保守自动合并精确率目标不低于 90%
- 人工锁定关系不会被自动任务覆盖

## 10. 实施计划

- [总体实施路线图](../superpowers/plans/2026-07-10-analysis-platform-roadmap.md)
- [Phase 0：Point-in-time 数据底座](../superpowers/plans/2026-07-10-point-in-time-data-foundation.md)
- [Phase 1：强势股模式 Shadow 引擎](../superpowers/plans/2026-07-10-strong-stock-pattern-shadow-engine.md)
- [Phase 2：事件证据 MVP](../superpowers/plans/2026-07-10-event-evidence-mvp.md)
- [Phase 3：事件演化和市场观察](../superpowers/plans/2026-07-10-event-evolution-market-alignment.md)
- [Phase 4：DecisionSupport 有限融合](../superpowers/plans/2026-07-10-decision-support-integration.md)

## 11. 非目标

当前不做：

- 年度预测模型
- 事件直接选股
- 新模型自动发布
- Rust 进程托管 Python Worker
- Kafka/Flink/图数据库
- 新分析结果接入自动交易
- 为追求形式完整而一次性实现全部框架
