# QBot 分析平台总体架构设计

> 市场事件模块的外部调研依据：[市场事件采集与分析框架调研](../../research/2026-07-10-market-event-framework-research.md)

## 1. 目标

在不破坏现有扫描、报告和模拟交易链路的前提下，为 QBot 增加两个可独立演进的分析模块：

1. 强势股模式研究与在线匹配模块。
2. 市场事件采集、逻辑链推演与每日分析模块。

架构必须满足：

- 研究效率高，但生产结果确定、可复现、可审计。
- 大模型不直接决定选股分数和交易动作。
- 研究任务失败不能影响日常扫描、报告和模拟交易。
- 模式、参数、特征和事件结论全部有版本和证据来源。
- 两个模块通过小而稳定的接口连接现有 QBot，而不是直接互相访问内部实现。

## 2. 架构决策

采用：

> Rust 生产内核 + Python 离线研究 Worker + PostgreSQL 版本化契约 + 大模型结构化事件适配器。

### 2.1 Rust 负责

- 交易日调度和任务编排。
- 日常特征快照生成。
- 已发布模式的加载和在线匹配。
- 市场环境和交易风险过滤。
- 事件结构的确定性验证与逻辑链计算。
- API、Telegram 输出和审计。
- 失败降级、幂等和运行状态管理。

### 2.2 Python Worker 负责

- 五年历史样本构造。
- 强势股和对照组标签生成。
- 特征标准化。
- 聚类、稳定性分析和样本外验证。
- 候选模式的统计解释。
- 生成待审核的模式版本。

Python Worker 不直接下单、不直接生成线上候选，也不作为实时接口依赖。

### 2.3 大模型负责

- 从新闻正文中抽取事件实体、动作、方向、时间范围和不确定性。
- 将事件转换为严格 JSON。
- 为已经由确定性模块生成的逻辑链编写自然语言说明。

大模型不得：

- 直接修改模式得分。
- 直接给出买入或卖出动作。
- 覆盖原始来源和系统计算事实。
- 在缺少来源证据时创造事件事实。

## 3. 顶层模块

```text
Official feeds / GDELT / optional commercial feeds
                         + Telegram / REST input
                                      |
                                      v
                              Event Evidence Store
                                      |
                    Exact / near-duplicate grouping
                                      |
                        Incremental event clustering
                                      |
                     End-of-day cluster refinement
                                      |
                 Announcement / news extraction tracks
                                      |
                         Event-entity relationship layer
                                      |
                           LLM Extraction Adapter
                                      |
                           Deterministic Event Reasoner
                    |
                    +----> Daily Market Analysis
                    |
                    +----> Structured event factors
                                  |
Historical OHLCV / sectors / limit-up / index / scans
                    |
                    v
             Feature Snapshot Store
                    |
              Python Research Worker
                    |
          Candidate Pattern Versions
                    |
              Review / Publish Gate
                    |
                    v
            Published Pattern Store
                    |
                    v
             Rust Pattern Matcher
                    |
      Market regime + event factor + risk filters
                    |
                    v
       Ranked candidates and explanations
```

## 4. 深模块与接口

### 4.1 `PatternResearchModule`

外部接口只暴露：

```text
run_training(as_of_date, training_config) -> TrainingRunResult
validate_pattern(pattern_version_id) -> ValidationResult
publish_pattern(pattern_version_id) -> PublishedPattern
```

内部隐藏：

- 样本标签。
- 特征处理。
- 聚类算法。
- 参数搜索。
- 稳定性检验。
- 统计报告生成。

调用方不需要了解 scikit-learn、聚类类型或特征缩放方式。

### 4.2 `PatternMatchingModule`

外部接口只暴露：

```text
match_market(trade_date, pattern_set_version) -> Vec<PatternCandidate>
explain_candidate(candidate_id) -> CandidateExplanation
```

模块内部负责：

- 加载模式。
- 计算当日特征。
- 相似度。
- 市场环境调整。
- 事件因子调整。
- 风险扣分。
- A/B/观察分层。

### 4.3 `EventIngestionModule`

外部接口：

```text
ingest(source_item) -> IngestResult
submit_manual_event(manual_input) -> EventEvidence
```

内部负责：

- 来源适配。
- 内容规范化。
- 内容哈希。
- 时间标准化。
- 重复聚合。
- 来源等级。
- 原始证据保存。

### 4.4 `EventReasoningModule`

外部接口：

```text
analyze_event(event_cluster_id) -> EventAnalysis
update_market_observation(event_cluster_id, trade_date) -> EventMarketObservation
build_daily_brief(trade_date) -> DailyEventBrief
```

内部负责：

- 公告和一般新闻的双轨抽取。
- LLM JSON 抽取和 Schema 校验。
- 复制稿、事件提及和事件簇分层。
- 实体和行业映射。
- 事件—实体关系评分。
- 因果链模板。
- 异常收益和行情确认。
- 情景和失效条件。
- 多维置信度保存。

### 4.5 `AnalysisOrchestrator`

外部接口：

```text
run_eod_analysis(trade_date) -> AnalysisRunSummary
run_research_cycle(as_of_date) -> ResearchRunSummary
```

它只负责顺序和状态，不复制业务逻辑。

## 5. 目录结构

建议新增：

```text
src/analysis/
├── mod.rs
├── orchestration.rs
├── contracts.rs
├── patterns/
│   ├── mod.rs
│   ├── feature_snapshot.rs
│   ├── model_repository.rs
│   ├── matcher.rs
│   ├── scoring.rs
│   └── explanation.rs
├── events/
│   ├── mod.rs
│   ├── ingestion.rs
│   ├── dedup.rs
│   ├── mentions.rs
│   ├── clustering.rs
│   ├── cluster_refinement.rs
│   ├── announcement_extraction.rs
│   ├── news_extraction.rs
│   ├── entity_linking.rs
│   ├── reasoning.rs
│   ├── market_confirmation.rs
│   └── reporting.rs
└── adapters/
    ├── llm.rs
    ├── official_feed.rs
    ├── gdelt.rs
    ├── commercial_news.rs
    └── manual_event.rs

research/
├── pyproject.toml
├── qbot_research/
│   ├── datasets.py
│   ├── labels.py
│   ├── features.py
│   ├── clustering.py
│   ├── validation.py
│   ├── interpretation.py
│   └── cli.py
└── tests/
```

不建议把新实现继续放入 `src/services/`。现有目录已经包含扫描、交易、报告、行情和 AI 等不同职责。新分析平台应该形成独立顶层模块。

## 6. 数据契约

两个运行时通过 PostgreSQL 表和版本化 JSON Schema 通信，不通过临时文件约定，也不让 Rust 调用 Python 内部函数。

### 6.1 共同版本字段

所有模型和分析结果必须保存：

- `schema_version`
- `logic_version`
- `feature_version`
- `data_cutoff`
- `created_at`
- `created_by`
- `input_fingerprint`

### 6.2 状态机

模式版本状态：

```text
draft -> validated -> approved -> published -> retired
```

事件处理状态：

```text
collected -> deduplicated -> clustered -> extracted -> reasoned -> published
                                           \-> rejected
```

事件生命周期状态：

```text
emerging -> active -> confirmed -> cooling -> closed
                    \-> contradicted
```

运行任务状态：

```text
pending -> running -> succeeded
                   \-> partial
                   \-> failed
```

所有状态转换应幂等并记录错误原因。

## 7. 任务编排

### 7.1 每个交易日

建议顺序：

1. 17:00 更新行情、指数、涨停和板块。
2. 数据完整性检查。
3. 17:30 运行现有扫描。
4. 17:40 生成分析特征快照。
5. 17:45 运行已发布模式的在线匹配。
6. 17:45 对当天事件做收盘精细聚类。
7. 17:50 更新事件—实体关系、逻辑链和异常收益确认。
8. 18:00 生成统一市场分析和候选报告。
9. 20:05 保存信号和分析归档。

现有报告任务应逐步调用 `AnalysisOrchestrator`，而不是在调度文件中拼装更多业务模块。

### 7.2 每周研究任务

周末或周五收盘后：

1. 更新滚动五年样本。
2. 训练四个周期的候选模式。
3. 做时间序列样本外验证。
4. 保存 `draft` 和 `validated` 版本。
5. 只有满足门槛并经过审核的版本才发布。

第一版禁止训练完成后自动发布。

## 8. 降级策略

- Python Worker 失败：继续使用上一个已发布模式版本。
- LLM 不可用：保存原始事件；使用规则抽取或将事件标记为待处理，不生成虚构分析。
- 外部新闻源失败：人工事件输入仍可用。
- 当日行情不完整：不生成新的在线候选，报告明确标记数据不完整。
- 事件与行情无法确认：事件进入观察层，不影响选股分数。
- 模型 Schema 不兼容：拒绝加载，不回退到猜测式解析。

## 9. 安全与审计

每个候选必须能够回答：

- 使用了哪个模式版本？
- 哪些特征匹配？
- 哪些风险被扣分？
- 是否受到事件影响？
- 事件来自哪些来源？
- 哪些信息是事实、计算、推断或未知？
- 当时可获得的数据截止到哪一天？

任何线上结果都不得依赖未持久化的 LLM 对话上下文。

## 10. 测试策略

### Rust

- 模式 JSON 合同测试。
- 模式匹配 golden tests。
- 事件 Schema、近重复和双层聚类测试。
- 公告文档级抽取和一般新闻抽取测试。
- 事件—实体关系测试。
- 逻辑链模板测试。
- 异常收益和行情确认测试。
- 调度顺序和幂等测试。
- 数据缺失和降级测试。

### Python

- 标签无未来数据测试。
- 特征时间对齐测试。
- 聚类重复运行稳定性测试。
- Walk-forward 拆分测试。
- 成本和基准计算测试。
- 模型导出合同测试。

### 跨语言

使用固定的 fixture：

- Python 导出模式。
- Rust 加载并计算候选。
- 两端对特征名称、类型、缺失值和标准化结果达成一致。

## 11. 实施分解

该架构拆为两个子项目：

1. [强势股模式研究与在线匹配设计](2026-07-10-strong-stock-pattern-engine-design.md)
2. [市场事件与逻辑链分析设计](2026-07-10-market-event-reasoning-design.md)

推荐实施顺序：

1. 建立共同运行表、版本合同和任务状态。
2. 实现强势股标签、特征快照和离线研究。
3. 实现 Rust 在线模式匹配。
4. 建立事件黄金测试集、人工输入和证据存储。
5. 接入一个官方事实源和 GDELT 适配器。
6. 实现近重复、增量聚类和收盘精炼。
7. 实现公告/新闻双轨抽取和事件—实体关系。
8. 实现逻辑链、异常收益和行情确认。
9. 将两类结果接入统一日报。
10. 最后才允许事件因子影响模式候选分层。

## 12. 非目标

第一版不做：

- 大模型直接选股。
- 自动发布新模型。
- 深度学习价格预测。
- 高频或分钟级事件交易。
- 通用知识图谱平台。
- 券商实盘下单。
- 多个 Python 常驻微服务。
- Kafka、Flink 或独立图数据库。

Python 第一版采用可重入的批处理 Worker，由调度器或运维任务启动；事件采集和分析采用 PostgreSQL 状态机与幂等任务，不引入额外消息队列。达到百万级日证据量、分钟级低延迟或多消费者重放需求后，再评估消息流和 CEP。
