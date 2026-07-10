# 市场事件智能与底层逻辑链设计

> Review 决议：[QBot 分析平台 Review 决议](../../reviews/2026-07-10-analysis-platform-review-resolution.md)
>
> 调研依据：[市场事件采集与分析框架调研](../../research/2026-07-10-market-event-framework-research.md)
>
> 总体架构：[QBot 分析平台总体架构设计](2026-07-10-analysis-platform-architecture-design.md)

## 1. 目标

每天收集市场事件，保留原始证据，识别新增事实，并以可追溯方式展示事件可能如何影响市场。

系统必须区分：

- 来源事实。
- 媒体引用。
- 媒体观点。
- 系统计算。
- 系统影响假设。
- 市场后续观察。
- 尚未知信息。

第一版不做事件驱动选股，只生成事实简报和解释上下文。

## 2. 设计原则

- 原始证据先于分析。
- `available_at` 决定历史回放能否使用。
- 复制稿不等于独立证据。
- 事实图与影响假设图严格分离。
- 市场走势与假设一致不等于存在因果。
- 初始影响假设必须在观察市场反应前冻结。
- 大模型不能生成无来源事实。
- 人工修订追加版本，不覆盖历史。
- 第一版不自动生成非直接实体的“受益股名单”。
- 事件权重第一版为 0。

## 3. 分阶段范围

### 3.1 Phase 2：事件证据 MVP

只实现：

- Telegram/REST 人工输入。
- 一个官方事实源。
- 原始证据。
- 精确和近重复。
- 固定事件 Schema。
- 直接实体映射。
- ClaimGraph。
- 每日事实简报。

不实现：

- GDELT。
- 跨来源复杂事件簇。
- ImpactHypothesisGraph 自动发布。
- 市场异常收益。
- 事件选股。

### 3.2 Phase 3：事件演化和市场观察

增加：

- GDELT 宏观补充。
- EventMention。
- EventCluster。
- 两阶段聚类。
- EventDelta。
- ImpactHypothesisGraph。
- 市场异常收益和板块观察。
- 事件类型历史基线。

## 4. 来源策略

### 4.1 第一版来源

1. Telegram/REST 人工输入。
2. 一个官方政策、监管、交易所或上市公司公告源。

官方源选择标准：

- 许可允许系统访问和保存必要内容。
- 有稳定 ID、发布时间和原始链接。
- 可明确区分正文和附件。
- 有可验证的发布时间。

### 4.2 后续来源

- GDELT：全球宏观、地缘和供应链补充。
- Event Registry、RavenPack 或其他商业源：可选适配器。
- 行业数据和商品源：后续按需求增加。

业务模型不得依赖供应商私有字段。

### 4.3 内容许可

每个来源保存：

```text
content_retention_policy
allowed_fields
allowed_retention_days
redistribution_allowed
source_terms_version
```

若不允许长期保存全文，只保存：

- 元数据。
- 哈希。
- 许可范围内摘要。
- 原始链接。
- 必要结构化事实。

## 5. 来源接口

```text
trait EventSource {
    source_id() -> str
    fetch(cursor, until) -> FetchBatch
}
```

`FetchBatch`：

```text
items
next_cursor
has_more
fetched_at
partial_failures
```

适配器内部处理：

- 认证。
- 分页。
- 限流。
- 重试。
- 源格式。
- 内容许可。

业务模块不感知具体供应商。

## 6. 时间语义

每条证据必须保存：

```text
occurred_at
published_at
first_seen_at
available_at
effective_trade_date
source_updated_at
```

规则：

- `published_at` 来自来源。
- `first_seen_at` 是 QBot 首次获取时间。
- `available_at` 是系统历史回放允许使用的最早时间。
- 来源后续修订产生新版本。
- 收盘后发布的信息只能用于下一交易日。
- 非交易日发布的信息映射到下一交易日。
- 无法确定发布时间的内容不能进入高置信历史事件研究。

## 7. 原始证据

### 7.1 `EventEvidence`

建议字段：

```text
evidence_id
source_id
source_item_id
source_url
source_tier
source_terms_version
occurred_at
published_at
first_seen_at
available_at
effective_trade_date
title
content
language
content_hash
raw_payload
version
supersedes_evidence_id
status
created_at
UNIQUE (source_id, source_item_id, version)
```

原始证据不可覆盖。

### 7.2 来源等级

| 等级 | 来源 | 默认范围 |
|---|---|---:|
| A | 政府、监管、交易所、公司正式公告 | 0.95-1.00 |
| B | 权威数据机构、原始采访 | 0.80-0.95 |
| C | 二次报道、行业媒体 | 0.60-0.85 |
| D | 社交媒体、未确认传闻 | 0.20-0.60 |

来源等级只是输入，不是事实真伪的最终结论。

## 8. 重复和近重复

### 8.1 `DuplicateGroup`

复制或近复制内容进入同一组。

检测顺序：

1. 来源内 ID。
2. 标准化 URL。
3. 正文哈希。
4. 标题和正文近重复。
5. 人工锁定关系。

多个转载稿只计算为一个独立证据源。

### 8.2 第一版行为

第一版不尝试复杂现实事件聚类，只提供：

- `duplicate`
- `near_duplicate`
- `independent`

中等置信关系保存为候选，不自动合并。

## 9. 双轨抽取

### 9.1 正式公告轨道

适用于：

- 政府政策。
- 监管公告。
- 交易所公告。
- 上市公司公告。

要求：

- 文档级抽取。
- 同一文档支持多个事件。
- 支持跨句主体、客体、金额、日期和条件。
- 数字、日期和证券代码确定性校验。
- 附件和正文引用可追踪。

### 9.2 一般输入轨道

适用于：

- 人工粘贴新闻。
- 人工提交链接。
- 后续媒体和 GDELT。

区分：

```text
fact
direct_quote
third_party_claim
journalist_interpretation
rumor
unknown
```

观点、传闻和未知不得进入 ClaimGraph 的事实节点。

## 10. LLM 结构化抽取

### 10.1 职责

大模型只生成候选结构：

```json
{
  "event_type": "policy",
  "event_subtype": "subsidy",
  "claims": [
    {
      "claim_type": "fact",
      "text": "...",
      "evidence_ids": [123],
      "confidence": 0.96
    }
  ],
  "entities": [],
  "amounts": [],
  "dates": [],
  "uncertainties": [],
  "missing_information": []
}
```

### 10.2 强制规则

- 每个事实引用至少一个 `evidence_id`。
- 无证据内容进入不确定字段。
- 输出必须通过 JSON Schema。
- Prompt、模型和参数必须保存。
- LLM 输出先进入候选层，不直接发布。
- 数字、日期和证券代码必须经过确定性校验。

### 10.3 失败降级

- 非法 JSON 最多重试一次修复。
- 仍失败则保持待人工处理。
- LLM 不可用不阻塞采集和证据保存。
- 低可信来源不会因为 LLM 抽取而升级来源可信度。

## 11. 实体映射

### 11.1 第一版实体

- 直接涉及的上市公司。
- 政府和监管机构。
- 一级行业。
- 商品或宏观变量。

第一版不根据模糊产业链关系自动映射具体受益股票。

### 11.2 `EntityLink`

```text
raw_name
canonical_type
canonical_id
role
match_method
confidence
review_status
evidence_ids
```

名称相似但无法确认时，不自动映射。

### 11.3 方向限制

事件方向绑定到具体实体：

```text
event_id
entity_id
role
expected_direction
expected_horizon
entity_relevance
source_confidence
```

禁止给整篇文章一个统一情绪分后应用到所有实体。

## 12. ClaimGraph

ClaimGraph 只保存有证据支持的事实。

### 12.1 节点

```text
PolicyFact
CompanyFact
MacroDataFact
SupplyFact
DemandFact
PriceFact
OperationalFact
RegulatoryFact
```

### 12.2 边

```text
issued_by
applies_to
affects_directly
reports
acquires
supplies
purchases
increases_from
decreases_from
effective_on
located_in
```

### 12.3 证据要求

每个节点和边保存：

```text
evidence_ids
extraction_method
confidence
review_status
schema_version
```

没有证据的推断不得进入 ClaimGraph。

## 13. EventDelta

Phase 3 增加 `EventDelta`，但数据合同在第一版预留。

它表示相对上一版本新增的内容：

```text
new_claims
repeated_claims
revised_values
removed_claims
status_changes
expectation_gap
new_uncertainties
resolved_uncertainties
```

每日简报优先展示增量，不重复长篇介绍同一事件。

## 14. ImpactHypothesisGraph

Phase 3 实现。

它保存系统推断，不保存来源事实。

### 14.1 节点

```text
PolicyVariable
DemandVariable
SupplyVariable
CostVariable
PriceVariable
LiquidityVariable
RiskPreferenceVariable
RevenueImpact
MarginImpact
CashFlowImpact
ValuationImpact
IndustryImpact
StockArchetypeImpact
ObservableIndicator
InvalidationCondition
```

### 14.2 边

```text
increases
decreases
depends_on
may_expand_demand
may_reduce_supply
may_raise_cost
may_reduce_cost
may_improve_margin
may_compress_margin
may_raise_risk_premium
may_lower_risk_premium
contradicted_by
observed_by
invalidated_by
```

### 14.3 每条边保存

```text
source_claim_ids
generation_method
logic_rule_id
confidence
assumptions
expected_horizon
observable_indicators
counter_scenario
invalidation_conditions
frozen_at
```

`generation_method`：

```text
domain_rule
historical_analogy
analyst_override
llm_candidate
```

LLM 候选边只有经过规则或人工审核后才能发布。

## 15. 初始假设冻结

流程：

```text
event available
-> build ClaimGraph
-> build initial hypothesis
-> freeze hypothesis
-> observe future market
-> append observations
```

禁止：

```text
observe market rally
-> rewrite initial hypothesis as bullish
```

若后续新增事实改变逻辑，应创建新假设版本并引用 `EventDelta`。

## 16. 市场观察

Phase 3 实现。

### 16.1 状态

```text
not_observed
market_aligned
market_contradicted
ambiguous
confounded
expired
```

不要使用 `confirmed` 表示因果确认。

### 16.2 独立置信度

保存：

```text
source_confidence
entity_relevance
logic_confidence
causal_confidence
market_alignment_score
crowding_score
```

价格一致只能提高 `market_alignment_score`，不能自动提高 `causal_confidence`。

### 16.3 观察指标

- 股票相对市场异常收益。
- 股票相对行业异常收益。
- 行业相对市场收益。
- 行业成交额异常。
- 行业上涨宽度。
- 龙头相对强度。
- 上下游同步或分化。
- 相关商品、汇率、利率和海外指数。
- 事件前是否已有明显预期交易。

### 16.4 事件窗口

- 突发事件：`[0,1]` 和 `[0,3]`。
- 政策和产业事件：`[0,5]` 和 `[0,20]`。
- 公司基本面事件：短期市场窗口加后续经营指标。

同一窗口出现财报、停复牌、处罚或重大宏观冲击时标记 `confounded`。

## 17. 事件历史基线

Phase 3 保存：

```text
event_type
event_subtype
entity_type
window
sample_count
median_abnormal_return
positive_rate
turnover_response
breadth_response
time_to_peak
failure_rate
data_cutoff
logic_version
```

当前事件只与当时以前可获得的历史事件比较。

## 18. 每日事实简报

Phase 2 输出固定结构：

### 18.1 今日新增事实

- 官方来源事实。
- 人工提交且已验证的事实。
- 发布时间和首次发现时间。
- 直接涉及实体。

### 18.2 今日修订

- 数字变化。
- 生效时间变化。
- 状态变化。
- 原事实撤回或更正。

### 18.3 未确认内容

- 第三方声明。
- 传闻。
- 缺失信息。

### 18.4 直接影响范围

只输出：

- 直接公司。
- 一级行业。
- 明确的上游或下游方向。
- 需要观察的股票类型。

不输出非直接实体“受益股名单”。

### 18.5 来源

- 主要原始来源。
- 代表性证据。
- 重复关系。
- 来源等级。
- 内容许可状态。

## 19. Phase 3 市场逻辑简报

在事实简报基础上增加：

- EventDelta。
- 初始影响假设。
- 市场对齐、矛盾或混杂。
- 可观察指标。
- 反向情景。
- 失效条件。
- 同类历史事件基线。

所有推断必须明确标记为推断。

## 20. 存储层次

```text
SourceItem
-> EventEvidence
-> DuplicateGroup
-> ExtractedClaim
-> EntityLink
-> ClaimGraph
-> EventDelta
-> EventMention
-> EventCluster
-> ImpactHypothesisGraph
-> EventMarketObservation
-> EventTypeStatistics
-> DailyEventBrief
```

每一层有独立主键、版本和上游引用。

## 21. API 和 Telegram

### 21.1 Phase 2 API

```text
POST /api/analysis/events/manual
GET  /api/analysis/events
GET  /api/analysis/events/:id
POST /api/analysis/events/:id/review
GET  /api/analysis/events/daily-brief
```

### 21.2 Telegram

```text
/event <文本或链接>
/events
/event_detail <id>
/event_review <id>
/market_facts
```

人工提交后立即返回：

- 是否重复。
- 是否可以读取来源。
- 证据 ID。
- 当前处理状态。
- 是否需要人工复核。

## 22. 调度

### Phase 2

```text
周期采集官方源
-> 保存证据
-> 精确/近重复
-> 抽取和校验
-> ClaimGraph
-> 18:00 事实简报
```

### Phase 3

```text
周期采集
-> 增量候选聚类
-> 收盘精炼
-> EventDelta
-> 初始假设或新版本
-> 市场观察
-> 市场逻辑简报
```

第一版使用 PostgreSQL 状态机和幂等任务，不引入 Kafka、Flink 或图数据库。

## 23. 与 `AiAnalysisService` 的关系

现有服务不再扩展。

迁移：

```text
/api/market/overview
-> 新 MarketSnapshot + DailyEventBrief
-> 保持兼容响应
-> 一个发布周期后删除自由 Prompt 逻辑
```

新事件模块不得调用旧 `build_ai_prompt()` 生成事实和逻辑。

## 24. 与选股的关系

### Phase 2 和 Phase 3 初期

- 事件权重为 0。
- 事件只显示在候选解释中。
- 不生成额外股票候选。
- 不写入自动交易候选表。

### 未来有限融合

达到事件质量和历史验证门槛后：

- 最高 `±5` 分。
- 只有直接实体或确定性行业映射可参与。
- 不能越过硬过滤。
- 每个边际分可审计。
- 事件不能单独把 Reject 提升为 A。

## 25. 错误和降级

- 来源失败：保留其他来源和人工输入。
- 内容许可不允许保存：只保存允许字段。
- LLM 不可用：保存证据，等待处理。
- Schema 失败：不发布。
- 实体映射歧义：保持未知。
- 无证据事实：拒绝发布。
- 初始假设未冻结：禁止市场观察任务。
- 市场数据不足：状态保持 `not_observed`。
- 单个事件失败：日报部分成功。

## 26. 测试

### 26.1 时间

- `published_at`、`first_seen_at` 和 `available_at` 不混用。
- 收盘后信息映射到下一交易日。
- 修订版本不回写历史。

### 26.2 重复

- 完全相同正文进入同一组。
- 近重复公告被识别。
- 独立来源不被错误合并。
- 人工锁定不被自动任务覆盖。

### 26.3 抽取

- 公告跨句参数组成同一事件。
- 同一公告多个事件不混合。
- 事实、引用、观点和传闻区分。
- 每个事实有证据。
- 数字和证券代码校验。
- Prompt 注入不能覆盖系统指令。

### 26.4 ClaimGraph

- 无证据节点不能创建。
- 每条边有证据。
- 事实和影响假设不会混入同一图。
- 修订产生新版本。

### 26.5 假设冻结

- 市场观察前存在 frozen 版本。
- 观察结果不能修改 frozen 图。
- 新事实创建新版本。

### 26.6 市场观察

- 原始收益和异常收益分别计算。
- 行业基准正确。
- 混杂事件标记 `confounded`。
- 一只股票上涨不代表行业对齐。
- 市场对齐不自动提高因果置信度。

## 27. 第一版验收

- 支持 Telegram/REST 人工事件。
- 接入一个官方事实源。
- 所有原始证据可追溯。
- `available_at` 正确。
- 精确和近重复可识别。
- 正式公告和一般输入使用不同抽取路径。
- 每个发布事实引用证据。
- 直接实体映射精确率目标不低于 95%。
- 无证据事实进入日报数量为 0。
- 非法 Schema 进入发布层数量为 0。
- 能生成 ClaimGraph。
- 能生成每日事实简报。
- 不生成非直接实体股票名单。
- 不影响选股分数和自动交易。
