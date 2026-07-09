# 市场事件与逻辑链分析设计

> 调研依据：[市场事件采集与分析框架调研](../../research/2026-07-10-market-event-framework-research.md)
>
> 本设计吸收 GDELT、Event Registry、RavenPack、Bloomberg NSTM、实时事件检测、中文金融文档抽取、因果知识图谱和金融事件研究中的成熟模式，但不绑定任何单一供应商。

## 1. 目标

每天自动收集市场事件，保留原始证据，识别事件实体和方向，并结合市场、板块和个股数据生成可追溯的底层逻辑链。

系统需要回答：

- 今天真正新增了哪些事件？
- 哪些是同一事件的重复报道？
- 哪些内容是事实，哪些是媒体判断，哪些是系统推断？
- 事件首先改变了什么经济变量？
- 如何沿产业链传导到收入、成本、利润、估值和风险偏好？
- 哪些行业和股票类型可能受益或受损？
- 当日市场是否确认该逻辑？
- 需要观察哪些后续指标？
- 什么条件出现时应判定逻辑失效？

## 2. 设计原则

- 原始证据先于分析结论。
- 自动新闻源和人工输入使用同一个数据模型。
- 大模型只做结构化抽取和语言表达。
- 因果链由确定性模板、实体映射和市场数据共同生成。
- 不把新闻情绪直接转换成买入信号。
- 同一事件可以有多种情景，不强行输出单一方向。
- 所有结论都必须回指来源、计算数据和逻辑版本。
- 无法确认的内容明确标记为未知，而不是补全。

## 3. 事件来源

采用混合模式。

### 3.1 自动来源

通过 `NewsSource` 接口接入：

- 官方政策和监管公告。
- 交易所和上市公司公告。
- 宏观数据发布。
- 主流财经新闻。
- 行业协会和产业数据。
- 海外宏观、商品和地缘事件。

第一版来源组合固定为：

1. 一个官方公告或政策事实源。
2. GDELT 全球宏观与地缘补充源。
3. Telegram/REST 人工输入。

Event Registry、RavenPack 或其他商业源保留为可选适配器。业务模型不得依赖某个供应商的私有字段。正文持久化必须遵守来源许可；不允许长期保存正文的来源只存元数据、哈希、许可范围内摘要和原始链接。

### 3.2 人工来源

支持：

- Telegram 转发或粘贴新闻。
- Telegram 提交链接。
- REST API 提交标题、正文、来源和备注。
- 人工修改事件优先级、实体、方向和合并关系。

人工修改不能覆盖原始记录，只生成修订版本。

## 4. 来源接口

```text
trait NewsSource {
    source_id() -> str
    fetch(cursor, until) -> FetchBatch
}
```

`FetchBatch` 必须包含：

- 来源游标。
- 原始项目。
- 拉取时间。
- 是否还有下一页。
- 部分失败信息。

适配器内部负责认证、分页、限流和源格式解析。业务模块不感知具体新闻供应商。

## 5. 原始证据模型

### 5.1 `market_event_evidence`

建议字段：

```text
id
source_id
source_item_id
source_url
source_tier
published_at
collected_at
title
content
language
content_hash
raw_payload JSONB
status
created_at
UNIQUE (source_id, source_item_id)
```

其中：

- `published_at` 是来源发布时间。
- `collected_at` 是系统首次看到的时间。
- `content_hash` 用于跨来源去重。
- `raw_payload` 保存来源原始字段。
- 原始正文只追加，不原地修改。

### 5.2 来源等级

建议配置化：

| 等级 | 典型来源 | 默认可信度 |
|---|---|---:|
| A | 政府、监管、交易所、公司正式公告 | 0.95-1.00 |
| B | 权威数据机构、主流财经媒体原始采访 | 0.80-0.95 |
| C | 二次报道、行业媒体、研究摘要 | 0.60-0.85 |
| D | 社交媒体、未确认传闻 | 0.20-0.60 |

可信度不是事实真伪的最终结论，只是分析输入之一。

## 6. 去重、提及与事件聚合

成熟系统必须区分“复制内容”和“同一现实事件的不同报道”。QBot 使用三层模型：

```text
EventEvidence -> DuplicateGroup -> EventMention -> EventCluster
```

### 6.1 `DuplicateGroup`

只聚合复制或近复制内容。多个转载稿只计算为一个独立证据源，避免用转载数量虚增事件重要性。

去重顺序：

1. 来源内 ID。
2. 标准化 URL。
3. 正文哈希。
4. 标题与正文近重复模型。
5. 人工锁定的重复关系。

### 6.2 `EventMention`

每条非重复证据对现实事件形成一个提及。提及保留：

- 证据来源。
- 提及时间。
- 提及的实体和动作。
- 是否带来新增事实。
- 是否只是评论或引用。
- 来源独立性。

### 6.3 `EventCluster`

事件簇代表同一现实事件，可以包含不同来源、不同语言和不同角度的报道。

建议表字段：

```text
event_cluster_id
canonical_title
event_time
first_seen_at
last_seen_at
lifecycle_status
primary_evidence_id
representative_evidence_ids
source_entropy
mention_count
independent_source_count
cluster_version
created_at
updated_at
```

事件生命周期：

```text
emerging -> active -> confirmed -> cooling -> closed
                    \-> contradicted
```

新证据可以更新事件簇，但不能覆盖历史版本。

### 6.4 两阶段聚类

#### 采集阶段

使用低成本逻辑快速建立候选事件：

- 时间邻近。
- 实体重叠。
- 动作或事件类型重叠。
- 轻量语义相似度。

该阶段追求低延迟，允许事件被拆成多个小簇。

#### 收盘精炼阶段

使用更严格的：

- 实体消歧结果。
- 动作和对象。
- 时间、地点和数量。
- 语义相似度。
- 来源独立性。

对候选簇进行合并或拆分。精炼结果必须保留旧簇重定向记录。

### 6.5 事件重要性

重要性不能只用报道数量。至少分开保存：

- 独立来源数量。
- 来源熵。
- 来源质量。
- 实体重要性。
- 新增事实数量。
- 新颖度。
- 市场覆盖范围。

### 6.6 错误合并保护

自动合并只在高置信度下执行。中等置信度关系保存为候选关联，允许人工确认。人工锁定的拆分或合并关系优先于自动任务。

## 7. 双轨结构化抽取

正式公告与一般新闻使用不同抽取路径，最终映射到统一事件 Schema。

### 7.1 正式文档轨道

适用于政府、监管、交易所和上市公司公告：

- 文档级抽取，而不是只分析标题或单句。
- 支持同一文档中的多个事件。
- 支持跨句分散的主体、客体、金额、日期、标的和条件。
- 数字、日期和证券代码进行确定性校验。
- 通过固定的事件 Schema 生成高可信事实节点。

### 7.2 一般新闻轨道

适用于媒体、行业和聚合新闻：

- 区分记者陈述、直接引用、第三方观点和未经确认传闻。
- 通过跨来源事件簇增强或削弱事实可信度。
- 媒体判断不得直接进入事实节点。
- 不确定内容保留为待验证声明。

## 8. LLM 结构化抽取

### 8.1 输入

- 事件组内高质量证据。
- 来源元数据。
- 已知证券、公司、行业和宏观实体字典。
- 严格 JSON Schema。

### 8.2 输出 Schema

```json
{
  "event_type": "policy",
  "event_subtype": "subsidy",
  "facts": [
    {
      "claim": "fact text",
      "evidence_ids": [123],
      "confidence": 0.96
    }
  ],
  "entities": [
    {
      "type": "industry",
      "name": "example",
      "canonical_id": "optional",
      "role": "affected"
    }
  ],
  "actions": [],
  "direction": "mixed",
  "time_horizon": "medium",
  "novelty": 0.7,
  "uncertainties": [],
  "missing_information": []
}
```

### 8.3 强制规则

- 每条事实必须引用一个或多个 `evidence_id`。
- 没有证据的内容只能进入 `uncertainties` 或 `missing_information`。
- 输出必须通过 Schema 校验。
- LLM 输出不能直接写入已发布分析表。
- 同一输入、Prompt 版本和模型参数必须保存。

### 8.4 失败降级

- JSON 不合法：最多重试一次修复请求。
- 仍失败：事件保持 `collected`，等待人工处理。
- LLM 不可用：不阻塞证据采集和去重。
- 低可信来源：抽取后仍保持低来源权重。

## 9. 实体与行业映射

### 9.1 规范实体

建议维护：

- 上市公司与股票代码。
- 行业和概念代码。
- 商品。
- 宏观变量。
- 国家和地区。
- 政策机构。
- 产业链环节。

### 9.2 映射来源

- `stock_info` 中的公司和行业。
- `sector_daily` 中的行业和概念。
- 人工维护的产业链关系。
- 公司别名词典。
- 事件中出现的证券代码。

### 9.3 映射置信度

实体映射应保存：

```text
raw_name
canonical_type
canonical_id
match_method
confidence
review_status
```

名称相似但无法确认时，不自动映射到具体股票。

### 9.4 `EventEntityLink`

事件的方向、相关性和新颖度必须绑定到具体实体，而不是绑定整篇文章。

建议字段：

```text
event_cluster_id
entity_type
entity_id
role
source_confidence
evidence_independence
entity_relevance
event_novelty
expected_direction
expected_horizon
logic_confidence
review_status
```

同一个事件可以：

- 对上游供给方为正面。
- 对下游采购方为负面。
- 对被顺带提及的公司保持低相关性。

禁止使用一个“整篇新闻情绪分”覆盖上述差异。

## 10. 确定性逻辑链

### 10.1 逻辑节点类型

```text
EventFact
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
MarketConfirmation
RiskOrInvalidation
```

### 10.2 逻辑边类型

```text
increases
decreases
restricts
subsidizes
raises_cost
reduces_cost
expands_demand
reduces_supply
improves_margin
compresses_margin
raises_risk_premium
lowers_risk_premium
depends_on
contradicted_by
confirmed_by
```

### 10.3 规则模板

第一版采用少量明确模板：

#### 政策补贴

```text
补贴或税收优惠
-> 终端成本下降或企业现金流改善
-> 需求提升或利润改善
-> 直接受益行业
-> 上游需求传导
-> 财政持续性和政策执行风险
```

#### 供给收缩

```text
停产、限产、制裁或产能退出
-> 有效供给下降
-> 商品或产品价格压力向上
-> 上游生产者受益
-> 下游成本上升
-> 替代供给和库存构成失效条件
```

#### 需求冲击

```text
订单、销量、出口或基建需求变化
-> 收入预期变化
-> 产能利用率变化
-> 毛利和现金流变化
-> 产业链库存变化
```

#### 利率与流动性

```text
利率、准备金、财政投放或汇率变化
-> 融资成本和流动性
-> 风险偏好和估值折现率
-> 高久期资产、金融和出口链差异化影响
```

#### 公司事件

```text
业绩、订单、回购、并购、事故或治理变化
-> 收入、成本、股本、资产或风险变化
-> 公司直接影响
-> 供应商、客户或同行间接影响
```

### 10.4 生成方式

1. 根据结构化事件选择模板。
2. 使用实体关系补全允许的产业链节点。
3. 根据规则计算正负方向。
4. 将未知依赖保留为条件节点。
5. 生成多情景分支。
6. 使用市场数据添加确认或矛盾节点。

LLM 可以将这张图转写为自然语言，但不能创建图中不存在的事实节点。

## 11. 多情景分析

事件结论至少支持：

- 基准情景。
- 乐观情景。
- 悲观情景。

每个情景包含：

```text
assumptions
logic_chain
beneficiaries
losers
observable_indicators
invalidation_conditions
confidence
```

例如政策利好不一定直接产生正面市场影响，还要区分：

- 政策是否新增。
- 规模是否超预期。
- 是否已有提前交易。
- 执行时间是否太长。
- 行业是否存在过剩产能。
- 利好是否只改善收入但恶化利润。

## 12. 市场影响与确认

事件本身不进入选股分数，必须先经过市场数据验证。股票上涨不等于事件导致上涨；验证必须尽量扣除市场、行业和已知风险因子的共同影响。

### 12.1 确认数据

- 股票相对市场的异常收益。
- 股票相对行业的异常收益。
- 行业相对市场收益。
- 行业成交额异常。
- 行业上涨家数比例。
- 行业内龙头相对强度。
- 涨停、炸板和冲高回落。
- 上下游是否同步或分化。
- 相关商品、汇率、利率或海外指数变化。
- 事件发生前是否已经存在明显预期交易。

### 12.2 事件窗口

按事件类型选择短窗口，避免长期收益被其他因素污染：

- 即时突发：`[0,1]`、`[0,3]` 个交易日。
- 政策和产业事件：`[0,5]`、`[0,20]` 个交易日。
- 公司基本面事件：使用短期市场窗口，并另外跟踪后续经营指标。

同一窗口内存在财报、停复牌、监管处罚或重大宏观事件时，必须标记 `confounded`，不得输出高置信因果判断。

### 12.3 确认状态

```text
unconfirmed
partially_confirmed
confirmed
contradicted
confounded
expired
```

### 12.4 维度分数

禁止用一个总分覆盖不同含义。分别保存：

```text
source_confidence
evidence_independence
entity_relevance
event_novelty
event_importance
logic_confidence
market_confirmation
crowding_score
```

`market_confirmation` 由确定性计算产生，输入包括异常收益、板块宽度、成交额、龙头和跨资产验证。

### 12.5 事件类型历史基线

每种事件类型建立历史统计：

```text
sample_count
median_abnormal_return
positive_rate
turnover_response
sector_breadth_response
time_to_peak
failure_rate
```

当前事件的影响评价应说明它相对同类历史事件处于什么位置，而不是只依赖模型主观判断。

市场确认分不等于买入分。它只能通过模式匹配模块的有限权重影响候选排序。

## 13. 事件因子存储

完整数据链为：

```text
SourceItem
-> EventEvidence
-> DuplicateGroup
-> EventMention
-> EventCluster
-> EventEntityLink
-> EventLogicGraph
-> EventMarketObservation
-> EventTypeStatistics
-> DailyMarketBrief
```

每一层保留独立主键、版本和上游引用，禁止为了查询方便把所有状态压入一个 JSON 字段。

### 13.1 `market_event_extractions`

保存：

- 事件组。
- Schema 版本。
- Prompt 版本。
- 模型。
- 温度和参数。
- 结构化输出。
- 校验状态。
- 输入指纹。

### 13.2 `market_event_analyses`

保存：

- 逻辑版本。
- 逻辑图 JSON。
- 情景。
- 受益和受损映射。
- 未知和失效条件。
- 总体置信度。
- 发布状态。

### 13.3 `market_event_confirmations`

按交易日保存：

```text
event_cluster_id
entity_type
entity_id
trade_date
confirmation_status
confirmation_score
abnormal_return_market
abnormal_return_industry
market_metrics JSONB
contradictions JSONB
confounding_events JSONB
created_at
PRIMARY KEY (event_cluster_id, entity_type, entity_id, trade_date)
```

### 13.4 `market_event_type_statistics`

保存各事件类型和观察窗口的历史异常收益、成交额、宽度和失败率统计。统计必须记录样本截止日期和计算版本。

### 13.5 `market_event_revisions`

人工修改采用追加记录：

- 修改前后内容。
- 修改人。
- 原因。
- 时间。
- 被替代版本。

## 14. 每日市场事件报告

报告顺序固定：

### 14.1 今日新增事实

只写来源支持的事实，并标明来源等级和发布时间。

### 14.2 主要逻辑链

按影响范围和置信度排序，每个事件展示：

```text
事实
-> 第一层变量
-> 企业基本面传导
-> 行业和股票类型
-> 市场确认
```

### 14.3 板块与风格影响

- 已确认受益。
- 已确认受损。
- 尚未确认。
- 市场走势与事件相矛盾。

### 14.4 明日观察指标

- 价格和成交额。
- 政策细则。
- 商品或汇率。
- 龙头和板块宽度。
- 上下游验证。

### 14.5 风险和失效条件

明确说明哪些条件会推翻当前逻辑。

### 14.6 来源列表

报告中的事件必须可以打开原始来源。每个事件至少展示：

- 主要原始证据。
- 代表性报道。
- 独立来源数量和来源熵。
- 是否存在相互矛盾的报道。
- 当前事件生命周期状态。

Telegram 受长度限制时提供摘要和事件详情按钮。

## 15. API 与 Telegram

建议新增：

```text
POST /api/analysis/events/manual
GET  /api/analysis/events
GET  /api/analysis/events/:id
POST /api/analysis/events/:id/review
GET  /api/analysis/daily-brief
```

Telegram：

```text
/event <文本或链接>
/events
/event_detail <id>
/event_review <id>
/market_logic
```

人工提交时立即回复：

- 是否已存在。
- 来源是否可读取。
- 当前状态。
- 是否等待抽取或人工修订。

## 16. 调度

### 16.1 自动采集

建议每 30-60 分钟拉取一次，最高频率不高于来源限制。

### 16.2 收盘分析

行情数据完整后：

1. 汇总当天事件。
2. 更新事件组。
3. 抽取待处理高优先级事件。
4. 构建或更新逻辑链。
5. 用收盘行情确认。
6. 发布每日报告。

### 16.3 次日更新

事件可能持续多日。系统每日更新确认状态，直到：

- 逻辑被确认。
- 逻辑被市场否定。
- 事件过期。
- 人工关闭。

### 16.4 第一版基础设施约束

第一版使用：

- PostgreSQL 保存证据、聚类、状态和审计。
- 可重入的异步批任务。
- 数据库任务锁和幂等键。
- 失败重试和死信状态。

第一版不引入 Kafka、Flink 或图数据库。模块接口必须保持可迁移性：当每日证据量、实时延迟或多消费者重放需求超过单机能力时，可以把 `EventEvidence` 接入事件流，而不改变上层领域合同。

## 17. 与现有 `AiAnalysisService` 的关系

现有模块将指数、涨跌家数、板块和单只涨幅股拼入 Prompt，由大模型自由生成文字。

迁移后：

- 市场事实采集迁入分析快照。
- 事件采集和证据保存进入 `EventIngestionModule`。
- 大模型调用迁入 `LlmExtractionAdapter`。
- 因果分析进入 `EventReasoningModule`。
- 日报由结构化结果渲染。

`AiAnalysisService` 最终应被缩小为兼容适配器，或在调用方迁移后删除。禁止在新旧模块中长期保留两套市场分析逻辑。

## 18. 错误处理和可观测性

记录指标：

- 每个来源抓取成功率和延迟。
- 新增证据数。
- 去重率。
- LLM 抽取成功率。
- Schema 失败率。
- 人工修订率。
- 事件到分析的处理延迟。
- 无来源事实数量，正常应为零。
- 事件确认和矛盾分布。

错误分类：

- 来源认证失败。
- 来源限流。
- 内容无法读取。
- 时间戳无效。
- 去重冲突。
- LLM 超时或非法输出。
- 实体映射歧义。
- 行情确认数据不足。

日报不得因为单个来源或单个事件失败而整体失败。

## 19. 测试

### 19.1 采集测试

- 游标和分页。
- 重复请求幂等。
- 来源发布时间和采集时间区分。
- 手工输入和自动输入合同一致。

### 19.2 去重与聚类测试

- 完全相同正文进入同一 `DuplicateGroup`。
- 不同标题的同一公告被识别为近重复。
- 同一事件的不同原创报道进入同一 `EventCluster`，但保持独立提及。
- 同一公司不同事件不能错误合并。
- 在线小簇能被收盘精炼任务正确合并。
- 错误大簇能被收盘精炼任务拆分。
- 已发布事件簇合并或拆分保留审计和重定向。
- 来源熵不被同一来源大量转载虚增。

### 19.3 抽取测试

- 固定事件 fixture 的 Schema。
- 正式公告中的跨句参数能够组成同一事件。
- 同一公告中的多个事件不会互相混合。
- 新闻中的事实、引用、观点和传闻能够区分。
- 每条事实有来源引用。
- 模糊内容进入未知字段。
- Prompt 注入文本不能改变系统指令。

### 19.4 逻辑链测试

- 政策、供给、需求、流动性和公司事件模板。
- 上下游方向正确。
- 多情景和失效条件完整。
- LLM 文案不能新增事实节点。

### 19.5 市场确认测试

- 原始收益与市场调整异常收益分别计算。
- 股票收益能够使用行业基准进一步调整。
- 相关板块强于指数时增加确认。
- 板块下跌且宽度恶化时生成矛盾。
- 事件前已有明显上涨时增加拥挤或预期交易扣分。
- 混杂事件窗口标记为 `confounded`。
- 旧事件随时间衰减。
- 单只股票上涨不能代表整个行业确认。
- 同类历史事件基线只使用当时可获得的历史样本。

## 20. 第一版验收标准

- 支持 Telegram/REST 人工提交事件。
- 接入一个官方公告或政策事实源。
- 接入 GDELT 宏观和地缘补充源。
- 所有原始证据持久化并可追溯，且遵守来源许可。
- 复制稿能够进入 `DuplicateGroup`，不会虚增独立来源数量。
- 相同现实事件能够跨来源聚合为 `EventCluster`。
- 在线候选聚类能够在收盘被精炼。
- 事件具有生命周期、代表性证据和来源熵。
- 正式公告和一般新闻使用不同抽取轨道。
- 大模型输出严格通过 Schema，事实必须引用证据。
- 事件与具体实体之间分别保存相关性、新颖度、方向和期限。
- 能生成至少五类事件的逻辑链。
- 能展示事实、推断、未知、前提和失效条件。
- 能计算市场和行业调整后的异常收益。
- 能用市场、板块、成交和宽度数据给出确认、矛盾或混杂状态。
- 能积累事件类型历史影响基线。
- 能生成每日事件市场报告。
- 事件不能绕过模式和风险模块直接生成交易动作。
