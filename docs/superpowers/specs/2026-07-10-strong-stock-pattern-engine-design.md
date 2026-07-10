# 强势股模式研究与 Shadow 匹配设计

> Review 决议：[QBot 分析平台 Review 决议](../../reviews/2026-07-10-analysis-platform-review-resolution.md)
>
> 总体架构：[QBot 分析平台总体架构设计](2026-07-10-analysis-platform-architecture-design.md)

## 1. 目标

研究历史强势股在启动前的共同结构，并验证这些结构是否能区分未来赢家和普通或失败样本。

系统必须分别回答：

1. 历史赢家可以分为哪些稳定原型？
2. 这些原型相对匹配对照组是否具有判别力？
3. 当前股票与哪个已验证原型最相似？
4. 该匹配相对简单基线是否提供增量价值？
5. 匹配在哪些条件下失效？

第一版只产生 shadow 候选，不影响现有 A/B 票池、模拟交易和实盘。

## 2. 第一版边界

| 周期 | 观察窗口 | 第一版状态 |
|---|---:|---|
| `week` | 5 个交易日 | 训练、验证、shadow 匹配 |
| `month` | 20 个交易日 | 训练、验证、shadow 匹配 |
| `quarter` | 60 个交易日 | 研究实验，不发布 |
| `year` | 250 个交易日 | 描述性画像，不训练预测模型 |

第一版模式类型：

- 连续趋势型
- 波动收缩突破型
- 超跌反转型

暂不包含：

- 事件驱动型
- 独立板块龙头聚类
- 年度预测模型
- 自动发布
- 自动交易接入

板块强度作为特征和验证维度，不先作为独立模式类型。

## 3. 前置数据要求

模式研究开始前必须具备：

- append-only 行情版本。
- 每日市值、流通市值、换手和估值版本。
- 历史证券主数据、上市和退市日期。
- 复权因子和公司行动。
- 每日证券状态。
- 每日 ST、停牌和价格限制。
- 历史行业归属。
- 主要指数和行业指数历史。
- 每日市场宽度。
- `available_at`。
- 可交易性过滤。

任何缺失都会使结果产生幸存者偏差、未来泄漏或不可交易样本污染。

价格特征使用前复权序列，公式和因子来源属于 `feature_version`；成交和容量评估使用未复权真实价格与成交额。

## 4. 模块分解

```text
PatternArchetypeDiscovery
-> PatternDiscriminativeValidator
-> PatternMatcher
-> CandidateRanker
```

### 4.1 `PatternArchetypeDiscovery`

职责：

- 在训练窗口内对正样本进行分层和聚类。
- 发现赢家内部的结构差异。
- 输出模式中心、分位区间和典型样本。

它只能回答“赢家像什么”，不能决定模式是否有效。

### 4.2 `PatternDiscriminativeValidator`

职责：

- 为每个模式建立匹配对照组。
- 计算基础成功率和模式成功率。
- 计算 Lift、Coverage 和 False positive rate。
- 运行 Purged Walk-forward。
- 与简单基线比较。
- 决定模式能否进入 `validated`。

### 4.3 `PatternMatcher`

职责：

- 加载已发布模式。
- 对当日股票计算模式相似度。
- 应用必要条件和风险扣分。
- 输出模式匹配详情。

### 4.4 `CandidateRanker`

职责：

- 组合模式判别力、相似度、相对强度、市场状态和风险。
- 生成 shadow A/B/Watch/Reject。
- 不接事件评分。
- 不接交易账户。

## 5. 强势标签

### 5.1 观察时点

对每个历史基准日 `t`，只使用 `available_at <= t` 的数据构造特征。

未来窗口只用于标签，不得用于任何输入、标准化、聚类参数或样本筛选。

### 5.2 标签输入

计算：

- 未来区间总收益。
- 相对主要指数收益。
- 相对历史行业指数收益。
- 最大有利波动。
- 最大回撤。
- 上涨连续性。
- 成交额和可交易性。
- 涨停无法成交比例。

### 5.3 正样本

第一版建议：

```text
future_excess_return > horizon_threshold
strength_score percentile >= 90
future_return > 0
tradable_sample = true
history_complete = true
```

阈值属于训练配置，不硬编码在线业务。

### 5.4 非独立样本问题

相邻日期的未来窗口高度重叠。样本数报告必须同时展示：

- 原始滚动样本数。
- 按非重叠窗口估算的有效样本数。
- 每只股票贡献的样本数。
- 每个年份贡献的样本数。

禁止把所有滚动样本当作独立观察。

## 6. 匹配对照组

每个正样本必须匹配对照样本。

匹配维度：

- 同一基准日期或邻近日期。
- 同行业。
- 相近总市值或流通市值。
- 相近价格区间。
- 相近 20 日成交额。
- 相近历史波动率。
- 相同可交易状态。

对照类型：

1. 普通股票。
2. 结构相似但未来未上涨的失败样本。
3. 假突破样本。
4. 未来显著跑输指数的负样本。

一个模式只有在对照组上显示增量判别力时才允许发布。

## 7. 强势评分

强势评分用于选择历史正样本，不直接作为线上候选分：

```text
strength_score =
    return_percentile
  + benchmark_excess_percentile
  + industry_excess_percentile
  + trend_consistency_percentile
  - drawdown_percentile
  - illiquidity_penalty
  - untradeable_limit_penalty
```

各分量归一化到统一尺度，权重写入训练配置和模型版本。

## 8. 特征体系

### 8.1 价格和趋势

- 1/5/10/20/60/120 日收益。
- 相对 MA5/10/20/60/120 偏离。
- 均线斜率和排序。
- 线性回归斜率与拟合优度。
- 距窗口高低点位置。
- 创新高频率。
- 收盘位置。
- 上下影比例。

年度模式未启用前，不要求 250 日特征进入第一版模型。

### 8.2 波动和回撤

- ATR 百分比。
- 实现波动率。
- 5/20、20/60 波动收缩比。
- 最大回撤。
- 回撤持续时间。
- 极端 K 线比例。
- 跳空频率。

### 8.3 成交和流动性

- 量比。
- 成交额比。
- 5/20、20/60 平均成交额变化。
- 上涨日与下跌日成交量比。
- 换手率及变化。
- 成交额横截面分位。
- 量价相关性。

### 8.4 相对强度

- 相对主要指数收益。
- 相对行业指数收益。
- 行业内收益分位。
- 行业内成交额分位。
- 行业强度和行业宽度。

### 8.5 结构

- Donchian 突破距离。
- 平台宽度。
- 支撑和阻力触碰次数。
- 假跌破收回。
- 回踩均线的 ATR 距离。
- 现有原始信号。
- 现有 `scan_ranker` 票池。

现有信号只作为辅助证据，不作为正样本硬门槛。

### 8.6 市场状态

- 指数高于 MA20/60。
- 全市场位于 MA20 以上比例。
- 新高新低比。
- 涨停、跌停和炸板。
- 市场成交额变化。
- 市场波动状态。

### 8.7 第一版排除

事件特征在第一版中全部排除。原因：

- 历史事件覆盖不一致。
- `first_seen_at` 和 `available_at` 尚未完整。
- 文本模型版本变化会造成特征漂移。
- 事件与价格先后关系容易泄漏。

## 9. 数据集存储

### 9.1 PostgreSQL

保存：

- 数据集 manifest。
- 最近在线特征。
- 训练运行记录。
- 模式版本。
- 验证摘要。
- shadow 候选。

### 9.2 Parquet

保存：

```text
features/
labels/
controls/
walk_forward_splits/
```

目录示例：

```text
research-data/
└── dataset_version=ptf-v1/
    ├── horizon=week/
    │   ├── year=2022/part-000.parquet
    │   └── ...
    ├── horizon=month/
    └── manifest.json
```

训练代码使用 Polars/DuckDB 或 PyArrow，不为 625 万级历史快照逐条读取 JSONB。

## 10. 原型发现

### 10.1 粗分

先根据可解释结构将正样本分到三个模式族：

- 连续趋势。
- 波动收缩突破。
- 超跌反转。

无法稳定归类的样本可以进入未分类池，不强制分配。

### 10.2 聚类

每个模式族内部比较：

- K-Means。
- Gaussian Mixture。

第一版不使用 HDBSCAN 作为线上模型来源。可以用于探索，但不能直接发布。

### 10.3 模式解释

每个原型必须保存：

- 高贡献特征。
- 中位数。
- 10/25/75/90 分位。
- 与对照组差异。
- 典型正例。
- 典型失败例。
- 必要条件。
- 风险条件。
- 失效条件。

不可解释或跨窗口不稳定的簇被丢弃。

## 11. Purged Walk-forward

### 11.1 划分

第一版建议：

```text
训练 24 个月
验证 6 个月
窗口前滚 3 个月
```

具体长度写入配置。

### 11.2 Purge

删除训练尾部与验证窗口标签发生重叠的样本。

### 11.3 Embargo

验证窗口后保留至少一个完整预测周期的隔离区，避免相邻样本信息泄漏。

### 11.4 最终保留期

最近一个完整验证区间作为最终保留期：

- 不用于特征筛选。
- 不用于聚类数选择。
- 不用于阈值调整。
- 只在候选模式冻结后运行一次。

## 12. 简单基线

必须实现并保存结果：

### 12.1 相对强度基线

- 20 日和 60 日相对指数收益。
- 横截面前 10%。

### 12.2 趋势基线

- `close > MA20 > MA60`。
- MA20 上升。

### 12.3 波动收缩突破基线

- 5 日波动低于 20 日。
- 20 日波动低于 60 日。
- 收盘突破前 20 日高点。

### 12.4 现有票池基线

- `pool_short_a`
- `pool_mid_a`
- 相关 B 档结果

复杂模式必须至少在一个核心指标上稳定优于基线，且不能通过显著提高换手或风险换取表面收益。

## 13. 验证指标

每个模式和基线都保存：

```text
positive_sample_count
control_sample_count
effective_sample_count
base_rate
precision
lift_over_base_rate
coverage
false_positive_rate
precision_at_10
precision_at_50
mean_excess_return
median_excess_return
cost_adjusted_return
win_rate
profit_factor
max_drawdown
max_losing_streak
turnover
capacity_estimate
yearly_results
regime_results
cluster_stability
calibration_error
top_stock_contribution
top_period_contribution
```

模式不能由少数股票、少数涨停样本或单一牛市阶段贡献全部结果。

## 14. 模式发布门槛

第一版必须满足：

- 独立验证样本达到配置门槛。
- 多数 Walk-forward 窗口 Lift 为正。
- 最终保留期没有严重失效。
- 成本后表现优于至少一个简单基线。
- 回撤和换手满足配置。
- 特征方向没有跨窗口根本反转。
- 结果不由少数样本驱动。
- 人工审核通过。

训练完成后只进入 `draft` 或 `validated`，禁止自动发布。

## 15. 模型合同

### 15.1 `analysis_pattern_versions`

```text
pattern_version_id
pattern_id
horizon
pattern_type
status
schema_version
feature_version
logic_version
dataset_version
model_payload
validation_payload
trained_from
trained_until
available_at_cutoff
approved_by
published_at
created_at
```

### 15.2 模型 payload

必须完整描述线上计算：

```text
required_features
scaler_parameters
cluster_parameters
distance_metric
necessary_conditions
risk_conditions
similarity_thresholds
validation_lift
validation_coverage
baseline_comparison
```

Rust 不得复制 Python 隐式默认值。

## 16. Rust 在线匹配

### 16.1 输入

- 当日 point-in-time 特征。
- 已发布模式集合。
- 当日市场状态。
- 证券可交易状态。

### 16.2 评分

```text
final_score =
    validated_pattern_strength
  + current_similarity
  + relative_strength
  + sector_confirmation
  + market_regime
  - extension_penalty
  - liquidity_penalty
  - data_quality_penalty
```

第一版没有事件分。

### 16.3 输出

```text
trade_date
code
name
horizon
pattern_version_id
pattern_type
similarity_score
validated_lift
final_score
shadow_tier
matched_features
risk_flags
supporting_signals
invalidations
input_fingerprint
```

### 16.4 Shadow 分层

- `Shadow A`：模式、判别力和市场状态均满足。
- `Shadow B`：模式匹配，但存在一个可观察风险。
- `Watch`：局部匹配或数据不足。
- `Reject`：硬过滤失败。

Shadow 分层不能被自动交易读取。

## 17. 与现有系统关系

### 17.1 `scan_ranker`

- 保留现有 A/B 票池。
- 作为基线。
- 新候选在独立接口和报告显示。
- 只有达到发布门槛后，才讨论替换旧票池。
- 不把新模式伪装成普通布尔信号。

### 17.2 自动交易

禁止接入，直到：

- 模拟成交成本和不可成交问题修复。
- 风险定仓完成。
- 市场日历和数据新鲜度完成。
- shadow 运行达到预设周期。

## 18. 错误处理

- 无 point-in-time 数据：不训练。
- 复权或证券状态缺失：样本排除并计数。
- 对照组不足：模式不得验证。
- 有效样本不足：保持 draft。
- 模型 Schema 不兼容：Rust 拒绝加载。
- Worker 失败：继续使用最后发布版本。
- 无已发布模式：跳过，不影响旧扫描。
- 市场状态未知：不加奖励并记录风险。

## 19. 测试

### 19.1 标签

- 标签只使用未来窗口。
- 特征只使用 `available_at <= t`。
- 退市股票保留在历史样本。
- 一字板不可交易样本排除。
- 公司行动不产生虚假收益。

### 19.2 对照组

- 行业、日期、规模、流动性匹配。
- 正样本不会进入自身对照。
- 失败突破样本正确识别。
- 对照构造不读取未来结果以外的未来信息。

### 19.3 Walk-forward

- Purge 删除重叠标签。
- Embargo 长度正确。
- 缩放器只拟合训练窗口。
- 最终保留期不参与调参。

### 19.4 跨语言

- Python 导出的固定模型可以被 Rust 加载。
- 同一 fixture 的相似度一致。
- 未知 Schema 被拒绝。
- 缺失特征不会静默变为 0。

### 19.5 Shadow

- 新候选不进入自动交易候选表。
- 旧票池不受影响。
- 报告显示模式版本、Lift、风险和最后成功日期。

## 20. 第一版验收

- 构造一周和一月 point-in-time 数据集。
- 保存正样本和三类对照组。
- 运行 Purged Walk-forward。
- 输出三个简单基线和 `scan_ranker` 基线。
- 训练三个模式族的候选原型。
- 保存 Lift、Coverage 和风险统计。
- 人工发布一个或多个模式。
- Rust 加载已发布模式。
- 生成全市场 shadow 候选。
- 不影响现有扫描、报告和自动交易。
