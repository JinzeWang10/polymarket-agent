# 世界杯结构性套利扫描器设计

日期：2026-06-11（世界杯开赛日）

## 目标

扫描 Polymarket 2026 世界杯盘口的结构性定价矛盾，发现无风险套利机会，
每 5 分钟扫描一次，通过飞书告警。

## 盘口宇宙

全部通过 Gamma API 按 slug 拉取事件（免认证）：

| 事件 slug | 席位数 | 层级 | 说明 |
|---|---|---|---|
| `world-cup-winner` | 1 | 6 | 夺冠（negRisk，60 个市场含未晋级队） |
| `world-cup-nation-to-reach-final` | 2 | 5 | 进决赛（48 国） |
| `world-cup-nation-to-reach-semifinals` | 4 | 4 | 进四强 |
| `world-cup-nation-to-reach-quarterfinals` | 8 | 3 | 进八强 |
| `world-cup-nation-to-reach-round-of-16` | 16 | 2 | 进十六强 |
| `world-cup-team-to-advance-to-knockout-stages` | 32 | 1 | 小组出线 |
| `world-cup-group-{a..l}-winner` ×12 | 1 | — | 小组头名（negRisk，含 "Other"） |

国家对齐用 `groupItemTitle`，别名表处理三个变体：
`Curaçao→Curacao`、`Bosnia-Herzegovina→Bosnia and Herzegovina`、`Congo DR→DR Congo`。
小组盘的 `Other` 不参与跨盘链检查。已关闭（closed）市场跳过。

## 两类检测

### 1. 阶段链倒挂（SUBSET_VIOLATION）

对每个国家，事件链必须单调：夺冠 ⊆ 决赛 ⊆ 4强 ⊆ 8强 ⊆ 16强 ⊆ 出线；
另有 小组头名 ⊆ 出线。

对每对有序层级 (子集 A=难, 超集 B=易) 检查买入组合：

```
cost = ask(B 的 YES) + ask(A 的 NO)
```

无论结果如何该组合至少赔付 1（若 A 实现两腿都赔 1，赔 2）。
`cost < 1 - min_edge` 即为确定性套利，利润 ≥ `(1 - cost) × 100¢`/股。

### 2. 名额求和（MARKET_SUM）

每个阶段事件恰好 N 队晋级，事件内 M 个市场的 YES 赔付之和恰为 N：

- 买全 YES：`sum(yes_asks) < N - edge` → 利润 `(N - sum) × 100¢`/套
- 买全 NO：`sum(no_asks) < (M - N) - edge` → 同理
- 小组盘（negRisk，N=1）即经典 buy-all-YES < 100¢

任一市场缺 ask 或已 closed 时跳过该事件的求和检查（淘汰开始后席位数失效）。

## 两阶段验证（控制 API 量）

1. **Gamma 初筛**：事件接口自带 `bestAsk/bestBid`（实测确认），19 次请求覆盖全宇宙，
   先用 Gamma 价格做所有约束运算。
2. **CLOB 验证**：仅对触发的候选拉真实 orderbook，重新用实际 best ask 计算，
   且每条腿在 ask 价位的可成交金额 ≥ `min_depth_usd` 才告警。
   链信号每条 2 个市场（≤4 次调用）；求和信号验证全部腿（小组 4 条，阶段 48 条，触发才发生）。

## 阈值（已确认）

- 扫描间隔：5 分钟（`worldcup_scan_interval_minutes`）
- 链套利最小利润：1¢/股（`worldcup_min_edge_cents`）
- 求和套利最小利润：5¢/套（`worldcup_min_sum_edge_cents`，多腿执行成本更高）
- 最小深度：$50/腿（`worldcup_min_depth_usd`）

## 组件

- `src/scanner/worldcup_scanner.py` — `WorldCupScanner`，模式仿照 `OutlierScanner`
  （fetch → 初筛 → 验证 → 回调流式发信号）
- `RawMarket` 增加 `best_ask`/`best_bid` 字段（alias `bestAsk`/`bestBid`）
- `FeishuAlerter.send_worldcup_signal()` — 结构套利卡片（中文）
- `main.py` 调度器增加第二个 job；`--once` 同时跑两个扫描器
- 去重：按 `(国家/事件, 约束类型)` 记忆上次 violation，变化 >2% 才重发（同 pipeline）

## 第二期：+EV 信号（接受风险，2026-06-12 加入）

### 3. 软链价值检测（VALUE_MISPRICING，WorldCupScanner 内）

相邻阶段 mid 价之比 = 市场隐含的条件晋级概率。对每对相邻阶段取全体国家
（≥8 国，easier mid ≥ 2¢）的比值中位数；某国比值 < 0.4×中位 → 高阶腿相对
便宜，> 2.5×中位 → 低阶腿相对便宜。CLOB 验证买入腿 ask 低于同梯队公允
≥ 3¢ 且深度达标才告警。confidence=low，非锁定利润。

实测首扫即命中：Qatar 进十六强 ask 4¢ vs 公允 ~9.2¢。

### 4. 滚球滞后扫描（LIVE_LAG，`live_lag_scanner.py`，60 秒一轮）

直播中单场盘秒级重定价，结构盘（小组头名/出线/16强，level ≤ 2）滞后数分钟。
检测：CLOB 分钟级价格历史，单场盘 10 分钟变动 ≥ 5¢ 而该队结构盘变动 < 1.5¢
→ 提示顺方向买入（涨买 YES / 跌买 NO），要求 ask < 97¢、深度 ≥ $50，
冷却 15 分钟。比赛识别：事件 slug `fifwc-x-y-日期`、endDate（=开球时间）
在过去 2.5h 内。结构盘映射缓存 1 小时。非无风险——比分可被扳回。

## 测试

respx mock Gamma `/events` 与 CLOB `/book`：

1. 链倒挂命中（含 CLOB 验证通过/深度不足两种结果）
2. 小组求和命中
3. 价格自洽时零误报
4. 国家别名对齐、closed 市场过滤
