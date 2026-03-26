# Binance 合约边界触发网格

这是一个 Binance U 本位合约网格策略示例，核心特性是：

- 不预先在所有格子挂单
- 每个边界触发同层挂单，不等待对侧边界确认
- 支持按固定比例（等比）生成网格
- 主要基于区间回归获利，但在价格单边移动时会逐层参与

## 代码结构

- `binance_client.py`: Binance 合约 REST 客户端（签名、下单、查单、行情）
- `dual_trigger_grid.py`: 边界触发接力网格策略（状态机、配置、参数校验）
- `bot.py`: 主流程入口（加载配置、初始化客户端、启动策略）

## 边界触发规则

核心原则：价格穿越某个边界，就在该边界执行对应方向的开仓。

同一组边界同时用于 LONG 和 SHORT，两条路径相互独立：

1. `LONG` 路径
- 价格突破 `L_i` -> 在 `L_i` 挂 `BUY LIMIT` 开多 -> 在 `L_{i+1}` 挂 `SELL LIMIT` 平多

2. `SHORT` 路径
- 价格跌破 `L_i` -> 在 `L_i` 挂 `SELL LIMIT` 开空 -> 在 `L_{i-1}` 挂 `BUY LIMIT` 平空

示例（SHORT）：
- `<70000` -> 挂 `70000` 空单
- `<69500` -> 挂 `69500` 空单
- `<69000` -> 挂 `69000` 空单

## 配置说明（新版）

- `symbol`: 交易对，例如 `BTCUSDT`
- `mode`: `long | short | both`
- `grids`: 最大活跃网格槽位（开仓单+未平仓单上限）
- `grid_ratio`: 每格比例，例 `0.005` 表示每格 0.5%
- `order_qty`: 每次下单的币数量（基础币单位），例如 `BTCUSDT` 下填 `0.002`
- `leverage`: 杠杆
- `poll_interval_sec`: 轮询间隔（秒）
- `status_interval_sec`: 终端统计输出间隔（秒）
- `csv_path`: 交易日志 CSV 路径（用于记录 cell/订单/成交，并在重启时恢复统计）

价格锚点规则：
- `mode=long`：只要求 `lower_price`
- `mode=short`：只要求 `upper_price`
- `mode=both`：需要同时提供 `lower_price` 和 `upper_price`

下单数量规则：
- 每次开仓单都按 `order_qty` 下单
- 程序会按交易对 `stepSize` 向下取整，若低于最小下单量/最小名义价值则该次跳过

触发式扩展规则：
- `short` 模式会从 `upper_price` 开始，随着价格向下自动扩展更多 cell（不再受固定区间限制）
- `long` 模式会从 `lower_price` 开始，随着价格向上自动扩展更多 cell
- 真正限制风险的是 `grids`（最大活跃槽位），而不是固定网格区间数量

CSV 与重启恢复：
- 启动时会把当前 cell 写入 CSV（`event=CELL`）
- 订单与成交事件都会写入 CSV（如 `ENTRY_PLACED`、`ENTRY_FILLED`、`EXIT_PLACED`、`CYCLE_CLOSED`）
- `CYCLE_CLOSED` 行包含单次网格利润 `pnl_usdt`
- 程序重启时会从 CSV 恢复累计 `total_trades` 和 `total_profit_usdt`
- 程序重启时会从交易所拉取当前未完成订单并恢复到对应 cell，避免重复挂单

## 配置示例

做多（只关心 `lower_price`）：

```json
{
  "symbol": "BTCUSDT",
  "mode": "long",
  "lower_price": 60000,
  "grids": 20,
  "grid_ratio": 0.005,
  "order_qty": 0.002,
  "leverage": 3,
  "poll_interval_sec": 1.0,
  "status_interval_sec": 5.0,
  "csv_path": "logs/grid_trades.csv"
}
```

做空（只关心 `upper_price`）：

```json
{
  "symbol": "BTCUSDT",
  "mode": "short",
  "upper_price": 70000,
  "grids": 20,
  "grid_ratio": 0.005,
  "order_qty": 0.002,
  "leverage": 3,
  "poll_interval_sec": 1.0,
  "status_interval_sec": 5.0,
  "csv_path": "logs/grid_trades.csv"
}
```

## 准备

1. 安装依赖

```bash
pip install -r requirements.txt
```

2. 配置 API Key

```bash
cp .env.example .env
source .env
```

3. 准备策略配置

```bash
cp config.example.json config.json
```

## 运行

```bash
python bot.py --config config.json
```

可选测试网地址（如你有 Testnet Key）：

```bash
python bot.py --config config.json --base-url https://testnet.binancefuture.com
```

## 联通与下单测试

先做联通 + 测试下单（不真实成交）：

```bash
python smoke_test_short.py --symbol BTCUSDT --notional 10
```

真实下一个市价空单并自动平仓：

```bash
python smoke_test_short.py --symbol BTCUSDT --notional 10 --real-order
```

## 风险说明

- 该脚本是教学级别，不是生产级。
- 仍有趋势风险：单边行情会导致某些格子的开仓后平仓迟迟不成交。
- 建议先用 Binance Testnet 或小资金验证。
