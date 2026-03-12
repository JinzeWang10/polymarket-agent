"""Generate PDF report of current Polymarket arbitrage opportunities (v2: orderbook-first)."""
from __future__ import annotations

from datetime import datetime, timezone

from fpdf import FPDF

from src.config import Settings
from src.main import create_pipeline
from src.models.opportunity import ArbitrageOpportunity, ConstraintType, OutlierInfo
from src.utils.logging import setup_logging

TYPE_LABELS = {
    ConstraintType.MUTUAL_EXCLUSION: "互斥冲突 (Mutual Exclusion)",
    ConstraintType.SUBSET_VIOLATION: "子集违反 (Subset Violation)",
    ConstraintType.MARKET_SUM: "市场总和异常 (Market Sum)",
    ConstraintType.NO_SIDE_ARB: "NO-Side 套利 (No-Side Arb)",
    ConstraintType.DIRECTIONAL_MISPRICING: "方向性定价错误 (Directional Mispricing)",
    ConstraintType.VALUE_MISPRICING: "价值错估 (Value Mispricing)",
    ConstraintType.PENNY_OPPORTUNITY: "低价标的 (Penny <1c)",
    ConstraintType.OUTLIER_ORDER: "异常挂单 (Outlier Order)",
}

PRIORITY_ORDER = [
    ConstraintType.NO_SIDE_ARB,
    ConstraintType.VALUE_MISPRICING,
    ConstraintType.PENNY_OPPORTUNITY,
    ConstraintType.MUTUAL_EXCLUSION,
    ConstraintType.DIRECTIONAL_MISPRICING,
    ConstraintType.SUBSET_VIOLATION,
    ConstraintType.OUTLIER_ORDER,
    ConstraintType.MARKET_SUM,
]


class ReportPDF(FPDF):
    def __init__(self) -> None:
        super().__init__()
        self.add_font("yahei", "", "C:/Windows/Fonts/msyh.ttc")
        self.add_font("yahei", "B", "C:/Windows/Fonts/msyhbd.ttc")
        self.set_auto_page_break(auto=True, margin=20)

    def header(self):
        self.set_font("yahei", "B", 14)
        self.cell(0, 10, "Polymarket 足球套利扫描报告 (Orderbook 实时价格)", new_x="LMARGIN", new_y="NEXT", align="C")
        self.set_font("yahei", "", 9)
        self.set_text_color(120, 120, 120)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        self.cell(0, 6, f"生成时间: {now}", new_x="LMARGIN", new_y="NEXT", align="C")
        self.set_text_color(0, 0, 0)
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("yahei", "", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"第 {self.page_no()}/{{nb}} 页", align="C")

    def section_title(self, title: str):
        self.set_font("yahei", "B", 12)
        self.set_fill_color(41, 98, 255)
        self.set_text_color(255, 255, 255)
        self.cell(0, 9, f"  {title}", new_x="LMARGIN", new_y="NEXT", fill=True)
        self.set_text_color(0, 0, 0)
        self.ln(3)

    def opportunity_card(self, opp: ArbitrageOpportunity, index: int):
        badge_colors = {
            ConstraintType.NO_SIDE_ARB: (220, 38, 38),
            ConstraintType.MUTUAL_EXCLUSION: (234, 88, 12),
            ConstraintType.DIRECTIONAL_MISPRICING: (202, 138, 4),
            ConstraintType.SUBSET_VIOLATION: (37, 99, 235),
            ConstraintType.MARKET_SUM: (107, 114, 128),
            ConstraintType.VALUE_MISPRICING: (139, 92, 246),
            ConstraintType.PENNY_OPPORTUNITY: (16, 185, 129),
            ConstraintType.OUTLIER_ORDER: (245, 158, 11),
        }
        r, g, b = badge_colors.get(opp.constraint_type, (107, 114, 128))

        # Type label + confidence
        label = TYPE_LABELS.get(opp.constraint_type, opp.constraint_type.value)
        conf = f" [{opp.confidence}]" if opp.confidence else ""
        self.set_font("yahei", "B", 9)
        self.set_text_color(r, g, b)
        self.cell(0, 6, f"#{index}  {label}{conf}", new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(0, 0, 0)

        # Team + League
        self.set_font("yahei", "B", 10)
        self.cell(0, 6, f"{opp.team} ({opp.league})", new_x="LMARGIN", new_y="NEXT")

        # Description (now includes orderbook prices from v2 detector)
        self.set_font("yahei", "", 9)
        desc = opp.description.replace("\u00a2", "c").replace("\u03a3", "Sum").replace("\u2192", "->")
        self.multi_cell(0, 5, desc, new_x="LMARGIN", new_y="NEXT")

        # Metrics line
        metrics_parts = [f"偏差: {opp.violation_pct:.1f}%"]
        if opp.potential_profit_cents is not None:
            metrics_parts.append(f"利润: {opp.potential_profit_cents:.1f}c")
        if opp.profit_pct is not None:
            metrics_parts.append(f"ROI: {opp.profit_pct:.1f}%")
        self.set_font("yahei", "", 9)
        self.cell(0, 6, "  |  ".join(metrics_parts), new_x="LMARGIN", new_y="NEXT")

        # URLs
        if opp.polymarket_urls:
            self.set_font("yahei", "", 8)
            self.set_text_color(41, 98, 255)
            for url in opp.polymarket_urls[:2]:
                self.cell(0, 5, url, new_x="LMARGIN", new_y="NEXT", link=url)
            self.set_text_color(0, 0, 0)

        # Separator
        self.ln(2)
        self.set_draw_color(220, 220, 220)
        self.line(self.get_x() + 5, self.get_y(), self.get_x() + 185, self.get_y())
        self.ln(4)

    def outlier_card(self, opp: ArbitrageOpportunity, index: int):
        info: OutlierInfo | None = opp.outlier_info
        if info is None:
            self.opportunity_card(opp, index)
            return

        r, g, b = (245, 158, 11)

        # ── Header: index + team + confidence ──
        conf_label = {"high": "高", "medium": "中", "low": "低"}.get(opp.confidence, opp.confidence)
        self.set_font("yahei", "B", 10)
        self.set_text_color(r, g, b)
        self.cell(0, 7,
            f"#{index}  {opp.team} ({opp.league})  [{conf_label}置信度]",
            new_x="LMARGIN", new_y="NEXT",
        )
        self.set_text_color(0, 0, 0)

        # ── Question ──
        if info.question:
            self.set_font("yahei", "", 9)
            self.set_text_color(80, 80, 80)
            self.cell(0, 5, info.question, new_x="LMARGIN", new_y="NEXT")
            self.set_text_color(0, 0, 0)

        self.ln(1)

        # ── Summary line ──
        ref = info.levels[0].ref_cents if info.levels else 0
        total_shares = sum(d.size for d in info.levels)
        self.set_font("yahei", "", 9)
        self.cell(0, 5,
            f"{info.side} 侧  |  "
            f"6h中位价: {ref:.1f}c  |  "
            f"异常挂单: {len(info.levels)} 笔 ({total_shares:.0f} shares)",
            new_x="LMARGIN", new_y="NEXT",
        )
        self.ln(1)

        # ── Outlier table header ──
        col_w = [30, 25, 40, 40]
        headers = ["卖单价格", "数量", "低于中位价", "偏离幅度"]
        self.set_font("yahei", "B", 8)
        self.set_fill_color(245, 245, 245)
        x0 = self.get_x() + 5
        self.set_x(x0)
        for i, h in enumerate(headers):
            self.cell(col_w[i], 5, h, fill=True)
        self.ln()

        # ── Outlier table rows ──
        self.set_font("yahei", "", 8)
        for d in info.levels:
            self.set_x(x0)
            self.set_text_color(220, 38, 38)
            self.cell(col_w[0], 5, f"{d.price_cents:.1f}c")
            self.set_text_color(0, 0, 0)
            self.cell(col_w[1], 5, f"{d.size:.0f}")
            self.cell(col_w[2], 5, f"-{d.gap_cents:.1f}c")
            self.cell(col_w[3], 5, f"{d.gap_pct:.1f}%")
            self.ln()

        self.ln(1)

        # ── Cross-arb alert ──
        if info.cross_arb and info.cross_arb_profit_cents is not None:
            self.set_font("yahei", "B", 9)
            self.set_text_color(220, 38, 38)
            best_price = min(d.price_cents for d in info.levels)
            opp_ask = info.opposite_ask_cents or 0
            self.cell(0, 6,
                f"市场内套利: {info.side} {best_price:.1f}c + 对手 {opp_ask:.1f}c = "
                f"{best_price + opp_ask:.1f}c < 100c -> 利润 {info.cross_arb_profit_cents:.1f}c/share",
                new_x="LMARGIN", new_y="NEXT",
            )
            self.set_text_color(0, 0, 0)

        # ── Profit & metrics ──
        self.set_font("yahei", "", 9)
        best_price = min(d.price_cents for d in info.levels)
        self.cell(0, 5,
            f"最佳买入: {best_price:.1f}c  |  "
            f"6h中位价: {ref:.1f}c  |  "
            f"预期利润: {opp.potential_profit_cents:.1f}c/share",
            new_x="LMARGIN", new_y="NEXT",
        )

        # ── URLs ──
        if opp.polymarket_urls:
            self.set_font("yahei", "", 8)
            self.set_text_color(41, 98, 255)
            for url in opp.polymarket_urls[:2]:
                self.cell(0, 5, url, new_x="LMARGIN", new_y="NEXT", link=url)
            self.set_text_color(0, 0, 0)

        # Separator
        self.ln(2)
        self.set_draw_color(220, 220, 220)
        self.line(self.get_x() + 5, self.get_y(), self.get_x() + 185, self.get_y())
        self.ln(4)


def generate_report(opps: list[ArbitrageOpportunity], output_path: str) -> None:
    pdf = ReportPDF()
    pdf.alias_nb_pages()
    pdf.add_page()

    # ── Summary ──
    pdf.section_title("扫描概要")
    pdf.set_font("yahei", "", 10)

    by_league: dict[str, list[ArbitrageOpportunity]] = {}
    by_type: dict[ConstraintType, int] = {}
    for opp in opps:
        by_league.setdefault(opp.league, []).append(opp)
        by_type[opp.constraint_type] = by_type.get(opp.constraint_type, 0) + 1

    actionable = [o for o in opps if o.constraint_type in (
        ConstraintType.NO_SIDE_ARB, ConstraintType.SUBSET_VIOLATION,
        ConstraintType.MUTUAL_EXCLUSION, ConstraintType.DIRECTIONAL_MISPRICING,
    )]
    value_opps = [o for o in opps if o.constraint_type == ConstraintType.VALUE_MISPRICING]
    penny_opps = [o for o in opps if o.constraint_type == ConstraintType.PENNY_OPPORTUNITY]
    outlier_opps = [o for o in opps if o.constraint_type == ConstraintType.OUTLIER_ORDER]
    structural = [o for o in opps if o.constraint_type == ConstraintType.MARKET_SUM]

    pdf.cell(0, 6, f"扫描信号总数: {len(opps)} (基于 orderbook 实际挂单价格)", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("yahei", "B", 10)
    if actionable:
        pdf.set_text_color(220, 38, 38)
        pdf.cell(0, 6, f"可操作套利信号: {len(actionable)}", new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)
    else:
        pdf.set_text_color(22, 163, 74)
        pdf.cell(0, 6, "可操作套利信号: 0 (市场当前高效定价)", new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)
    pdf.set_font("yahei", "", 10)
    if value_opps:
        pdf.set_text_color(139, 92, 246)
        pdf.cell(0, 6, f"价值错估信号: {len(value_opps)}", new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)
    if penny_opps:
        pdf.set_text_color(16, 185, 129)
        pdf.cell(0, 6, f"低价标的 (<1c): {len(penny_opps)}", new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)
    if outlier_opps:
        pdf.set_text_color(245, 158, 11)
        pdf.cell(0, 6, f"异常挂单: {len(outlier_opps)}", new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)
    pdf.cell(0, 6, f"结构性指标: {len(structural)}", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, f"涉及联赛: {', '.join(by_league.keys())}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    pdf.set_font("yahei", "B", 10)
    pdf.cell(0, 6, "按类型分布:", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("yahei", "", 9)
    for ct in PRIORITY_ORDER:
        if ct in by_type:
            label = TYPE_LABELS.get(ct, ct.value)
            pdf.cell(0, 5, f"  - {label}: {by_type[ct]}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    # ── Methodology note ──
    pdf.set_font("yahei", "", 8)
    pdf.set_text_color(100, 100, 100)
    pdf.multi_cell(0, 4,
        "v2 方法: 所有检测基于 CLOB orderbook 的实际 best ask/bid 价格, 而非 Gamma API mid-price。"
        "只有在 orderbook 实际挂单价格下仍然存在价格矛盾的信号才会出现在报告中。"
        "Market Sum 使用 mid-price 作为结构性参考指标。",
        new_x="LMARGIN", new_y="NEXT",
    )
    pdf.set_text_color(0, 0, 0)
    pdf.ln(4)

    # ── Actionable opportunities ──
    if actionable:
        pdf.section_title(f"可操作套利信号 ({len(actionable)} 个)")
        pdf.set_font("yahei", "", 9)
        pdf.set_text_color(220, 38, 38)
        pdf.multi_cell(0, 5,
            "以下信号基于 orderbook 实际挂单价格检测, 在当前市场条件下可能存在可执行的套利机会。",
            new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(0, 0, 0)
        pdf.ln(2)
        sorted_actionable = sorted(actionable, key=lambda o: PRIORITY_ORDER.index(o.constraint_type))
        for i, opp in enumerate(sorted_actionable, 1):
            pdf.opportunity_card(opp, i)
    else:
        pdf.section_title("可操作套利: 无")
        pdf.set_font("yahei", "", 10)
        pdf.set_text_color(100, 100, 100)
        pdf.multi_cell(0, 6,
            "当前扫描未发现基于 orderbook 实际价格的可执行套利机会。\n"
            "所有曾经在 mid-price 下显示异常的市场, 在实际 best ask/bid 价格下均不构成套利。\n"
            "市场做市商已通过 bid-ask spread 消化了大部分价格矛盾。",
            new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(0, 0, 0)
        pdf.ln(4)

    # ── Value mispricing ──
    if value_opps:
        high_conf = [o for o in value_opps if o.confidence == "high"]
        med_conf = [o for o in value_opps if o.confidence == "medium"]
        low_conf = [o for o in value_opps if o.confidence == "low"]

        pdf.section_title(f"价值错估信号 ({len(value_opps)} 个)")
        pdf.set_font("yahei", "", 9)
        pdf.set_text_color(139, 92, 246)
        pdf.multi_cell(0, 5,
            "跨市场隐含概率分析: 用一个市场的价格推断另一个市场的合理范围。"
            "例如 top4=99% 意味着降级概率应 <=1%, 若降级定价 5% 则明显高估。"
            "这些是方向性价值机会, 非无风险套利。",
            new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(0, 0, 0)
        pdf.ln(2)

        sorted_value = sorted(value_opps,
            key=lambda o: ({"high": 0, "medium": 1, "low": 2}.get(o.confidence, 3), -o.violation_pct))
        for i, opp in enumerate(sorted_value, 1):
            pdf.opportunity_card(opp, i)

    # ── Penny opportunities ──
    if penny_opps:
        pdf.section_title(f"低价标的 <1c ({len(penny_opps)} 个)")
        pdf.set_font("yahei", "", 9)
        pdf.set_text_color(16, 185, 129)
        pdf.multi_cell(0, 5,
            "以下标的 YES 卖单价格低于 1 分钱 (0.01$), 属于极端长尾赔率。"
            "买入成本极低 (<1c/share), 若命中返还 100c。"
            "风险极高, 大部分不会命中, 仅作为低成本投机参考。",
            new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(0, 0, 0)
        pdf.ln(2)
        sorted_penny = sorted(penny_opps, key=lambda o: o.potential_profit_cents or 0, reverse=True)
        for i, opp in enumerate(sorted_penny, 1):
            pdf.opportunity_card(opp, i)

    # ── Outlier orders ──
    if outlier_opps:
        pdf.section_title(f"异常挂单 ({len(outlier_opps)} 个)")
        pdf.set_font("yahei", "", 9)
        pdf.set_text_color(245, 158, 11)
        pdf.multi_cell(0, 5,
            "以下挂单价格显著偏离同市场主流价格集群, 可能是误挂(fat-finger)或割肉单。"
            "检测方法: 1) 与加权中位数的偏离度; 2) 与对手侧隐含价值的交叉验证。"
            "若 YES+NO ask < 100c, 则存在市场内套利机会。",
            new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(0, 0, 0)
        pdf.ln(2)
        sorted_outlier = sorted(outlier_opps,
            key=lambda o: o.potential_profit_cents or 0, reverse=True)
        for i, opp in enumerate(sorted_outlier, 1):
            pdf.outlier_card(opp, i)

    # ── Structural indicators (market sum) ──
    if structural:
        pdf.section_title(f"结构性参考指标 ({len(structural)} 个)")
        pdf.set_font("yahei", "", 9)
        pdf.set_text_color(107, 114, 128)
        pdf.multi_cell(0, 5,
            "市场总和 (overround) 反映整个市场的定价效率。"
            "正 overround 表示做市商抽取的溢价, 负值表示市场定价偏低。"
            "此类信号为参考性质, 非单一可交易头寸。",
            new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(0, 0, 0)
        pdf.ln(2)
        for i, opp in enumerate(structural, 1):
            pdf.opportunity_card(opp, i)

    pdf.output(output_path)
    print(f"PDF report saved to: {output_path}")


if __name__ == "__main__":
    setup_logging("INFO")
    settings = Settings(config_path="config.yaml")
    pipeline = create_pipeline(settings)
    opps = pipeline.run()

    now = datetime.now().strftime("%Y-%m-%d")
    output = f"reports/polymarket-arbitrage-orderbook-{now}.pdf"
    generate_report(opps, output)
