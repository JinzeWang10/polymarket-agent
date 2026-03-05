"""Generate PDF report of current Polymarket arbitrage opportunities."""
from __future__ import annotations

from datetime import datetime, timezone

from fpdf import FPDF

from src.config import Settings
from src.main import create_pipeline
from src.models.opportunity import ArbitrageOpportunity, ConstraintType
from src.utils.logging import setup_logging

TYPE_LABELS = {
    ConstraintType.MUTUAL_EXCLUSION: "互斥冲突 (Mutual Exclusion)",
    ConstraintType.SUBSET_VIOLATION: "子集违反 (Subset Violation)",
    ConstraintType.MARKET_SUM: "市场总和异常 (Market Sum)",
    ConstraintType.NO_SIDE_ARB: "NO-Side 套利 (No-Side Arb)",
    ConstraintType.DIRECTIONAL_MISPRICING: "方向性定价错误 (Directional Mispricing)",
}

PRIORITY_ORDER = [
    ConstraintType.NO_SIDE_ARB,
    ConstraintType.MUTUAL_EXCLUSION,
    ConstraintType.DIRECTIONAL_MISPRICING,
    ConstraintType.SUBSET_VIOLATION,
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
        self.cell(0, 10, "Polymarket 足球套利扫描报告 (含盘口验证)", new_x="LMARGIN", new_y="NEXT", align="C")
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

    def sub_section(self, title: str):
        self.set_font("yahei", "B", 10)
        self.set_fill_color(240, 240, 240)
        self.cell(0, 7, f"  {title}", new_x="LMARGIN", new_y="NEXT", fill=True)
        self.ln(2)

    def opportunity_card(self, opp: ArbitrageOpportunity, index: int):
        badge_colors = {
            ConstraintType.NO_SIDE_ARB: (220, 38, 38),
            ConstraintType.MUTUAL_EXCLUSION: (234, 88, 12),
            ConstraintType.DIRECTIONAL_MISPRICING: (202, 138, 4),
            ConstraintType.SUBSET_VIOLATION: (37, 99, 235),
            ConstraintType.MARKET_SUM: (107, 114, 128),
        }
        r, g, b = badge_colors.get(opp.constraint_type, (107, 114, 128))

        # Type label
        label = TYPE_LABELS.get(opp.constraint_type, opp.constraint_type.value)
        self.set_font("yahei", "B", 9)
        self.set_text_color(r, g, b)
        self.cell(0, 6, f"#{index}  {label}", new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(0, 0, 0)

        # Team
        self.set_font("yahei", "B", 10)
        self.cell(0, 6, f"球队: {opp.team}", new_x="LMARGIN", new_y="NEXT")

        # Description (mid-price based)
        self.set_font("yahei", "", 9)
        desc = opp.description.replace("\u00a2", "c").replace("\u03a3", "Sum").replace("\u2192", "->")
        self.multi_cell(0, 5, f"Mid-price 信号: {desc}", new_x="LMARGIN", new_y="NEXT")

        # Metrics
        metrics = f"偏差: {opp.violation_pct:.1f}%"
        if opp.potential_profit_cents is not None:
            metrics += f"  |  Mid-price 理论利润: {opp.potential_profit_cents:.1f}c"
        self.cell(0, 6, metrics, new_x="LMARGIN", new_y="NEXT")

        # ── Orderbook verification ──
        ob = opp.orderbook
        if ob and ob.verified:
            self.ln(1)
            if ob.executable:
                self.set_font("yahei", "B", 9)
                self.set_text_color(22, 163, 74)  # green
                self.cell(0, 6, ">>> 盘口验证: 可执行", new_x="LMARGIN", new_y="NEXT")
                self.set_text_color(0, 0, 0)
                self.set_font("yahei", "", 9)
                if ob.actual_cost_cents is not None:
                    self.cell(0, 5, f"  实际成本 (best ask): {ob.actual_cost_cents:.1f}c", new_x="LMARGIN", new_y="NEXT")
                if ob.actual_profit_cents is not None:
                    self.cell(0, 5, f"  实际利润: {ob.actual_profit_cents:.1f}c ({ob.actual_profit_pct:.1f}% ROI)", new_x="LMARGIN", new_y="NEXT")
            else:
                self.set_font("yahei", "B", 9)
                self.set_text_color(220, 38, 38)  # red
                self.cell(0, 6, ">>> 盘口验证: 不可执行", new_x="LMARGIN", new_y="NEXT")
                self.set_text_color(0, 0, 0)
                self.set_font("yahei", "", 9)

            # Show orderbook detail
            if ob.best_ask_a or ob.best_ask_b:
                parts = []
                if ob.has_liquidity_a:
                    a = ob.best_ask_a
                    parts.append(f"Token A best ask: {a.price_cents:.1f}c x {a.size:.0f}" if a else "")
                else:
                    parts.append("Token A: 无流动性")
                if ob.has_liquidity_b:
                    b = ob.best_ask_b
                    parts.append(f"Token B best ask: {b.price_cents:.1f}c x {b.size:.0f}" if b else "")
                else:
                    parts.append("Token B: 无流动性")
                self.cell(0, 5, f"  {' | '.join(parts)}", new_x="LMARGIN", new_y="NEXT")

            if ob.depth_token_a > 0 or ob.depth_token_b > 0:
                self.cell(0, 5, f"  盘口深度: A={ob.depth_token_a:.0f} shares, B={ob.depth_token_b:.0f} shares", new_x="LMARGIN", new_y="NEXT")

            if ob.notes:
                notes = ob.notes.replace("\u00a2", "c")
                self.set_text_color(100, 100, 100)
                self.multi_cell(0, 5, f"  备注: {notes}", new_x="LMARGIN", new_y="NEXT")
                self.set_text_color(0, 0, 0)

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


def generate_report(opps: list[ArbitrageOpportunity], output_path: str) -> None:
    pdf = ReportPDF()
    pdf.alias_nb_pages()
    pdf.add_page()

    # ── Summary ──
    pdf.section_title("扫描概要")
    pdf.set_font("yahei", "", 10)

    by_league: dict[str, list[ArbitrageOpportunity]] = {}
    by_type: dict[ConstraintType, int] = {}
    executable_count = 0
    no_liquidity_count = 0
    for opp in opps:
        by_league.setdefault(opp.league, []).append(opp)
        by_type[opp.constraint_type] = by_type.get(opp.constraint_type, 0) + 1
        if opp.orderbook and opp.orderbook.verified:
            if opp.orderbook.executable:
                executable_count += 1
            elif not opp.orderbook.has_liquidity_a or not opp.orderbook.has_liquidity_b:
                no_liquidity_count += 1

    pdf.cell(0, 6, f"Mid-price 信号总数: {len(opps)}", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("yahei", "B", 10)
    pdf.set_text_color(22, 163, 74)
    pdf.cell(0, 6, f"盘口验证可执行: {executable_count}", new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(220, 38, 38)
    pdf.cell(0, 6, f"无流动性 (不可执行): {no_liquidity_count}", new_x="LMARGIN", new_y="NEXT")
    pdf.set_text_color(0, 0, 0)
    pdf.set_font("yahei", "", 10)
    pdf.cell(0, 6, f"涉及联赛: {', '.join(by_league.keys())}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    pdf.set_font("yahei", "B", 10)
    pdf.cell(0, 6, "按类型分布:", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("yahei", "", 9)
    for ct in PRIORITY_ORDER:
        if ct in by_type:
            label = TYPE_LABELS.get(ct, ct.value)
            pdf.cell(0, 5, f"  - {label}: {by_type[ct]}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # ── Executable opportunities first ──
    executable_opps = [o for o in opps if o.orderbook and o.orderbook.executable]
    if executable_opps:
        pdf.section_title(f"可执行套利 ({len(executable_opps)} 个)")
        pdf.set_font("yahei", "", 9)
        pdf.set_text_color(22, 163, 74)
        pdf.multi_cell(0, 5,
            "以下机会经盘口验证, 在当前 orderbook 的 best ask 价格下仍有利润空间。",
            new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(0, 0, 0)
        pdf.ln(2)
        for i, opp in enumerate(executable_opps, 1):
            pdf.opportunity_card(opp, i)

    # ── Non-executable: has liquidity but no profit ──
    has_liq_no_profit = [
        o for o in opps
        if o.orderbook and o.orderbook.verified and not o.orderbook.executable
        and o.orderbook.has_liquidity_a and o.orderbook.has_liquidity_b
    ]
    if has_liq_no_profit:
        pdf.section_title(f"有流动性但不可执行 ({len(has_liq_no_profit)} 个)")
        pdf.set_font("yahei", "", 9)
        pdf.set_text_color(202, 138, 4)
        pdf.multi_cell(0, 5,
            "Mid-price 有信号, 盘口有流动性, 但实际 ask 价格下无利润。做市商已修正价差。",
            new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(0, 0, 0)
        pdf.ln(2)
        for i, opp in enumerate(has_liq_no_profit, 1):
            pdf.opportunity_card(opp, i)

    # ── No liquidity ──
    no_liq = [
        o for o in opps
        if o.orderbook and o.orderbook.verified and not o.orderbook.executable
        and (not o.orderbook.has_liquidity_a or not o.orderbook.has_liquidity_b)
    ]
    if no_liq:
        pdf.section_title(f"无流动性 (死盘口) ({len(no_liq)} 个)")
        pdf.set_font("yahei", "", 9)
        pdf.set_text_color(150, 150, 150)
        pdf.multi_cell(0, 5,
            "Orderbook 完全为空或仅单边有挂单。Mid-price 信号无意义, 纯粹是死盘口的定价噪音。",
            new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(0, 0, 0)
        pdf.ln(2)
        for i, opp in enumerate(no_liq, 1):
            pdf.opportunity_card(opp, i)

    # ── Informational (market_sum etc. without token verification) ──
    informational = [
        o for o in opps
        if o.orderbook and o.orderbook.verified and not o.orderbook.executable
        and o.constraint_type == ConstraintType.MARKET_SUM
    ]
    if informational:
        pdf.section_title(f"参考信息 ({len(informational)} 个)")
        pdf.set_font("yahei", "", 9)
        pdf.set_text_color(150, 150, 150)
        pdf.multi_cell(0, 5,
            "市场总和异常等结构性指标, 非单一可交易头寸。",
            new_x="LMARGIN", new_y="NEXT",
        )
        pdf.set_text_color(0, 0, 0)
        pdf.ln(2)
        for i, opp in enumerate(informational, 1):
            pdf.opportunity_card(opp, i)

    pdf.output(output_path)
    print(f"PDF report saved to: {output_path}")


if __name__ == "__main__":
    setup_logging("INFO")
    settings = Settings(config_path="config.yaml")
    pipeline = create_pipeline(settings)
    opps = pipeline.run()

    now = datetime.now().strftime("%Y-%m-%d")
    output = f"reports/polymarket-arbitrage-verified-{now}.pdf"
    generate_report(opps, output)
