from __future__ import annotations

import csv
import json
import math
import sys
from pathlib import Path

import plotly.graph_objects as go

HERE = Path(__file__).resolve().parent
SKILL_SCRIPTS = HERE.parents[1] / ".agents" / "skills" / "create-dhauz-html" / "scripts"
sys.path.insert(0, str(SKILL_SCRIPTS))
from dhauz_plotly import COLORS, apply_dhauz_theme, to_offline_fragment  # noqa: E402

PREDICTIONS = HERE / "previsoes_com_agenda_90d.csv"
METRICS = HERE / "metricas_com_agenda_90d.csv"
RUN_MANIFEST = HERE / "manifest_com_agenda_90d.json"
PLOT_FRAGMENT = HERE / "grafico_backtest_com_agenda_90d.html"
REPORT_MANIFEST = HERE / "manifest_backtest_com_agenda_90d.json"

DFU_NAMES = {
    "Rob\ufffd tica".replace(" ", ""): "Robótica",
    "Ginecologia e Obstetr\ufffdcia": "Ginecologia e Obstetrícia",
}
DFUS = ["Robótica", "Infantil", "Ortopedia", "Ginecologia e Obstetrícia", "Cirurgia Geral", "Outros"]
HORIZONS = ["D1", "D2", "D7", "D30", "D90"]
DEFAULT_DFU = "Cirurgia Geral"
DEFAULT_HORIZON = "D1"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def normalize_dfu(value: str) -> str:
    return DFU_NAMES.get(value, value)


def num(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def pt(value: float | None, decimals: int = 2) -> str:
    if value is None:
        return "n/d"
    return f"{value:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def main() -> None:
    predictions = read_csv(PREDICTIONS)
    metrics_rows = read_csv(METRICS)
    run = json.loads(RUN_MANIFEST.read_text(encoding="utf-8-sig"))

    if run.get("run_id") != "backtest_rolling_20251020_90d_d1c4f895":
        raise ValueError("O manifesto não corresponde ao backtest com_agenda solicitado")
    if run.get("estabelecimento") != {"codigo": "1", "nome": "Hospital Nossa Senhora do Pilar S.A."}:
        raise ValueError("O manifesto não confirma o filtro do Hospital Nossa Senhora do Pilar")
    if run.get("variantes") != ["com_agenda"] or {r["variante"] for r in predictions} != {"com_agenda"}:
        raise ValueError("Este relatório espera somente a variante com_agenda")
    if run.get("dias_previstos") != 90:
        raise ValueError("Este relatório espera um período de 90 dias")

    for row in predictions:
        row["dfu"] = normalize_dfu(row["dfu"])
    observed_dfus = {row["dfu"] for row in predictions}
    observed_horizons = {row["horizonte"] for row in predictions}
    if observed_dfus != set(DFUS):
        raise ValueError(f"DFUs divergentes: {sorted(observed_dfus)}")
    if observed_horizons != set(HORIZONS):
        raise ValueError(f"Horizontes divergentes: {sorted(observed_horizons)}")

    data: dict[str, dict[str, list[dict[str, str]]]] = {
        dfu: {horizon: [] for horizon in HORIZONS} for dfu in DFUS
    }
    for row in predictions:
        data[row["dfu"]][row["horizonte"]].append(row)
    expected_days = int(run["dias_previstos"])
    for dfu in DFUS:
        for horizon in HORIZONS:
            series = sorted(data[dfu][horizon], key=lambda row: row["ds"])
            data[dfu][horizon] = series
            if len(series) != expected_days or len({row["ds"] for row in series}) != expected_days:
                raise ValueError(f"{dfu}/{horizon}: série incompleta ou com datas duplicadas")

    metric_map = {}
    for row in metrics_rows:
        row["dfu"] = normalize_dfu(row["dfu"])
        if row["dfu"] != "Todos":
            metric_map[(row["dfu"], row["horizonte"])] = row
    if len(metric_map) != len(DFUS) * len(HORIZONS):
        raise ValueError("Métricas ausentes para alguma combinação de DFU e horizonte")

    selected = data[DEFAULT_DFU][DEFAULT_HORIZON]
    default_metric = metric_map[(DEFAULT_DFU, DEFAULT_HORIZON)]
    default_wape_value = num(default_metric["wape"])
    default_wape = f"{pt(default_wape_value * 100, 1)}%" if default_wape_value is not None else "n/d"
    initial_metric_line = (
        f"<b>MAE</b> {pt(num(default_metric['mae']))} &nbsp; "
        f"<b>RMSE</b> {pt(num(default_metric['rmse']))} &nbsp; "
        f"<b>Viés</b> {pt(num(default_metric['vies_previsao_menos_realizado']))} &nbsp; "
        f"<b>WAPE</b> {default_wape} &nbsp; "
        f"<b>n</b> {int(default_metric['n_observacoes']):,}".replace(",", ".")
    )
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=[row["ds"] for row in selected],
        y=[float(row["realizado"]) for row in selected],
        mode="lines", name="Realizado", line={"color": COLORS["observed"], "width": 2.4},
        hovertemplate="Data %{x|%d/%m/%Y}<br>Cirurgias: %{y:.0f}<extra>Realizado</extra>",
    ))
    fig.add_trace(go.Scatter(
        x=[row["ds"] for row in selected],
        y=[float(row["previsao_final"]) for row in selected],
        mode="lines", name="Previsto · com agenda", line={"color": COLORS["baseline"], "width": 2.4},
        customdata=[[row["data_corte"], row["agendamentos"]] for row in selected],
        hovertemplate=(
            "Data prevista %{x|%d/%m/%Y}<br>Corte dos dados: %{customdata[0]}"
            "<br>Agendamentos conhecidos: %{customdata[1]}<br>Cirurgias: %{y:.1f}"
            "<extra>Previsto · com agenda</extra>"
        ),
    ))
    apply_dhauz_theme(fig, x_title="Data prevista", y_title="Cirurgias por dia (contagem)", height=570)
    fig.update_layout(
        title={"text": f"{DEFAULT_DFU} · {DEFAULT_HORIZON}", "x": 0},
        margin={"l": 62, "r": 28, "t": 145, "b": 64},
        annotations=[{
            "text": initial_metric_line, "xref": "paper", "yref": "paper", "x": 0, "y": 1.3,
            "xanchor": "left", "yanchor": "bottom", "showarrow": False,
            "align": "left", "font": {"size": 13, "color": COLORS["primary"]},
        }],
        yaxis={"rangemode": "tozero"},
    )

    controls = (
        '<div class="selection-controls">'
        '<label for="dfu-select-agenda-90">DFU</label> '
        '<select id="dfu-select-agenda-90" aria-label="Selecionar DFU">'
        + "".join(f'<option value="{dfu}"{" selected" if dfu == DEFAULT_DFU else ""}>{dfu}</option>' for dfu in DFUS)
        + '</select> &nbsp; <label for="horizon-select-agenda-90">Horizonte</label> '
        '<select id="horizon-select-agenda-90" aria-label="Selecionar horizonte">'
        + "".join(f'<option value="{h}"{" selected" if h == DEFAULT_HORIZON else ""}>{h}</option>' for h in HORIZONS)
        + "</select></div>"
    )
    payload = {
        dfu: {
            horizon: {
                "ds": [r["ds"] for r in data[dfu][horizon]],
                "realizado": [float(r["realizado"]) for r in data[dfu][horizon]],
                "previsto": [float(r["previsao_final"]) for r in data[dfu][horizon]],
                "data_corte": [r["data_corte"] for r in data[dfu][horizon]],
                "agendamentos": [int(r["agendamentos"]) for r in data[dfu][horizon]],
                "metric": metric_map[(dfu, horizon)],
            }
            for horizon in HORIZONS
        }
        for dfu in DFUS
    }
    js = """
<script>
(function () {
  const series = __DATA__;
  const dfuSelect = document.getElementById('dfu-select-agenda-90');
  const horizonSelect = document.getElementById('horizon-select-agenda-90');
  const graph = document.querySelector('.plotly-slot .plotly-graph-div');
  const format = (value, digits) => value === null || value === '' || !Number.isFinite(Number(value))
    ? 'n/d' : Number(value).toLocaleString('pt-BR', {minimumFractionDigits: digits, maximumFractionDigits: digits});
  function refresh() {
    const dfu = dfuSelect.value;
    const horizon = horizonSelect.value;
    const item = series[dfu][horizon];
    const metric = item.metric;
    const wape = metric.wape === '' || !Number.isFinite(Number(metric.wape))
      ? 'n/d' : format(Number(metric.wape) * 100, 1) + '%';
    const metricLine = '<b>MAE</b> ' + format(metric.mae, 2) +
      ' &nbsp; <b>RMSE</b> ' + format(metric.rmse, 2) +
      ' &nbsp; <b>Viés</b> ' + format(metric.vies_previsao_menos_realizado, 2) +
      ' &nbsp; <b>WAPE</b> ' + wape +
      ' &nbsp; <b>n</b> ' + Number(metric.n_observacoes).toLocaleString('pt-BR');
    Plotly.update(graph,
      {x: [item.ds, item.ds], y: [item.realizado, item.previsto], customdata: [item.ds, item.ds.map((_, i) => [item.data_corte[i], item.agendamentos[i]])]},
      {title: {text: dfu + ' · ' + horizon, x: 0}, annotations: [{
        text: metricLine, xref: 'paper', yref: 'paper', x: 0, y: 1.3,
        xanchor: 'left', yanchor: 'bottom', showarrow: false, align: 'left',
        font: {size: 13, color: '#230550'}
      }]}
    );
  }
  dfuSelect.addEventListener('change', refresh);
  horizonSelect.addEventListener('change', refresh);
})();
</script>
""".replace("__DATA__", json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    PLOT_FRAGMENT.write_text(controls + to_offline_fragment(fig, include_plotlyjs=True) + js, encoding="utf-8")

    aggregate_d1 = next(row for row in metrics_rows if row["dfu"] == "Todos" and row["horizonte"] == "D1")
    total_predictions = len(predictions)
    period_start = run["data_inicio_periodo"]
    period_end = run["data_fim_periodo"]
    report = {
        "title": "Backtest de cirurgias eletivas · Curitiba · com agenda · 90 dias",
        "sidebar_title": "Backtest com agenda 90d",
        "slides": [
            {
                "type": "cover", "menu": "Abertura", "eyebrow": "Backtest retrospectivo",
                "title": "Previsão de cirurgias eletivas",
                "subtitle": "Hospital Nossa Senhora do Pilar S.A. · realizado e previsto com features de agenda, com seleção de DFU e horizonte.",
                "period": f"Intervalo previsto: 20/10/2025 a 17/01/2026 · D1, D2, D7, D30 e D90",
                "evidence": "Job run 116639050936094 · Databricks pessoal",
            },
            {
                "type": "kpi_summary", "menu": "Resumo", "title": "Escopo do backtest",
                "cards": [
                    {"label": "Período avaliado", "value": "90 dias", "note": f"{period_start} a {period_end}"},
                    {"label": "Previsões", "value": f"{total_predictions:,}".replace(",", "."), "note": "90 dias × 6 DFUs × 5 horizontes"},
                    {"label": "Variante", "value": "Com agenda", "note": "Uma variante selecionada"},
                    {"label": "Origens móveis", "value": str(run["quantidade_origens_moveis"]), "note": "Treino recalculado conforme o horizonte"},
                ],
                "takeaways": [
                    {"heading": "Gráfico interativo", "text": "Escolha uma DFU e um horizonte. O gráfico compara realizado e previsto por dia; as métricas MAE, RMSE, viés e WAPE acompanham a seleção."},
                    {"heading": "Métrica agregada D1", "text": f"Nos seis grupos combinados: MAE {pt(num(aggregate_d1['mae']))}, RMSE {pt(num(aggregate_d1['rmse']))}, viés {pt(num(aggregate_d1['vies_previsao_menos_realizado']), 3)} e WAPE {pt(num(aggregate_d1['wape']) * 100, 1)}%. São {int(aggregate_d1['n_observacoes']):,} observações (90 dias × 6 DFUs)."},
                ],
            },
            {
                "type": "chart", "menu": "Gráfico e métricas", "title": "Previsto e realizado por seleção",
                "subtitle": "As métricas aparecem acima do gráfico e correspondem à DFU e ao horizonte selecionados.",
                "plotly_file": PLOT_FRAGMENT.name,
                "risk": "WAPE não é definido quando o realizado da DFU no período é zero; nesse caso, o indicador aparece como n/d.",
            },
            {
                "type": "method", "menu": "Método e limites", "title": "Método, dados e limitações",
                "paragraphs": [
                    f"Resultado do job run 116639050936094 (artefato {run['run_id']}). Escopo: hub Curitiba, estabelecimento 1 (Hospital Nossa Senhora do Pilar S.A.). Foram avaliados 90 dias consecutivos, de {period_start} a {period_end}, para seis DFUs, cinco horizontes e somente a variante com_agenda. Isso produz {total_predictions:,} previsões e 90 observações por DFU/horizonte; o consolidado por horizonte tem 540 observações.",
                    "Para uma data prevista T e horizonte H, o treino usa somente dados anteriores ao início da janela de previsão T − (H − 1) dias. Cada horizonte tem seu próprio corte móvel; o gráfico compara a previsão e o realizado na mesma data-alvo.",
                    "A elegibilidade exige dt_inicio_real menos o primeiro dt_agendamento maior ou igual a 24 horas, vinculados por hub e nr_cirurgia. O modelo com_agenda inclui as features de agenda disponíveis no corte. Timestamps são usados como armazenados, sem conversão de fuso. As fontes são somente leitura e os resultados ficam em arquivos no Workspace pessoal, sem gravação em tabelas.",
                ],
                "limitation": "A reconstrução histórica da agenda é aproximada porque dt_agenda não é snapshot temporal versionado. As métricas descrevem este período retrospectivo e não demonstram impacto operacional futuro.",
                "footer": "Curitiba · Hospital Nossa Senhora do Pilar · Job run 116639050936094 · Dhauz design system 2.0.0",
            },
        ],
    }
    REPORT_MANIFEST.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "plot_fragment": str(PLOT_FRAGMENT), "manifest": str(REPORT_MANIFEST),
        "dias": expected_days, "linhas_previsao": total_predictions,
        "combinacoes_metricas": len(metric_map), "inicio": period_start, "fim": period_end,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
