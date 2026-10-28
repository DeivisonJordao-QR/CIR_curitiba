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

PREDICTIONS = HERE / "360d_previsoes_rolling.csv"
METRICS = HERE / "360d_metricas.csv"
RUN_MANIFEST = HERE / "360d_manifest.json"
PLOT_FRAGMENT = HERE / "grafico_backtest_eletivas_curitiba_360d.html"
REPORT_MANIFEST = HERE / "manifest_backtest_eletivas_curitiba_360d.json"

DFUS = [
    "Robótica",
    "Infantil",
    "Ortopedia",
    "Ginecologia e Obstetrícia",
    "Cirurgia Geral",
    "Outros",
]
HORIZONS = ["D1", "D2", "D7", "D30", "D90"]
DEFAULT_DFU = "Cirurgia Geral"
DEFAULT_HORIZON = "D1"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def num(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def pt(value: float | None, decimals: int = 2) -> str:
    if value is None:
        return "n/d"
    return f"{value:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def metric_text(row: dict[str, str]) -> str:
    mae = pt(num(row["mae"]))
    rmse = pt(num(row["rmse"]))
    bias = pt(num(row["vies_previsao_menos_realizado"]))
    wape_value = num(row["wape"])
    wape = f"{pt(wape_value * 100, 1)}%" if wape_value is not None else "n/d"
    return (
        f"<b>MAE</b> {mae} &nbsp; <b>RMSE</b> {rmse} &nbsp; "
        f"<b>Viés</b> {bias} &nbsp; <b>WAPE</b> {wape} "
        f"&nbsp; <b>n</b> {int(row['n_observacoes'])}"
    )


def main() -> None:
    predictions = read_csv(PREDICTIONS)
    metrics_rows = read_csv(METRICS)
    run = json.loads(RUN_MANIFEST.read_text(encoding="utf-8-sig"))
    expected_establishment = {
        "codigo": "1",
        "nome": "Hospital Nossa Senhora do Pilar S.A.",
    }
    if run.get("estabelecimento") != expected_establishment:
        raise ValueError("O manifesto não confirma o filtro do Hospital Nossa Senhora do Pilar")

    if set(row["variante"] for row in predictions) != {"sem_agenda"}:
        raise ValueError("Este relatório espera somente a variante sem_agenda")
    if set(row["dfu"] for row in predictions) != set(DFUS):
        raise ValueError("O CSV não contém exatamente as seis DFUs esperadas")
    if set(row["horizonte"] for row in predictions) != set(HORIZONS):
        raise ValueError("O CSV deve conter D1, D2, D7, D30 e D90")

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
            if len(series) != expected_days:
                raise ValueError(f"{dfu}/{horizon}: esperados {expected_days}, encontrados {len(series)}")
            if len({row["ds"] for row in series}) != expected_days:
                raise ValueError(f"{dfu}/{horizon}: datas duplicadas")

    metric_map = {
        (row["dfu"], row["horizonte"]): row
        for row in metrics_rows if row["dfu"] != "Todos"
    }
    if len(metric_map) != len(DFUS) * len(HORIZONS):
        raise ValueError("Métricas ausentes para alguma combinação de DFU e horizonte")

    selected = data[DEFAULT_DFU][DEFAULT_HORIZON]
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
        mode="lines", name="Previsto · sem agenda", line={"color": COLORS["baseline"], "width": 2.4},
        customdata=[[row["data_corte"]] for row in selected],
        hovertemplate=("Data prevista %{x|%d/%m/%Y}<br>Corte dos dados: %{customdata[0]}"
                       "<br>Cirurgias: %{y:.1f}<extra>Previsto · sem agenda</extra>"),
    ))
    apply_dhauz_theme(
        fig, x_title="Data prevista", y_title="Cirurgias por dia (contagem)", height=570,
    )
    default_metric = metric_map[(DEFAULT_DFU, DEFAULT_HORIZON)]
    fig.update_layout(
        title={"text": f"{DEFAULT_DFU} · {DEFAULT_HORIZON}", "x": 0},
        margin={"l": 62, "r": 28, "t": 118, "b": 64},
        annotations=[{
            "text": metric_text(default_metric), "xref": "paper", "yref": "paper",
            "x": 0, "y": 1.18, "xanchor": "left", "yanchor": "bottom",
            "showarrow": False, "align": "left", "font": {"size": 13, "color": COLORS["primary"]},
        }],
        yaxis={"rangemode": "tozero"},
    )

    fragment = to_offline_fragment(fig, include_plotlyjs=True)
    controls = (
        '<div class="selection-controls">'
        '<label for="dfu-select-360">DFU</label> '
        '<select id="dfu-select-360" aria-label="Selecionar DFU">'
        + "".join(f'<option value="{dfu}"{" selected" if dfu == DEFAULT_DFU else ""}>{dfu}</option>' for dfu in DFUS)
        + '</select> &nbsp; '
        '<label for="horizon-select-360">Horizonte</label> '
        '<select id="horizon-select-360" aria-label="Selecionar horizonte">'
        + "".join(f'<option value="{h}"{" selected" if h == DEFAULT_HORIZON else ""}>{h}</option>' for h in HORIZONS)
        + '</select></div>'
    )
    payload = {
        dfu: {
            horizon: {
                "ds": [r["ds"] for r in data[dfu][horizon]],
                "realizado": [float(r["realizado"]) for r in data[dfu][horizon]],
                "previsto": [float(r["previsao_final"]) for r in data[dfu][horizon]],
                "data_corte": [r["data_corte"] for r in data[dfu][horizon]],
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
  const dfuSelect = document.getElementById('dfu-select-360');
  const horizonSelect = document.getElementById('horizon-select-360');
  const graph = document.querySelector('.plotly-slot .plotly-graph-div');
  function refresh() {
    const dfu = dfuSelect.value;
    const horizon = horizonSelect.value;
    const item = series[dfu][horizon];
    const metric = item.metric;
    const format = (value, digits) => value === null || value === '' || !Number.isFinite(Number(value))
      ? 'n/d' : Number(value).toLocaleString('pt-BR', {minimumFractionDigits: digits, maximumFractionDigits: digits});
    const wape = metric.wape === '' || !Number.isFinite(Number(metric.wape))
      ? 'n/d' : format(Number(metric.wape) * 100, 1) + '%';
    const metricLine = '<b>MAE</b> ' + format(metric.mae, 2) +
      ' &nbsp; <b>RMSE</b> ' + format(metric.rmse, 2) +
      ' &nbsp; <b>Viés</b> ' + format(metric.vies_previsao_menos_realizado, 2) +
      ' &nbsp; <b>WAPE</b> ' + wape +
      ' &nbsp; <b>n</b> ' + Number(metric.n_observacoes).toLocaleString('pt-BR');
    Plotly.update(graph,
      {x: [item.ds, item.ds], y: [item.realizado, item.previsto], customdata: [item.ds, item.data_corte]},
      {title: {text: dfu + ' · ' + horizon, x: 0}, annotations: [{
        text: metricLine, xref: 'paper', yref: 'paper', x: 0, y: 1.18,
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
    PLOT_FRAGMENT.write_text(controls + fragment + js, encoding="utf-8")

    all_metrics = {row["horizonte"]: row for row in metrics_rows if row["dfu"] == "Todos"}
    first = predictions[0]
    last = max(row["ds"] for row in predictions)
    report = {
        "title": "Backtest rolling de cirurgias eletivas — Curitiba — 360 dias",
        "sidebar_title": "Backtest eletivas 360d",
        "slides": [
            {
                "type": "cover", "menu": "Abertura", "eyebrow": "Backtest retrospectivo",
                "title": "Previsão de cirurgias eletivas",
                "subtitle": "Hospital Nossa Senhora do Pilar S.A. · realizado e previsto sem features de agenda, com seleção de DFU e horizonte.",
                "period": f"Intervalo previsto: 20/10/2025 a {last} · D1, D2, D7, D30 e D90",
                "evidence": f"Job run {run['run_id']} · Databricks pessoal",
            },
            {
                "type": "kpi_summary", "menu": "Resumo", "title": "Escopo do backtest",
                "cards": [
                    {"label": "Período avaliado", "value": "360 dias", "note": f"20/10/2025 a {last}"},
                    {"label": "Previsões", "value": "10.800", "note": "360 dias × 6 DFUs × 5 horizontes"},
                    {"label": "Estabelecimento", "value": "Nº 1", "note": "Hospital Nossa Senhora do Pilar S.A."},
                    {"label": "Origens móveis", "value": str(run["quantidade_origens_moveis"]), "note": "Sem agenda · treino móvel por horizonte"},
                ],
                "takeaways": [
                    {"heading": "Gráfico interativo", "text": "Escolha uma DFU e um horizonte. O gráfico mostra as séries diárias de realizado e previsto; as métricas MAE, RMSE, viés e WAPE acompanham a seleção."},
                    {"heading": "Métrica agregada D1", "text": f"Nos seis grupos combinados: MAE {pt(num(all_metrics['D1']['mae']))}, RMSE {pt(num(all_metrics['D1']['rmse']))}, viés {pt(num(all_metrics['D1']['vies_previsao_menos_realizado']))} e WAPE {pt(num(all_metrics['D1']['wape']) * 100, 1)}%. São {int(all_metrics['D1']['n_observacoes']):,} observações (360 dias × 6 DFUs)."},
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
                    f"Resultado do run {run['run_id']}. Escopo: hub Curitiba, estabelecimento 1 (Hospital Nossa Senhora do Pilar S.A.). Foram avaliados {run['dias_previstos']} dias consecutivos, de 20/10/2025 a {last}, para seis DFUs, cinco horizontes e somente a variante sem_agenda. Isso produz 10.800 previsões e 360 observações por DFU/horizonte; o consolidado por horizonte tem 2.160 observações.",
                    "Para uma data prevista T e horizonte H, o treino usa somente dados anteriores ao início da janela de previsão T − (H − 1) dias. Assim, cada horizonte tem seu próprio corte móvel. O gráfico compara a previsão e o realizado na mesma data-alvo.",
                    "A elegibilidade usa dt_inicio_real menos o primeiro dt_agendamento maior ou igual a 24 horas, vinculados por hub e nr_cirurgia. Os timestamps são usados como armazenados, sem conversão de fuso. Fontes somente leitura; os resultados foram salvos em arquivos no Workspace pessoal, sem gravação em tabelas.",
                ],
                "limitation": "Este backtest é retrospectivo, cobre um intervalo e uma única variante. A reconstrução histórica da agenda é aproximada porque dt_agenda não é snapshot temporal versionado. As métricas descrevem o período avaliado e não demonstram impacto operacional futuro.",
                "footer": f"Curitiba · Hospital Nossa Senhora do Pilar · Job run {run['run_id']} · Dhauz design system 2.0.0",
            },
        ],
    }
    REPORT_MANIFEST.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "plot_fragment": str(PLOT_FRAGMENT), "manifest": str(REPORT_MANIFEST),
        "dias": expected_days, "linhas_previsao": len(predictions),
        "combinacoes_metricas": len(metric_map), "inicio": first["ds"], "fim": last,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
