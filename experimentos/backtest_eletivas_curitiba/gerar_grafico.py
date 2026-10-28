from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[1]
sys.path.insert(0, str(WORKSPACE / ".agents" / "skills" / "create-dhauz-html" / "scripts"))

from dhauz_plotly import COLORS, apply_dhauz_theme, to_offline_fragment  # noqa: E402


CSV_PATH = HERE / "previsoes_rolling.csv"
HTML_PATH = HERE / "grafico_comparativo.html"
HORIZONS = (1, 2, 7, 30, 90)
DFUS = [
    "Robótica",
    "Infantil",
    "Ortopedia",
    "Ginecologia e Obstetrícia",
    "Cirurgia Geral",
    "Outros",
]
VARIANT_LABELS = {
    "sem_agenda": ("Previsto · sem agenda", COLORS["baseline"]),
    "com_agenda": ("Previsto · com agenda", COLORS["recommended"]),
}


def main() -> None:
    source = pd.read_csv(CSV_PATH, encoding="utf-8", parse_dates=["ds"])
    required = {
        "ds", "dfu", "distancia_previsao", "realizado", "previsao_final",
        "variante", "data_corte",
    }
    missing = required.difference(source.columns)
    if missing:
        raise ValueError(f"Colunas ausentes no CSV: {sorted(missing)}")
    source["distancia_previsao"] = source["distancia_previsao"].astype(int)
    source["data_corte"] = pd.to_datetime(source["data_corte"])
    if set(source["dfu"].unique()) != set(DFUS):
        raise ValueError("O CSV não contém exatamente as seis DFUs esperadas")
    if set(source["distancia_previsao"].unique()) != set(HORIZONS):
        raise ValueError("O CSV deve conter D1, D2, D7, D30 e D90")
    variants = sorted(source["variante"].unique())
    if not variants or not set(variants).issubset(VARIANT_LABELS):
        raise ValueError(f"Variantes desconhecidas no CSV: {variants}")

    key = ["ds", "dfu", "distancia_previsao"]
    if source.groupby(key)["realizado"].nunique().gt(1).any():
        raise ValueError("O realizado diverge entre variantes na mesma data/DFU/horizonte")
    expected_dates = source["ds"].nunique()
    expected_rows = expected_dates * len(DFUS) * len(HORIZONS) * len(variants)
    if len(source) != expected_rows or source.duplicated(key + ["variante"]).any():
        raise ValueError(f"Esperadas {expected_rows} linhas únicas; encontradas {len(source)}")

    fig = make_subplots(
        rows=3, cols=2, subplot_titles=DFUS,
        horizontal_spacing=0.10, vertical_spacing=0.19,
    )
    positions = [(1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)]
    visibility_by_horizon: dict[int, list[bool]] = {h: [] for h in HORIZONS}
    for horizon in HORIZONS:
        horizon_data = source.loc[source["distancia_previsao"] == horizon]
        actual = horizon_data[["ds", "dfu", "realizado"]].drop_duplicates(["ds", "dfu"])
        for index, dfu in enumerate(DFUS):
            row, col = positions[index]
            actual_dfu = actual.loc[actual["dfu"] == dfu].sort_values("ds")
            if len(actual_dfu) != expected_dates:
                raise ValueError(f"A DFU {dfu}, D{horizon}, não tem datas completas")
            series = [("Realizado", "realizado", COLORS["observed"])]
            series.extend(
                (VARIANT_LABELS[variant][0], variant, VARIANT_LABELS[variant][1])
                for variant in variants
            )
            for label, variant, color in series:
                if variant == "realizado":
                    plot = actual_dfu
                    y = plot["realizado"].astype(float)
                    customdata = plot[["ds"]].astype(str).to_numpy()
                    hover = (
                        "Data %{customdata[0]}<br>Cirurgias: %{y:.0f}"
                        "<extra>Realizado</extra>"
                    )
                else:
                    plot = horizon_data.loc[
                        (horizon_data["dfu"] == dfu)
                        & (horizon_data["variante"] == variant)
                    ].sort_values("ds")
                    if len(plot) != expected_dates:
                        raise ValueError(
                            f"A variante {variant}, DFU {dfu}, D{horizon}, está incompleta"
                        )
                    y = plot["previsao_final"].astype(float)
                    customdata = plot[["data_corte"]].astype(str).to_numpy()
                    hover = (
                        "Data prevista %{x|%d/%m/%Y}<br>Corte dos dados: %{customdata[0]}"
                        "<br>Cirurgias: %{y:.0f}<extra>" + label + "</extra>"
                    )
                fig.add_trace(
                    go.Scatter(
                        x=plot["ds"], y=y, mode="lines", name=label,
                        legendgroup=label,
                        showlegend=(index == 0 and horizon == HORIZONS[0]),
                        line={"color": color, "width": 2.5},
                        customdata=customdata,
                        hovertemplate=hover,
                    ),
                    row=row, col=col,
                )
                for selected_horizon in HORIZONS:
                    visibility_by_horizon[selected_horizon].append(
                        selected_horizon == horizon
                    )

    buttons = [
        {
            "label": f"D{horizon}", "method": "update",
            "args": [
                {"visible": visibility_by_horizon[horizon]},
                {"title": f"Previsto e realizado por DFU · horizonte D{horizon}"},
            ],
        }
        for horizon in HORIZONS
    ]
    apply_dhauz_theme(
        fig,
        x_title="Data prevista",
        y_title="Cirurgias por DFU (contagem)",
        height=650,
    )
    fig.update_xaxes(title_text="")
    fig.update_yaxes(title_text="")
    fig.update_xaxes(title_text="Data prevista", row=3)
    fig.update_yaxes(title_text="Cirurgias (contagem)", row=3, col=1, rangemode="tozero")
    fig.update_yaxes(rangemode="tozero")
    fig.update_annotations(font_size=13)
    fig.update_layout(
        title=f"Previsto e realizado · horizonte D{HORIZONS[0]}",
        updatemenus=[{
            "type": "buttons", "direction": "right", "x": 0, "y": 1.16,
            "xanchor": "left", "yanchor": "top", "buttons": buttons,
            "showactive": True,
        }],
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.04,
                "xanchor": "left", "x": 0},
    )
    HTML_PATH.write_text(to_offline_fragment(fig, include_plotlyjs=True), encoding="utf-8")
    print(json.dumps({
        "html": str(HTML_PATH), "dias": expected_dates,
        "horizontes": [f"D{h}" for h in HORIZONS],
        "variantes": variants, "linhas": len(source),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
