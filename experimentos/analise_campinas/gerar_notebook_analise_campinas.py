from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent


def source(text: str) -> list[str]:
    return dedent(text).strip("\n").splitlines(keepends=True)


cells = [
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": source(
            """
            # Análise de realizado versus previsto — Campinas

            Compara as cirurgias realizadas e previstas para **90 dias**, todas as DFUs e os horizontes **D1, D2, D7, D30 e D90**, usando as views `planejamento_integrado.ds_pipeline.realizado_demanda_cirurgias_campinas` e `planejamento_integrado.ds_pipeline.previsao_demanda_cirurgias_campinas`.

            A data inicial fica configurável no widget da próxima célula (padrão: **20/10/2025**). O período é o mesmo para todos os horizontes. Para cada data-alvo `T` e horizonte `H`, o notebook seleciona a previsão cuja origem é `T - (H - 1)` dias, isto é, `data_processamento = data_evento - (distancia_previsao - 1)`. Assim, os horizontes comparam previsões diferentes para as mesmas datas realizadas.

            O filtro de unidade é `hub = Campinas` e `unidade_hospital = HVC`, conforme o contrato das views. O notebook é somente leitura e não grava tabelas.

            **Métricas:** MAE, MAPE (média percentual calculada somente nos dias com realizado maior que zero), WAPE (soma dos erros absolutos dividida pela soma do realizado) e viés direcional (média de `previsto - realizado`, em cirurgias/dia; positivo indica superestimação, negativo indica subestimação). A cobertura mostra quantos dos 90 dias têm previsão para cada DFU/horizonte.
            """
        ),
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source(
            r'''
            from datetime import date, datetime, timedelta
            from html import escape
            import json
            import math

            import pandas as pd
            import plotly.graph_objects as go

            REALIZADO_TABLE = "planejamento_integrado.ds_pipeline.realizado_demanda_cirurgias_campinas"
            PREVISAO_TABLE = "planejamento_integrado.ds_pipeline.previsao_demanda_cirurgias_campinas"
            HUB = "Campinas"
            UNIDADE = "HVC"
            HORIZONTES = [1, 2, 7, 30, 90]
            DIAS_PERIODO = 90

            dbutils.widgets.text("data_inicio_periodo", "2025-10-20", "Data inicial (AAAA-MM-DD)")
            data_inicio_texto = dbutils.widgets.get("data_inicio_periodo").strip()
            data_inicio = datetime.strptime(data_inicio_texto, "%Y-%m-%d").date()
            data_fim = data_inicio + timedelta(days=DIAS_PERIODO - 1)

            print(f"Período: {data_inicio:%d/%m/%Y} a {data_fim:%d/%m/%Y} ({DIAS_PERIODO} dias)")
            print(f"Escopo: {HUB} · {UNIDADE} · horizontes {[f'D{h}' for h in HORIZONTES]}")
            '''
        ),
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": source(
            """
            ## Carregar a série pareada

            A consulta mantém todos os dias e DFUs do realizado. Quando uma previsão não existir para uma combinação, ela permanece nula; a cobertura e o número de observações das métricas deixam essa ausência visível.
            """
        ),
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source(
            r'''
            inicio_sql = data_inicio.isoformat()
            fim_sql = data_fim.isoformat()
            lista_horizontes = ", ".join(str(h) for h in HORIZONTES)

            query = f"""
            WITH realizado AS (
                SELECT
                    CAST(data_evento AS DATE) AS data_evento,
                    TRIM(dfu) AS dfu,
                    SUM(CAST(realizado AS DOUBLE)) AS realizado
                FROM {REALIZADO_TABLE}
                WHERE hub = '{HUB}'
                  AND unidade_hospital = '{UNIDADE}'
                  AND data_evento BETWEEN DATE'{inicio_sql}' AND DATE'{fim_sql}'
                  AND dfu IS NOT NULL
                GROUP BY CAST(data_evento AS DATE), TRIM(dfu)
            ),
            previsoes_ranked AS (
                SELECT
                    CAST(data_evento AS DATE) AS data_evento,
                    CAST(data_processamento AS DATE) AS data_processamento,
                    CAST(distancia_previsao AS INT) AS horizonte,
                    TRIM(dfu) AS dfu,
                    CAST(previsao AS DOUBLE) AS previsto,
                    modelo,
                    ROW_NUMBER() OVER (
                        PARTITION BY CAST(data_evento AS DATE), CAST(distancia_previsao AS INT), TRIM(dfu)
                        ORDER BY CASE WHEN modelo = 'campeao' THEN 1 ELSE 0 END DESC,
                                 CAST(data_processamento AS DATE) DESC
                    ) AS rn
                FROM {PREVISAO_TABLE}
                WHERE hub = '{HUB}'
                  AND unidade_hospital = '{UNIDADE}'
                  AND data_evento BETWEEN DATE'{inicio_sql}' AND DATE'{fim_sql}'
                  AND CAST(distancia_previsao AS INT) IN ({lista_horizontes})
                  AND CAST(data_processamento AS DATE) = DATE_SUB(
                      CAST(data_evento AS DATE), CAST(distancia_previsao AS INT) - 1
                  )
                  AND dfu IS NOT NULL
            ),
            previsoes AS (
                SELECT data_evento, data_processamento, horizonte, dfu, previsto, modelo
                FROM previsoes_ranked
                WHERE rn = 1
            ),
            combinacoes AS (
                SELECT r.data_evento, r.dfu, h.horizonte, r.realizado
                FROM realizado r
                CROSS JOIN (SELECT EXPLODE(ARRAY({lista_horizontes})) AS horizonte) h
            )
            SELECT
                c.data_evento,
                c.dfu,
                c.horizonte,
                c.realizado,
                p.previsto,
                p.data_processamento,
                p.modelo
            FROM combinacoes c
            LEFT JOIN previsoes p
              ON p.data_evento = c.data_evento
             AND p.dfu = c.dfu
             AND p.horizonte = c.horizonte
            ORDER BY c.data_evento, c.dfu, c.horizonte
            """

            series_spark = spark.sql(query)
            series_pd = series_spark.toPandas()
            if series_pd.empty:
                raise ValueError("A consulta não retornou realizado para o período e unidade informados.")

            series_pd["data_evento"] = pd.to_datetime(series_pd["data_evento"])
            series_pd["data_processamento"] = pd.to_datetime(series_pd["data_processamento"])
            series_pd["realizado"] = pd.to_numeric(series_pd["realizado"], errors="coerce")
            series_pd["previsto"] = pd.to_numeric(series_pd["previsto"], errors="coerce")
            DFUS = sorted(series_pd["dfu"].dropna().unique().tolist())
            if not DFUS:
                raise ValueError("Nenhuma DFU encontrada no realizado para o período.")

            linhas_esperadas = DIAS_PERIODO * len(DFUS) * len(HORIZONTES)
            if len(series_pd) != linhas_esperadas:
                raise ValueError(
                    f"Grade inesperada: {len(series_pd)} linhas; esperadas {linhas_esperadas} "
                    f"({DIAS_PERIODO} dias × {len(DFUS)} DFUs × {len(HORIZONTES)} horizontes)."
                )

            cobertura = (
                series_pd.groupby(["dfu", "horizonte"], as_index=False)
                .agg(n_previsto=("previsto", "count"))
            )
            faltantes = cobertura[cobertura["n_previsto"] < DIAS_PERIODO]
            if not faltantes.empty:
                print("Aviso: há combinações com previsões ausentes; confira n e cobertura nas métricas.")
                display(faltantes.assign(horizonte=faltantes["horizonte"].map(lambda h: f"D{h}")))

            display(series_spark.limit(15))
            '''
        ),
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": source(
            """
            ## Métricas por DFU e horizonte

            O viés direcional é calculado como média do erro assinado (`previsto - realizado`), em cirurgias por dia. MAPE exclui apenas dias com realizado zero; WAPE mantém os dias zero no numerador e usa a soma do realizado no denominador.
            """
        ),
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source(
            r'''
            metric_rows = []
            for (dfu, horizonte), grupo in series_pd.groupby(["dfu", "horizonte"], sort=True):
                valido = grupo.dropna(subset=["realizado", "previsto"]).copy()
                erro = valido["previsto"] - valido["realizado"]
                realizado_positivo = valido["realizado"] > 0
                mape_n = int(realizado_positivo.sum())
                mape = (
                    float((erro[realizado_positivo].abs() / valido.loc[realizado_positivo, "realizado"]).mean() * 100)
                    if mape_n else None
                )
                soma_realizado = float(valido["realizado"].sum())
                wape = float(erro.abs().sum() / soma_realizado * 100) if soma_realizado > 0 else None
                bias = float(erro.mean()) if len(valido) else None
                if bias is None:
                    direcao = "sem previsões"
                elif bias > 0:
                    direcao = "superestima"
                elif bias < 0:
                    direcao = "subestima"
                else:
                    direcao = "sem viés médio"
                metric_rows.append({
                    "dfu": dfu,
                    "horizonte": f"D{int(horizonte)}",
                    "mae_cirurgias_dia": float(erro.abs().mean()) if len(valido) else None,
                    "mape_percentual": mape,
                    "mape_n_realizado_maior_zero": mape_n,
                    "wape_percentual": wape,
                    "bias_direcional_cirurgias_dia": bias,
                    "direcao_bias": direcao,
                    "n_previsto": int(len(valido)),
                    "cobertura_percentual": float(len(valido) / DIAS_PERIODO * 100),
                })

            metricas_pd = pd.DataFrame(metric_rows)
            metricas_pd["ordem_horizonte"] = metricas_pd["horizonte"].str[1:].astype(int)
            metricas_pd = metricas_pd.sort_values(["dfu", "ordem_horizonte"]).reset_index(drop=True)
            display(metricas_pd.drop(columns=["ordem_horizonte"]))
            '''
        ),
    },
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": source(
            """
            ## Gráfico interativo

            Use os seletores para escolher DFU e horizonte. O gráfico mostra as 90 datas do período selecionado; os indicadores ao lado do gráfico são recalculados para a combinação ativa.
            """
        ),
    },
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source(
            r'''
            CORES = {"realizado": "#2563eb", "previsto": "#595959", "primaria": "#230550", "destaque": "#640ee6"}
            HORIZONTES_TEXTO = [f"D{h}" for h in HORIZONTES]
            HORIZONTE_PARA_INT = {f"D{h}": h for h in HORIZONTES}
            DFU_PADRAO = "Cirurgia Geral" if "Cirurgia Geral" in DFUS else DFUS[0]
            HORIZONTE_PADRAO = "D1"

            def formatar_numero(valor, casas=2):
                if valor is None or pd.isna(valor) or not math.isfinite(float(valor)):
                    return "n/d"
                return f"{float(valor):,.{casas}f}".replace(",", "X").replace(".", ",").replace("X", ".")

            def metric_dict(dfu, horizonte):
                linha = metricas_pd[(metricas_pd["dfu"] == dfu) & (metricas_pd["horizonte"] == horizonte)].iloc[0]
                return {key: (None if pd.isna(value) else value) for key, value in linha.to_dict().items()}

            payload = {}
            for dfu in DFUS:
                payload[dfu] = {}
                for horizonte_texto in HORIZONTES_TEXTO:
                    horizonte = HORIZONTE_PARA_INT[horizonte_texto]
                    grupo = series_pd[(series_pd["dfu"] == dfu) & (series_pd["horizonte"] == horizonte)].sort_values("data_evento")
                    payload[dfu][horizonte_texto] = {
                        "datas": grupo["data_evento"].dt.strftime("%Y-%m-%d").tolist(),
                        "realizado": [None if pd.isna(v) else float(v) for v in grupo["realizado"]],
                        "previsto": [None if pd.isna(v) else float(v) for v in grupo["previsto"]],
                        "origem": [None if pd.isna(v) else v.strftime("%Y-%m-%d") for v in grupo["data_processamento"]],
                        "modelo": [None if pd.isna(v) else str(v) for v in grupo["modelo"]],
                        "metricas": metric_dict(dfu, horizonte_texto),
                    }

            def montar_cards(dfu, horizonte):
                m = payload[dfu][horizonte]["metricas"]
                bias = m["bias_direcional_cirurgias_dia"]
                if bias is None:
                    direcao = "sem previsões"
                elif bias > 0:
                    direcao = "superestima"
                elif bias < 0:
                    direcao = "subestima"
                else:
                    direcao = "sem viés médio"
                cards = [
                    ("MAE", formatar_numero(m["mae_cirurgias_dia"]), "cirurgias/dia"),
                    ("MAPE", formatar_numero(m["mape_percentual"], 1) + ("%" if m["mape_percentual"] is not None else ""), f"n={m['mape_n_realizado_maior_zero']} com realizado > 0"),
                    ("WAPE", formatar_numero(m["wape_percentual"], 1) + ("%" if m["wape_percentual"] is not None else ""), "ponderado pelo realizado total"),
                    ("Viés direcional", formatar_numero(bias), f"cirurgias/dia · {direcao}"),
                    ("Cobertura", formatar_numero(m["cobertura_percentual"], 1) + "%", f"{m['n_previsto']} de {DIAS_PERIODO} dias com previsão"),
                ]
                return "".join(
                    f'<div class="metric-card"><strong>{escape(str(valor))}</strong><span>{escape(str(label))}</span><small>{escape(str(nota))}</small></div>'
                    for label, valor, nota in cards
                )

            inicial = payload[DFU_PADRAO][HORIZONTE_PADRAO]
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=inicial["datas"], y=inicial["realizado"], mode="lines", name="Realizado",
                line={"color": CORES["realizado"], "width": 2.4},
                hovertemplate="Data %{x|%d/%m/%Y}<br>Cirurgias: %{y:.0f}<extra>Realizado</extra>",
            ))
            fig.add_trace(go.Scatter(
                x=inicial["datas"], y=inicial["previsto"], mode="lines", name="Previsto",
                line={"color": CORES["previsto"], "width": 2.4},
                customdata=[[inicial["origem"][i], inicial["modelo"][i]] for i in range(len(inicial["datas"]))],
                hovertemplate="Data-alvo %{x|%d/%m/%Y}<br>Origem: %{customdata[0]}<br>Modelo: %{customdata[1]}<br>Cirurgias: %{y:.1f}<extra>Previsto</extra>",
            ))
            fig.update_layout(
                title={"text": f"{DFU_PADRAO} · {HORIZONTE_PADRAO}", "x": 0},
                template="plotly_white", height=560, margin={"l": 65, "r": 25, "t": 70, "b": 60},
                font={"family": 'Inter, "Segoe UI", Arial, sans-serif', "color": CORES["primaria"]},
                paper_bgcolor="#ffffff", plot_bgcolor="#ffffff", hovermode="x unified",
                legend={"orientation": "h", "y": 1.08, "x": 0},
                xaxis={"title": "Data prevista", "gridcolor": "#e6e0ed"},
                yaxis={"title": "Cirurgias por dia (contagem)", "rangemode": "tozero", "gridcolor": "#e6e0ed"},
            )
            plot_html = fig.to_html(full_html=False, include_plotlyjs=True, config={"responsive": True, "displaylogo": False})
            payload_json = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).replace("</", "<\\/")
            dfu_options = "".join(
                f'<option value="{escape(str(dfu), quote=True)}"{" selected" if dfu == DFU_PADRAO else ""}>{escape(str(dfu))}</option>'
                for dfu in DFUS
            )
            horizonte_options = "".join(
                f'<option value="{h}"{" selected" if h == HORIZONTE_PADRAO else ""}>{h}</option>'
                for h in HORIZONTES_TEXTO
            )
            cards_iniciais = montar_cards(DFU_PADRAO, HORIZONTE_PADRAO)

            html = f"""
            <style>
              .campinas-report {{ font-family: Inter, "Segoe UI", Arial, sans-serif; color: #230550; }}
              .campinas-controls {{ display:flex; flex-wrap:wrap; gap:12px 20px; align-items:center; margin: 4px 0 14px; }}
              .campinas-controls label {{ font-weight:700; margin-right:6px; }}
              .campinas-controls select {{ min-width:170px; border:1px solid #e6e0ed; border-radius:8px; padding:8px 10px; color:#230550; background:#fff; font:inherit; }}
              .campinas-metrics {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(145px,1fr)); gap:10px; margin:0 0 8px; }}
              .campinas-metrics .metric-card {{ border:1px solid #e6e0ed; border-radius:10px; padding:10px 12px; background:#fff; min-height:68px; }}
              .campinas-metrics strong {{ display:block; color:#640ee6; font-size:18px; margin-bottom:3px; }}
              .campinas-metrics span {{ display:block; font-weight:700; font-size:12px; }}
              .campinas-metrics small {{ display:block; color:#595959; font-size:11px; margin-top:3px; }}
              @media(max-width:720px) {{ .campinas-controls {{ align-items:flex-start; flex-direction:column; gap:8px; }} .campinas-controls select {{ width:100%; }} }}
            </style>
            <div class="campinas-report">
              <div class="campinas-controls">
                <div><label for="campinas-dfu">DFU</label><select id="campinas-dfu">{dfu_options}</select></div>
                <div><label for="campinas-horizonte">Horizonte</label><select id="campinas-horizonte">{horizonte_options}</select></div>
                <span>Período: {data_inicio:%d/%m/%Y} a {data_fim:%d/%m/%Y}</span>
              </div>
              <div id="campinas-metricas" class="campinas-metrics">{cards_iniciais}</div>
              {plot_html}
            </div>
            <script>
            (function() {{
              const series = {payload_json};
              const wrapper = document.currentScript.parentElement;
              const dfuSelect = wrapper.querySelector('#campinas-dfu');
              const horizonSelect = wrapper.querySelector('#campinas-horizonte');
              const graph = wrapper.querySelector('.plotly-graph-div');
              const metrics = wrapper.querySelector('#campinas-metricas');
              function update() {{
                const dfu = dfuSelect.value;
                const horizon = horizonSelect.value;
                const item = series[dfu][horizon];
                const dates = item.datas;
                Plotly.update(graph,
                  {{x:[dates,dates], y:[item.realizado,item.previsto], customdata:[dates,dates.map((_,i)=>[item.origem[i],item.modelo[i]])]}},
                  {{title:{{text:dfu+' · '+horizon,x:0}}}}
                );
                const m = item.metricas;
                const fmt = (v,d=2) => v === null || !Number.isFinite(Number(v)) ? 'n/d' : Number(v).toLocaleString('pt-BR',{{minimumFractionDigits:d,maximumFractionDigits:d}});
                const direction = m.bias_direcional_cirurgias_dia === null ? 'sem previsões' : Number(m.bias_direcional_cirurgias_dia)>0 ? 'superestima' : Number(m.bias_direcional_cirurgias_dia)<0 ? 'subestima' : 'sem viés médio';
                const cards = [
                  ['MAE',fmt(m.mae_cirurgias_dia),'cirurgias/dia'],
                  ['MAPE',m.mape_percentual===null?'n/d':fmt(m.mape_percentual,1)+'%',`n=${{m.mape_n_realizado_maior_zero}} com realizado > 0`],
                  ['WAPE',m.wape_percentual===null?'n/d':fmt(m.wape_percentual,1)+'%','ponderado pelo realizado total'],
                  ['Viés direcional',fmt(m.bias_direcional_cirurgias_dia),`cirurgias/dia · ${{direction}}`],
                  ['Cobertura',fmt(m.cobertura_percentual,1)+'%',`${{m.n_previsto}} de {DIAS_PERIODO} dias com previsão`]
                ];
                metrics.innerHTML = cards.map(c=>'<div class="metric-card"><strong>'+c[1]+'</strong><span>'+c[0]+'</span><small>'+c[2]+'</small></div>').join('');
              }}
              dfuSelect.addEventListener('change',update);
              horizonSelect.addEventListener('change',update);
            }})();
            </script>
            """
            displayHTML(html)
            '''
        ),
    },
]

notebook = {
    "cells": cells,
    "metadata": {
        "application/vnd.databricks.v1+notebook": {
            "computePreferences": None,
            "dashboards": [],
            "environmentMetadata": None,
            "language": "python",
            "notebookMetadata": {"pythonIndentUnit": 4},
            "notebookName": "analise_realizado_previsao_campinas_90d",
            "widgets": {},
        },
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "version": "3"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

target = Path(__file__).with_name("analise_realizado_previsao_campinas_90d.ipynb")
target.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
print(target)
