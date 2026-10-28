"""Backtest rolling por intervalo-alvo e horizonte para Curitiba."""

from __future__ import annotations

import json
import time
from collections import deque
from datetime import date, datetime, timedelta
from uuid import uuid4

import pandas as pd
from pyspark.sql import SparkSession

from com_agenda.implementation import run as run_with_agenda
from common import (
    AGENDA_TABLE,
    CALENDAR_TABLE,
    DFUS,
    ESTABLISHMENT_CODE,
    ESTABLISHMENT_NAME,
    HUB,
    LONG_HORIZON,
    PEOPLE_TABLE,
    START_DATE,
    SURGERIES_TABLE,
    _calendar,
    _historical_agenda_panel,
    load_backtest_inputs,
    personal_experiment_dir,
)
from sem_agenda.implementation import run as run_without_agenda


MIN_TRAIN_DAYS = 730
EVALUATION_HORIZONS = (1, 2, 7, 30, 90)
AVAILABLE_VARIANTS = ("sem_agenda", "com_agenda")


def _log(message: str) -> None:
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    print(f"[{timestamp}] {message}", flush=True)


def _duration(seconds: float) -> str:
    total = max(0, int(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def parse_period_start(value: str | date) -> date:
    if isinstance(value, date):
        return value
    for pattern in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, pattern).date()
        except ValueError:
            pass
    raise ValueError("Data inicial do período inválida; use AAAA-MM-DD ou DD/MM/AAAA")


def parse_variants(value: str | tuple[str, ...] | list[str]) -> tuple[str, ...]:
    """Parse selected model variants from a widget string or Python sequence."""
    if isinstance(value, str):
        variants = tuple(item.strip().lower() for item in value.split(",") if item.strip())
    else:
        variants = tuple(str(item).strip().lower() for item in value if str(item).strip())
    if not variants:
        raise ValueError("Selecione ao menos uma variante: sem_agenda e/ou com_agenda")
    if len(set(variants)) != len(variants):
        raise ValueError(f"Variantes repetidas: {variants}")
    unknown = sorted(set(variants).difference(AVAILABLE_VARIANTS))
    if unknown:
        raise ValueError(
            f"Variantes desconhecidas: {unknown}; disponíveis: {AVAILABLE_VARIANTS}"
        )
    return tuple(name for name in AVAILABLE_VARIANTS if name in variants)


def _future_calendar_for_cutoff(calendar: pd.DataFrame, cutoff: date, days: int) -> pd.DataFrame:
    dates = pd.date_range(cutoff, periods=days, freq="D")
    grid = pd.MultiIndex.from_product([dates, DFUS], names=["ds", "dfu"]).to_frame(index=False)
    future = grid.merge(calendar, on="ds", how="left")
    future["feriado"] = future["feriado"].fillna(0).astype("int64")
    future["periodo_baixa"] = (
        ((future["ds"].dt.month == 12) & (future["ds"].dt.day >= 22))
        | ((future["ds"].dt.month == 1) & (future["ds"].dt.day <= 5))
    ).astype("int64")
    return future


def _as_of_for_target(target_date: date, horizon: int) -> date:
    """Forecast starts H-1 days before the target; training ends the prior day."""
    return target_date - timedelta(days=horizon - 1)


def _rolling_origins(start: date, end: date, horizons: tuple[int, ...]) -> list[date]:
    origins = {
        _as_of_for_target(target, horizon)
        for target in (start + timedelta(days=offset) for offset in range((end - start).days + 1))
        for horizon in horizons
    }
    return sorted(origins)


def _agenda_at_cutoff(
    agenda_rows: pd.DataFrame, future_grid: pd.DataFrame, cutoff: date
) -> pd.DataFrame:
    """Build the known agenda at the start of a forecast origin without future revisions."""
    cutoff_value = pd.Timestamp(cutoff)
    created = agenda_rows["dt_agendamento"]
    scheduled_for = agenda_rows["dt_agenda"]
    cancelled_at = agenda_rows["cancelled_at"]
    after_cancellation = (cancelled_at > cutoff_value).fillna(False)
    status_available = (
        agenda_rows["ds_status_agenda"].ne("Cancelada").fillna(False)
        | after_cancellation
    )
    active = agenda_rows.loc[
        (created <= cutoff_value).fillna(False)
        & (cancelled_at.isna() | after_cancellation)
        & status_available
        & (scheduled_for.dt.normalize() >= cutoff_value)
    ].copy()
    active["ds"] = pd.to_datetime(active["dt_agenda"]).dt.normalize()
    scheduled = (
        active.groupby(["ds", "dfu"], as_index=False)["nr_cirurgia"]
        .nunique()
        .rename(columns={"nr_cirurgia": "agendamentos"})
    )
    future = future_grid.merge(scheduled, on=["ds", "dfu"], how="left")
    future["agendamentos"] = future["agendamentos"].fillna(0).astype("int64")
    return future


def _metric_rows(predictions: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    rows = []
    for dfu in ["Todos", *DFUS]:
        subset = predictions if dfu == "Todos" else predictions.loc[predictions["dfu"] == dfu]
        for (variant, lead), group in subset.groupby(
            ["variante", "distancia_previsao"], sort=True
        ):
            error = group["previsao_final"].astype(float) - group["realizado"].astype(float)
            denominator = float(group["realizado"].sum())
            rows.append(
                {
                    "data_inicio_periodo": start.isoformat(),
                    "data_fim_periodo": end.isoformat(),
                    "variante": variant,
                    "dfu": dfu,
                    "horizonte": f"D{int(lead)}",
                    "lead_dias": int(lead),
                    "mae": float(error.abs().mean()),
                    "rmse": float((error.pow(2).mean()) ** 0.5),
                    "vies_previsao_menos_realizado": float(error.mean()),
                    "wape": float(error.abs().sum() / denominator) if denominator else None,
                    "n_observacoes": int(len(group)),
                }
            )
    return pd.DataFrame(rows)


def run_backtest(
    spark: SparkSession,
    period_start_value: str | date,
    forecast_days: int,
    variants_value: str | tuple[str, ...] | list[str] = AVAILABLE_VARIANTS,
) -> dict[str, str]:
    """Run a rolling-origin evaluation for every target date and requested horizon."""
    run_started = time.monotonic()
    period_start = parse_period_start(period_start_value)
    selected_variants = parse_variants(variants_value)
    if not 1 <= int(forecast_days) <= LONG_HORIZON:
        raise ValueError(f"Dias previstos devem estar entre 1 e {LONG_HORIZON}")
    forecast_days = int(forecast_days)
    period_end = period_start + timedelta(days=forecast_days - 1)
    evaluated_horizons = tuple(
        lead for lead in EVALUATION_HORIZONS if lead <= forecast_days
    )
    if not evaluated_horizons:
        raise ValueError("O intervalo precisa incluir pelo menos o horizonte D1")

    _log(
        "Backtest rolling iniciado: "
        f"período={period_start.isoformat()} a {period_end.isoformat()}, "
        f"dias={forecast_days}, horizontes={[f'D{h}' for h in evaluated_horizons]}, "
        f"variantes={selected_variants}, DFUs={len(DFUS)}"
    )
    input_started = time.monotonic()
    _log("Carregando fontes e construindo painel histórico de agenda...")
    as_of, daily, agenda_rows_spark, eligibility_audit = load_backtest_inputs(spark)
    origins = _rolling_origins(period_start, period_end, evaluated_horizons)
    _log(
        f"Fontes carregadas em {_duration(time.monotonic() - input_started)}; "
        f"{len(origins)} origens móveis identificadas; data disponível até {as_of}."
    )
    earliest_origin = origins[0]
    minimum_origin = date.fromisoformat(START_DATE) + timedelta(days=MIN_TRAIN_DAYS)
    if earliest_origin < minimum_origin:
        raise ValueError(
            "O primeiro corte móvel deve ser "
            f"{minimum_origin.isoformat()} ou posterior para ter {MIN_TRAIN_DAYS} dias de treino"
        )
    if period_end >= as_of:
        raise ValueError(
            f"Ainda não há dados realizados completos até {period_end.isoformat()}"
        )

    preparation_started = time.monotonic()
    _log("Convertendo fontes para Pandas e preparando realizado, painel e calendário...")
    daily_pd = daily.toPandas()
    daily_pd["ds"] = pd.to_datetime(daily_pd["ds"])
    actual = daily_pd.loc[
        (daily_pd["ds"] >= pd.Timestamp(period_start))
        & (daily_pd["ds"] <= pd.Timestamp(period_end)),
        ["ds", "dfu", "y"],
    ].rename(columns={"y": "realizado"})
    if actual["ds"].nunique() != forecast_days or actual.duplicated(["ds", "dfu"]).any():
        raise ValueError(
            f"O período {period_start} a {period_end} não possui dados realizados completos"
        )

    if "com_agenda" in selected_variants:
        _log("Preparando painel pontual e agenda histórica para a variante com_agenda...")
        agenda_panel_started = time.monotonic()
        panel = _historical_agenda_panel(spark, agenda_rows_spark, daily).toPandas()
        panel["ds"] = pd.to_datetime(panel["ds"])
        agenda_pd = agenda_rows_spark.toPandas()
        for column in ("dt_agendamento", "dt_agenda", "cancelled_at"):
            agenda_pd[column] = pd.to_datetime(agenda_pd[column])
        _log(
            "Painel da agenda preparado em "
            f"{_duration(time.monotonic() - agenda_panel_started)}."
        )
    else:
        panel = None
        agenda_pd = None
    calendar_pd = _calendar(spark).toPandas()
    calendar_pd["ds"] = pd.to_datetime(calendar_pd["ds"])
    _log(f"Preparação concluída em {_duration(time.monotonic() - preparation_started)}.")

    all_forecasts = []
    recent_origin_durations: deque[float] = deque(maxlen=10)
    for origin_index, origin in enumerate(origins, start=1):
        origin_started = time.monotonic()
        available_horizons = [
            lead for lead in evaluated_horizons
            if period_start <= origin + timedelta(days=lead - 1) <= period_end
        ]
        if not available_horizons:
            continue
        model_horizon = max(available_horizons)
        detailed_progress = (
            origin_index == 1 or origin_index % 10 == 0 or origin_index == len(origins)
        )

        def model_progress(message: str) -> None:
            if detailed_progress:
                _log(f"Origem {origin_index}/{len(origins)} ({origin}): {message}")

        _log(
            f"Origem {origin_index}/{len(origins)} iniciada: início={origin}, "
            f"corte={origin - timedelta(days=1)}, "
            f"horizontes-alvo={[f'D{h}' for h in available_horizons]}, "
            f"trecho-modelo={model_horizon} dias."
        )
        origin_ts = pd.Timestamp(origin)
        feature_started = time.monotonic()
        training_daily = daily_pd.loc[daily_pd["ds"] < origin_ts].copy()
        if training_daily["ds"].nunique() < MIN_TRAIN_DAYS:
            raise ValueError(
                f"Histórico de treino menor que {MIN_TRAIN_DAYS} dias "
                f"no corte {origin - timedelta(days=1)}"
            )
        training_panel = (
            panel.loc[panel["ds"] < origin_ts].copy()
            if panel is not None else None
        )
        future_calendar = _future_calendar_for_cutoff(
            calendar_pd, origin, model_horizon
        )
        future_agenda = (
            _agenda_at_cutoff(agenda_pd, future_calendar, origin)
            if agenda_pd is not None else None
        )
        _log(
            f"Origem {origin_index}/{len(origins)}: dados de treino e features "
            f"preparados em {_duration(time.monotonic() - feature_started)}; "
            f"dias de treino={training_daily['ds'].nunique()}."
        )

        variant_forecast_map = {}
        if "sem_agenda" in selected_variants:
            variant_started = time.monotonic()
            _log(
                f"Origem {origin_index}/{len(origins)}: treinamento/inferência "
                "sem_agenda iniciados."
            )
            variant_forecast_map["sem_agenda"] = run_without_agenda(
                training_daily, future_calendar, origin, horizon_days=model_horizon,
                progress=model_progress,
            )
            _log(
                f"Origem {origin_index}/{len(origins)}: sem_agenda concluída em "
                f"{_duration(time.monotonic() - variant_started)}."
            )
        if "com_agenda" in selected_variants:
            variant_started = time.monotonic()
            _log(
                f"Origem {origin_index}/{len(origins)}: treinamento/inferência "
                "com_agenda iniciados."
            )
            variant_forecast_map["com_agenda"] = run_with_agenda(
                training_panel, training_daily, future_agenda, origin,
                horizon_days=model_horizon,
                progress=model_progress,
            )
            _log(
                f"Origem {origin_index}/{len(origins)}: com_agenda concluída em "
                f"{_duration(time.monotonic() - variant_started)}."
            )
        target_dates = {
            origin + timedelta(days=lead - 1) for lead in available_horizons
        }
        for variant_forecasts in variant_forecast_map.values():
            selected = variant_forecasts.loc[
                variant_forecasts["ds"].isin(pd.to_datetime(list(target_dates)))
            ].copy()
            selected = selected.merge(
                actual, on=["ds", "dfu"], how="left", validate="many_to_one"
            )
            if selected["realizado"].isna().any():
                raise ValueError("Realizados ausentes nas datas do backtest móvel")
            selected["horizonte"] = selected["distancia_previsao"].map(
                lambda lead: f"D{int(lead)}"
            )
            selected["data_inicio_periodo"] = period_start
            selected["data_corte"] = origin - timedelta(days=1)
            all_forecasts.append(selected)

        origin_seconds = time.monotonic() - origin_started
        recent_origin_durations.append(origin_seconds)
        elapsed = time.monotonic() - run_started
        average_origin = sum(recent_origin_durations) / len(recent_origin_durations)
        remaining_origins = len(origins) - origin_index
        eta_seconds = average_origin * remaining_origins
        _log(
            f"Origem {origin_index}/{len(origins)} concluída em "
            f"{_duration(origin_seconds)}; progresso={origin_index / len(origins):.1%}; "
            f"média móvel={_duration(average_origin)}/origem; "
            f"decorrido={_duration(elapsed)}; ETA aproximada={_duration(eta_seconds)}."
        )

    forecasts = pd.concat(all_forecasts, ignore_index=True)
    forecasts = forecasts.sort_values(
        ["ds", "dfu", "distancia_previsao", "variante"]
    ).reset_index(drop=True)
    expected_rows = (
        forecast_days * len(DFUS) * len(evaluated_horizons) * len(selected_variants)
    )
    if len(forecasts) != expected_rows or forecasts.duplicated(
        ["ds", "dfu", "distancia_previsao", "variante"]
    ).any():
        raise ValueError(
            f"Backtest móvel incompleto: esperado {expected_rows} linhas, "
            f"encontrado {len(forecasts)}"
        )
    forecasts["horizonte"] = forecasts["distancia_previsao"].map(
        lambda lead: f"D{int(lead)}"
    )
    metrics = _metric_rows(forecasts, period_start, period_end)

    as_of_text = as_of.isoformat()
    run_id = f"backtest_rolling_{period_start:%Y%m%d}_{forecast_days}d_{uuid4().hex[:8]}"
    root = personal_experiment_dir(spark) / "backtest" / run_id
    root.mkdir(parents=True, exist_ok=False)
    predictions_path = root / "previsoes_rolling.csv"
    metrics_path = root / "metricas.csv"
    forecasts.to_csv(predictions_path, index=False, date_format="%Y-%m-%d")
    metrics.to_csv(metrics_path, index=False)
    manifest = {
        "run_id": run_id,
        "hub": HUB,
        "estabelecimento": {
            "codigo": ESTABLISHMENT_CODE,
            "nome": ESTABLISHMENT_NAME,
        },
        "data_processamento": as_of_text,
        "data_inicio_periodo": period_start.isoformat(),
        "data_fim_periodo": period_end.isoformat(),
        "dias_previstos": forecast_days,
        "horizontes_avaliados": [f"D{lead}" for lead in evaluated_horizons],
        "quantidade_origens_moveis": len(origins),
        "tempo_execucao_segundos": round(time.monotonic() - run_started, 2),
        "observacoes_por_horizonte": forecast_days * len(DFUS),
        "dias_treino_minimos": MIN_TRAIN_DAYS,
        "variantes": list(selected_variants),
        "normalizacao_especialidades_curitiba": {
            "Ortopedia / Traumatologia": "Ortopedia",
            "Ginecologia": "Ginecologia e Obstetrícia",
            "Cancerologia": "Cirurgia Geral (mesmo grupo de Cirurgia Oncológica)",
        },
        "progresso_execucao": "logs com tempos por origem e variante, etapas de treino/inferência e ETA móvel",
        "metricas_descritivas_sem_limite_de_aprovacao": ["MAE", "RMSE", "vies", "WAPE"],
        "regra_eletividade": "dt_inicio_real - min(dt_agendamento) >= 24h por hub+nr_cirurgia",
        "sem_conversao_de_fuso": True,
        "fontes_somente_leitura": [
            SURGERIES_TABLE, AGENDA_TABLE, PEOPLE_TABLE, CALENDAR_TABLE
        ],
        "gravacao_em_tabelas": False,
        "historico_agenda_ponto_no_tempo": "aproximado; fonte consultada não é snapshot versionado de dt_agenda",
        "auditoria_eletividade": eligibility_audit,
        "artefatos": {
            "previsoes_rolling": str(predictions_path),
            "metricas": str(metrics_path),
        },
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    output = {
        "run_id": run_id,
        "pasta": str(root),
        "previsoes": str(predictions_path),
        "metricas": str(metrics_path),
        "manifesto": str(manifest_path),
    }
    _log(
        f"Backtest concluído em {_duration(time.monotonic() - run_started)}; "
        f"previsões={len(forecasts)} linhas; artefatos em {root}."
    )
    print(json.dumps(output, ensure_ascii=False, indent=2), flush=True)
    return output
