"""Backtest walk-forward de cirurgias não eletivas com covariáveis point-in-time do PS.

Lê o snapshot de PS preparado e as 90 previsões por horizonte do experimento
dfu_seis_d1_d90. Para cada origem cirúrgica, monta a trajetória de PS que estava
disponível naquela origem e replica o Prophet da referência. Só grava CSV/JSON
sob o Workspace pessoal; não grava tabelas.
"""

from __future__ import annotations

import json
import math
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


USER = "deivison.jordao-dhauz@hospitalcare.com.br"
HUB = "Curitiba"
ESTABLISHMENT_CODE = "1"
ESTABLISHMENT_NAME = "Hospital Nossa Senhora do Pilar S.A."
SURGERIES_TABLE = "bi_dev.gold.hc_cirurgia"
AGENDA_TABLE = "bi_dev.gold.hc_agenda_cirurgica"
CALENDAR_TABLE = "planejamento_integrado.gold.calendario_curitiba"
PREPARED_DIR = Path(
    f"/Workspace/Users/{USER}/previsao_ps_curitiba/artifacts/1_preparacao_dados/"
    "prepared_data_20260925_125725"
)
PS_RUN_DIR = Path(
    f"/Workspace/Users/{USER}/previsao_ps_curitiba/experimentos/evolucao_modelo/"
    "runs/dfu_seis_d1_d90/snapshot_20260925_eventos_90d"
)
EVENT_START = pd.Timestamp("2026-06-27")
EVENT_END = pd.Timestamp("2026-09-24")
EVALUATION_HORIZONS = (1, 2, 7, 30, 90)
MIN_TRAIN_DAYS = 730

PS_FEATURES = (
    "Cardiologia",
    "Geniturinario",
    "Respiratorio_Tosse_Febre",
    "Doencas_Infecciosas_Parasitarias",
    "Geral",
    "Ortopedia",
)
PS_SOURCE_COLUMNS = {
    "Cardiologia": "Pilar_Cardiologia",
    "Geniturinario": "Pilar_Geniturinario",
    "Respiratorio_Tosse_Febre": "Pilar_Respiratorio_Tosse_Febre",
    "Doencas_Infecciosas_Parasitarias": "Pilar_Doencas_Infecciosas_Parasitarias",
    "Ortopedia": "Pilar_Ortopedia",
}
PS_DFU_TO_FEATURE = {
    f"Pilar_{feature}": feature for feature in PS_FEATURES
}


def _personal_root(spark: SparkSession) -> Path:
    user = spark.sql("SELECT current_user() AS user").first()["user"]
    if (user or "").casefold() != USER.casefold():
        raise RuntimeError(f"Identidade Databricks não é a esperada: {user}")
    root = (Path("/Workspace/Users") / user / "experimentos" / "previsao_urgentes_curitiba").resolve()
    user_home = (Path("/Workspace/Users") / user).resolve()
    if user_home not in root.parents:
        raise RuntimeError("Destino fora do Workspace pessoal")
    return root


def _log(log_path: Path, message: str) -> None:
    timestamp = datetime.now(ZoneInfo("America/Fortaleza")).isoformat(timespec="seconds")
    line = f"{timestamp} {message}"
    print(line, flush=True)
    with log_path.open("a", encoding="utf-8") as stream:
        stream.write(line + "\n")


def _read_ps_predictions() -> dict[int, pd.DataFrame]:
    """Carrega um checkpoint D+1..D+90 e valida a chave horizonte/origem/evento/DFU."""
    expected_dfus = set(PS_DFU_TO_FEATURE)
    run_manifest_path = PS_RUN_DIR / "manifest.json"
    if not run_manifest_path.is_file():
        raise FileNotFoundError(f"Manifesto do backtest de PS ausente: {run_manifest_path}")
    run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    if (
        run_manifest.get("event_start") != EVENT_START.date().isoformat()
        or run_manifest.get("event_end") != EVENT_END.date().isoformat()
        or run_manifest.get("horizons") != list(range(1, 91))
        or run_manifest.get("source_model") != "v1_prod Darts LightGBM, seed 42, retrain_days 7"
    ):
        raise ValueError("Manifesto do PS não corresponde ao backtest v1_prod solicitado")
    result: dict[int, pd.DataFrame] = {}
    required = {"data_evento", "data_processamento", "distancia_previsao", "dfu_ho", "previsao"}
    expected_events = pd.date_range(EVENT_START, EVENT_END, freq="D")
    for horizon in range(1, 91):
        path = PS_RUN_DIR / f"previsoes_d{horizon:02d}.csv"
        if not path.is_file():
            raise FileNotFoundError(f"Checkpoint do PS ausente: {path}")
        frame = pd.read_csv(path, parse_dates=["data_evento", "data_processamento"])
        missing = sorted(required.difference(frame.columns))
        if missing:
            raise ValueError(f"{path.name}: colunas ausentes: {missing}")
        frame["data_evento"] = frame["data_evento"].dt.normalize()
        frame["data_processamento"] = frame["data_processamento"].dt.normalize()
        if not frame["distancia_previsao"].eq(horizon).all():
            raise ValueError(f"{path.name}: distância de previsão incorreta")
        if frame.duplicated(["data_evento", "data_processamento", "dfu_ho"]).any():
            raise ValueError(f"{path.name}: chaves duplicadas")
        if set(frame["dfu_ho"].unique()) != expected_dfus:
            raise ValueError(f"{path.name}: DFUs não correspondem às seis séries aprovadas")
        actual_events = pd.DatetimeIndex(frame["data_evento"].unique()).sort_values()
        if not actual_events.equals(expected_events) or len(frame) != len(expected_events) * len(expected_dfus):
            raise ValueError(f"{path.name}: não cobre 90 datas × seis DFUs")
        values = pd.to_numeric(frame["previsao"], errors="coerce").to_numpy(dtype=float)
        if not np.isfinite(values).all() or (values < 0).any():
            raise ValueError(f"{path.name}: previsão nula, não finita ou negativa")
        expected_origins = frame["data_evento"] - pd.to_timedelta(horizon - 1, unit="D")
        if not frame["data_processamento"].eq(expected_origins).all():
            raise ValueError(f"{path.name}: data_processamento não confere com D+{horizon}")
        result[horizon] = frame
    return result


def _load_ps_history() -> pd.DataFrame:
    path = PREPARED_DIR / "target_time_series.pkl"
    if not path.is_file():
        raise FileNotFoundError(f"Snapshot preparado de PS ausente: {path}")
    source = pd.read_pickle(path)
    metadata_path = PREPARED_DIR / "metadata.pkl"
    if not metadata_path.is_file():
        raise FileNotFoundError(f"Metadados do snapshot preparados ausentes: {metadata_path}")
    metadata = pd.read_pickle(metadata_path)
    snapshot_end = pd.Timestamp(metadata["data_period"]["end"]).normalize()
    if snapshot_end != EVENT_END:
        raise ValueError(f"Snapshot PS termina em {snapshot_end.date()}, esperado {EVENT_END.date()}")
    source.index = pd.DatetimeIndex(source.index).tz_localize(None).normalize()
    if source.index.has_duplicates or not source.index.is_monotonic_increasing:
        raise ValueError("O índice do snapshot de PS deve ser diário, único e crescente")
    expected = set(PS_SOURCE_COLUMNS.values()) | {
        "Pilar_Geral", "Pilar_Digestivo", "Pilar_Lesoes_Causas_Externas"
    }
    missing = sorted(expected.difference(source.columns))
    if missing:
        raise ValueError(f"Snapshot PS sem séries necessárias: {missing}")
    history = pd.DataFrame(index=source.index)
    for feature, column in PS_SOURCE_COLUMNS.items():
        history[feature] = pd.to_numeric(source[column], errors="coerce")
    history["Geral"] = source[[
        "Pilar_Geral", "Pilar_Digestivo", "Pilar_Lesoes_Causas_Externas"
    ]].sum(axis=1, min_count=3)
    history = history[list(PS_FEATURES)].astype(float)
    if history.isna().any().any() or (history < 0).any().any():
        raise ValueError("Snapshot PS contém covariáveis históricas inválidas")
    return history


def _non_elective_daily(spark: SparkSession) -> tuple[pd.Series, dict[str, int]]:
    """Usa o complemento da regra de eletivas sobre a mesma população do Pilar."""
    surgery = (
        spark.table(SURGERIES_TABLE)
        .where(F.col("hub") == HUB)
        .where(F.trim(F.col("cd_estabelecimento").cast("string")) == ESTABLISHMENT_CODE)
    )
    agenda = (
        spark.table(AGENDA_TABLE)
        .where(F.col("hub") == HUB)
        .where(F.lower(F.trim(F.col("ds_estabelecimento"))) == ESTABLISHMENT_NAME.casefold())
    )
    surgery_required = {
        "hub", "cd_estabelecimento", "nr_cirurgia", "dt_inicio_real",
        "ds_status_cirurgia", "ds_procedimento",
    }
    agenda_required = {"hub", "nr_cirurgia", "dt_agendamento", "ds_estabelecimento", "ds_procedimento"}
    if missing := sorted(surgery_required.difference(surgery.columns)):
        raise ValueError(f"{SURGERIES_TABLE} sem colunas: {missing}")
    if missing := sorted(agenda_required.difference(agenda.columns)):
        raise ValueError(f"{AGENDA_TABLE} sem colunas: {missing}")
    for frame, source, column in (
        (surgery, SURGERIES_TABLE, "dt_inicio_real"),
        (agenda, AGENDA_TABLE, "dt_agendamento"),
    ):
        if frame.schema[column].dataType.simpleString() not in ("timestamp", "timestamp_ntz"):
            raise TypeError(f"{source}.{column} deve preservar data e hora")

    base = (
        surgery.where(F.col("ds_status_cirurgia") == "Realizada")
        .where(F.col("dt_inicio_real").isNotNull())
        .where(F.col("nr_cirurgia").isNotNull())
        .where(~F.lower(F.col("ds_procedimento")).like("%robótica%"))
        .where(F.to_date("dt_inicio_real") <= F.lit(EVENT_END.date()).cast("date"))
        .withColumn("nr_cirurgia", F.col("nr_cirurgia").cast("string"))
    )
    window = Window.partitionBy("hub", "nr_cirurgia").orderBy(F.col("dt_inicio_real").asc())
    surgeries = base.withColumn("_row", F.row_number().over(window)).where(F.col("_row") == 1).drop("_row")

    agenda_base = (
        agenda.where(F.col("nr_cirurgia").isNotNull())
        .where(~F.lower(F.col("ds_procedimento")).like("%robótica%"))
        .withColumn("nr_cirurgia", F.col("nr_cirurgia").cast("string"))
    )
    first_creation = agenda_base.groupBy("hub", "nr_cirurgia").agg(
        F.min("dt_agendamento").alias("first_created")
    )
    classified = surgeries.join(first_creation, ["hub", "nr_cirurgia"], "left").withColumn(
        "eletiva_24h",
        F.when(F.col("first_created").isNull(), F.lit(False))
        .otherwise(F.expr("dt_inicio_real >= first_created + INTERVAL 24 HOURS")),
    )
    classified = classified.withColumn("nao_eletiva_24h", ~F.col("eletiva_24h"))

    audit_row = classified.agg(
        F.count("*").alias("populacao_base"),
        F.sum(F.col("eletiva_24h").cast("int")).alias("eletivas_24h"),
        F.sum(F.col("nao_eletiva_24h").cast("int")).alias("nao_eletivas_24h"),
        F.sum(F.col("first_created").isNull().cast("int")).alias("sem_agenda_criacao_valida"),
        F.sum((F.col("first_created").isNotNull() & F.expr(
            "dt_inicio_real < first_created + INTERVAL 24 HOURS"
        )).cast("int")).alias("com_agenda_menos_24h"),
        F.sum((F.col("first_created").isNotNull() & F.expr(
            "dt_inicio_real = first_created + INTERVAL 24 HOURS"
        )).cast("int")).alias("exatamente_24h_eletivas"),
    ).first().asDict()
    audit = {key: int(value or 0) for key, value in audit_row.items()}
    if audit["populacao_base"] != audit["eletivas_24h"] + audit["nao_eletivas_24h"]:
        raise RuntimeError("A partição 24h não fecha com a população cirúrgica")
    if audit["populacao_base"] == 0:
        raise RuntimeError("A população cirúrgica do Pilar está vazia")

    daily = (
        classified.where(F.col("nao_eletiva_24h"))
        .where(F.to_date("dt_inicio_real") >= F.lit("2023-01-01").cast("date"))
        .groupBy(F.to_date("dt_inicio_real").alias("data_evento"))
        .agg(F.countDistinct("nr_cirurgia").alias("realizado"))
        .toPandas()
    )
    daily["data_evento"] = pd.to_datetime(daily["data_evento"]).dt.normalize()
    series = daily.set_index("data_evento")["realizado"].sort_index()
    index = pd.date_range("2023-01-01", EVENT_END, freq="D")
    series = series.reindex(index, fill_value=0).astype(float)
    series.index.name = "data_evento"
    return series, audit


def _school_vacation(spark: SparkSession) -> pd.Series:
    calendar = spark.table(CALENDAR_TABLE)
    required = {"data", "ferias_escolares"}
    if missing := sorted(required.difference(calendar.columns)):
        raise ValueError(f"{CALENDAR_TABLE} sem colunas: {missing}")
    result = (
        calendar.groupBy(F.to_date("data").alias("data_evento"))
        .agg(F.max(F.when(F.col("ferias_escolares") == True, 1).otherwise(0)).alias("ferias_escolares"))
        .toPandas()
    )
    result["data_evento"] = pd.to_datetime(result["data_evento"]).dt.normalize()
    return result.set_index("data_evento")["ferias_escolares"].astype(float)


def _future_ps_path(
    predictions: dict[int, pd.DataFrame], origin: pd.Timestamp, length: int
) -> pd.DataFrame:
    rows = []
    for lead in range(1, length + 1):
        event_date = origin + pd.Timedelta(days=lead - 1)
        source = predictions[lead]
        selected = source.loc[
            source["data_processamento"].eq(origin)
            & source["data_evento"].eq(event_date),
            ["dfu_ho", "previsao"],
        ]
        if len(selected) != len(PS_FEATURES):
            raise ValueError(
                f"PS sem trajetória completa na origem {origin.date()}, D+{lead}, "
                f"evento {event_date.date()}: {len(selected)} linhas"
            )
        row = {PS_DFU_TO_FEATURE[name]: float(value) for name, value in zip(
            selected["dfu_ho"], selected["previsao"], strict=True
        )}
        if set(row) != set(PS_FEATURES):
            raise ValueError(f"D+{lead}: DFUs PS divergentes em {origin.date()}")
        row["data_evento"] = event_date
        rows.append(row)
    return pd.DataFrame(rows).set_index("data_evento")[list(PS_FEATURES)].sort_index()


def _metrics(predictions: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for horizon, group in predictions.groupby("distancia_previsao", sort=True):
        error = group["previsao"] - group["realizado"]
        denominator = float(group["realizado"].sum())
        rows.append({
            "distancia_previsao": int(horizon),
            "observacoes": int(len(group)),
            "origens": int(group["data_processamento"].nunique()),
            "primeiro_evento": group["data_evento"].min().date().isoformat(),
            "ultimo_evento": group["data_evento"].max().date().isoformat(),
            "mae": float(error.abs().mean()),
            "rmse": float(np.sqrt(np.square(error).mean())),
            "vies": float(error.mean()),
            "wape": float(error.abs().sum() / denominator) if denominator else math.nan,
        })
    return pd.DataFrame(rows)


def run_backtest(spark: SparkSession) -> Path:
    """Executa as origens diárias com caminho PS realmente disponível em cada corte."""
    personal_root = _personal_root(spark)
    run_name = f"nao_eletivas_{EVENT_START:%Y%m%d}_{EVENT_END:%Y%m%d}_{datetime.now():%Y%m%d_%H%M%S}"
    output_dir = personal_root / "backtests" / run_name
    checkpoint_dir = output_dir / "checkpoints_origem"
    output_dir.mkdir(parents=True, exist_ok=False)
    checkpoint_dir.mkdir()
    log_path = output_dir / "execucao.log"
    run_started = time.monotonic()
    total_origins = len(pd.date_range(EVENT_START, EVENT_END, freq="D"))
    _log(log_path, f"status=started origins={total_origins} horizons={EVALUATION_HORIZONS} event_start={EVENT_START.date()} event_end={EVENT_END.date()} output={output_dir}")
    _log(log_path, "status=loading_ps_predictions run=v1_prod retrain_days=7")
    ps_predictions = _read_ps_predictions()
    _log(log_path, "status=loading_ps_snapshot snapshot=prepared_data_20260925_125725")
    ps_history = _load_ps_history()
    _log(log_path, f"status=loading_surgery_sources surgery_table={SURGERIES_TABLE} agenda_table={AGENDA_TABLE}")
    surgery_history, audit = _non_elective_daily(spark)
    _log(log_path, f"status=loading_calendar calendar_table={CALENDAR_TABLE} feature=ferias_escolares")
    school_vacation = _school_vacation(spark)

    if EVENT_END > ps_history.index.max():
        raise ValueError("O snapshot realizado do PS não cobre o fim da janela de evento")
    if not surgery_history.index.isin(ps_history.index).all():
        raise ValueError("Snapshot do PS não cobre todas as datas históricas de treino cirúrgico")
    ps_history = ps_history.reindex(surgery_history.index)
    dates = pd.date_range(surgery_history.index.min(), EVENT_END, freq="D")
    surgery_history = surgery_history.reindex(dates, fill_value=0.0)
    ps_history = ps_history.reindex(dates)
    vacation = school_vacation.reindex(dates).fillna(0.0)
    historical_covariates = ps_history.copy()
    historical_covariates["ferias_escolares"] = vacation
    if historical_covariates.isna().any().any():
        raise ValueError("Covariáveis históricas do PS/calendário contêm lacunas")

    from darts import TimeSeries
    from darts.models import Prophet
    try:
        import mlflow
        mlflow.autolog(disable=True)
    except ImportError:
        pass

    path_rows = []
    origin_rows = []
    origin_durations: list[float] = []

    # A origem começa no primeiro dia para o qual há vintages PS. Cada arquivo D+q
    # fornece o valor do evento origem+q-1 emitido exatamente naquela origem.
    for origin_number, origin in enumerate(pd.date_range(EVENT_START, EVENT_END, freq="D"), start=1):
        origin_started = time.monotonic()
        _log(log_path, f"status=origin_started origin={origin.date()} progress={origin_number}/{total_origins}")
        path_length = min(90, (EVENT_END - origin).days + 1)
        forecast_dates = pd.date_range(origin, periods=path_length, freq="D")
        train_end = origin - pd.Timedelta(days=1)
        train_series_pd = surgery_history.loc[:train_end]
        if len(train_series_pd) < MIN_TRAIN_DAYS:
            raise ValueError(f"Treino insuficiente em {origin.date()}: {len(train_series_pd)} dias")

        future_ps = _future_ps_path(ps_predictions, origin, path_length)
        future_calendar = vacation.reindex(forecast_dates).fillna(0.0)
        future_cov_pd = future_ps.copy()
        future_cov_pd["ferias_escolares"] = future_calendar
        all_cov_pd = pd.concat([
            historical_covariates.loc[:train_end], future_cov_pd,
        ]).sort_index()
        all_cov_pd = all_cov_pd.loc[~all_cov_pd.index.duplicated(keep="last")]
        if not all_cov_pd.index.equals(pd.date_range(all_cov_pd.index.min(), forecast_dates[-1], freq="D")):
            raise ValueError(f"Covariáveis não formam série diária na origem {origin.date()}")

        train_ts = TimeSeries.from_series(train_series_pd.astype(float), freq="D")
        covariates_ts = TimeSeries.from_dataframe(
            all_cov_pd.rename_axis("data_evento").reset_index(),
            time_col="data_evento", value_cols=list(all_cov_pd.columns), freq="D",
        )
        model = Prophet(country_holidays="BR")
        try:
            model.fit(series=train_ts, future_covariates=covariates_ts)
            predicted = model.predict(n=path_length, future_covariates=covariates_ts)
        except Exception as exc:
            _log(log_path, f"status=origin_failed origin={origin.date()} error={type(exc).__name__}: {exc}")
            raise
        prediction_frame = predicted.to_dataframe().iloc[:, 0]
        prediction_frame.index = pd.DatetimeIndex(prediction_frame.index).tz_localize(None).normalize()

        origin_checkpoint = pd.DataFrame({
            "data_processamento": origin,
            "data_evento": prediction_frame.index,
            "distancia_previsao": range(1, path_length + 1),
            "previsao": prediction_frame.to_numpy(dtype=float).clip(min=0),
            "realizado": [float(surgery_history.loc[event_date]) for event_date in prediction_frame.index],
        })
        origin_checkpoint.to_csv(checkpoint_dir / f"origem_{origin:%Y%m%d}.csv", index=False)

        for lead, event_date in enumerate(forecast_dates, start=1):
            if lead not in EVALUATION_HORIZONS:
                continue
            path_rows.append({
                "data_processamento": origin,
                "data_evento": event_date,
                "distancia_previsao": lead,
                "previsao": max(0.0, float(prediction_frame.loc[event_date])),
                "realizado": float(surgery_history.loc[event_date]),
                "modelo_cirurgia": "Prophet_country_holidays_BR",
                "modelo_covariavel_ps": "v1_prod_retreino_semanal",
                "snapshot_ps": "prepared_data_20260925_125725",
                "run_ps": "snapshot_20260925_eventos_90d",
            })
        origin_rows.append({
            "data_processamento": origin,
            "dias_previstos": path_length,
            "treino_inicio": train_series_pd.index.min(),
            "treino_fim": train_end,
            "previsoes_ps_path_completo": True,
        })
        origin_duration = time.monotonic() - origin_started
        origin_durations.append(origin_duration)
        remaining_origins = total_origins - origin_number
        eta_seconds = remaining_origins * (sum(origin_durations) / len(origin_durations))
        eta_fortaleza = datetime.now(ZoneInfo("America/Fortaleza")).timestamp() + eta_seconds
        eta_text = datetime.fromtimestamp(eta_fortaleza, ZoneInfo("America/Fortaleza")).strftime("%Y-%m-%d %H:%M:%S")
        _log(log_path, f"status=origin_done origin={origin.date()} progress={origin_number}/{total_origins} forecast_days={path_length} elapsed_min={origin_duration/60:.2f} run_elapsed_min={(time.monotonic()-run_started)/60:.2f} eta_remaining_min={eta_seconds/60:.1f} eta_fortaleza={eta_text} eta_basis=mean_completed_origins checkpoints={checkpoint_dir}")

    predictions = pd.DataFrame(path_rows).sort_values(
        ["distancia_previsao", "data_evento", "data_processamento"]
    ).reset_index(drop=True)
    if predictions.empty or predictions.duplicated(["data_processamento", "data_evento"]).any():
        raise RuntimeError("Backtest não produziu previsões válidas")
    metrics = _metrics(predictions)
    coverage = pd.DataFrame(origin_rows)
    predictions.to_csv(output_dir / "previsoes_horizontes.csv", index=False)
    metrics.to_csv(output_dir / "metricas_por_horizonte.csv", index=False)
    coverage.to_csv(output_dir / "auditoria_origens.csv", index=False)
    manifest = {
        "event_start_ps": EVENT_START.date().isoformat(),
        "event_end_ps": EVENT_END.date().isoformat(),
        "event_days_ps": int((EVENT_END - EVENT_START).days + 1),
        "horizons_evaluated": list(EVALUATION_HORIZONS),
        "surgery_target": "complemento da regra de eletivas: sem first_created válido ou dt_inicio_real < first_created + 24h",
        "rule_boundary": "exatamente 24h é eletiva",
        "surgery_scope": {"hub": HUB, "cd_estabelecimento": ESTABLISHMENT_CODE, "estabelecimento": ESTABLISHMENT_NAME},
        "ps_run": str(PS_RUN_DIR),
        "ps_snapshot": str(PREPARED_DIR),
        "ps_features": list(PS_FEATURES),
        "ps_geral": "Pilar_Geral + Pilar_Digestivo + Pilar_Lesoes_Causas_Externas",
        "point_in_time": "para cada origem, usa D+1..D+n do PS emitidos naquela mesma origem",
        "table_writes": False,
        "surgery_audit": audit,
        "observations_by_horizon": metrics.to_dict(orient="records"),
        "note": "D+90 tem uma origem com este intervalo PS de 90 datas; métrica é diagnóstica e não tem estabilidade para seleção de modelo.",
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    _log(log_path, f"status=complete elapsed_min={(time.monotonic()-run_started)/60:.1f} output={output_dir}")
    _log(log_path, f"surgery_rule_audit={audit}")
    _log(log_path, "metrics_by_horizon_start")
    _log(log_path, metrics.to_string(index=False))
    return output_dir

