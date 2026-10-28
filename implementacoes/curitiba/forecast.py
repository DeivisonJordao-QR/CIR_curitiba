"""Modelos de previsão diária para as variantes com e sem agenda."""

from __future__ import annotations

from datetime import timedelta
from typing import Callable

import pandas as pd

from common import DFUS, LONG_HORIZON, SHORT_HORIZON


CALENDAR_FEATURES = ["feriado", "periodo_baixa"]
SHORT_LAGS = [1, 2, 3, 4, 5, 6, 7]
LONG_LAGS = [1, 2, 3, 4, 5, 6, 7, 14]
ProgressCallback = Callable[[str], None]


def _forecast(lags: list[int], *, long_term: bool = False):
    try:
        from lightgbm import LGBMRegressor
        from mlforecast import MLForecast
        from mlforecast.lag_transforms import RollingMean
    except ImportError as exc:
        raise RuntimeError(
            "Instale as dependências de requirements.txt no ambiente do notebook "
            "antes de executar o experimento."
        ) from exc

    date_features = ["dayofweek", "month", "year", "week"]
    if long_term:
        date_features += ["quarter", "is_month_start", "is_month_end"]
    return MLForecast(
        models=[LGBMRegressor(verbosity=-1, random_state=42, n_jobs=1)],
        freq="D",
        lags=lags,
        lag_transforms={1: [RollingMean(window_size=7), RollingMean(window_size=30)]},
        date_features=date_features,
    )


def _as_pandas(frame) -> pd.DataFrame:
    result = frame.copy() if isinstance(frame, pd.DataFrame) else frame.toPandas()
    result["ds"] = pd.to_datetime(result["ds"])
    return result.sort_values(["dfu", "ds"]).reset_index(drop=True)


def _check_future_grid(future: pd.DataFrame, as_of, horizon: int) -> None:
    expected = set(pd.date_range(as_of, periods=horizon, freq="D"))
    for dfu in DFUS:
        actual = set(future.loc[future["dfu"] == dfu, "ds"])
        if actual != expected:
            raise ValueError(f"Calendário futuro incompleto para DFU {dfu}")
    if future.duplicated(["ds", "dfu"]).any():
        raise ValueError("Calendário futuro contém duplicatas por data/DFU")


def _daily_total_model(
    train: pd.DataFrame,
    future: pd.DataFrame,
    horizon: int,
    *,
    progress: ProgressCallback | None = None,
    stage: str = "modelo_diario",
) -> pd.DataFrame:
    """Reproduz o LightGBM local por DFU do caminho de longo prazo."""
    pieces = []
    for index, dfu in enumerate(DFUS, start=1):
        if progress:
            progress(f"{stage}: treinamento DFU {index}/{len(DFUS)} ({dfu}) iniciado")
        history = train.loc[train["dfu"] == dfu].copy()
        exog = future.loc[future["dfu"] == dfu].copy()
        history["unique_id"] = dfu
        exog["unique_id"] = dfu
        fit_cols = ["unique_id", "ds", "y", *CALENDAR_FEATURES]
        x_cols = ["unique_id", "ds", *CALENDAR_FEATURES]
        model = _forecast(LONG_LAGS, long_term=True)
        model.fit(history[fit_cols].sort_values("ds"), static_features=[])
        if progress:
            progress(f"{stage}: treinamento DFU {index}/{len(DFUS)} ({dfu}) concluído; inferência iniciada")
        prediction = model.predict(h=horizon, X_df=exog[x_cols].sort_values("ds"))
        pieces.append(
            prediction[["ds", "LGBMRegressor"]]
            .assign(dfu=dfu)
            .rename(columns={"LGBMRegressor": "previsao"})
        )
        if progress:
            progress(f"{stage}: inferência DFU {index}/{len(DFUS)} ({dfu}) concluída")
    return pd.concat(pieces, ignore_index=True)


def predict_without_agenda(
    daily_frame, future_frame, as_of, horizon_days: int = LONG_HORIZON,
    *, progress: ProgressCallback | None = None,
) -> pd.DataFrame:
    """Usa a agenda somente no rótulo comum, nunca em features ou pós-processamento."""
    if not 1 <= horizon_days <= LONG_HORIZON:
        raise ValueError(f"Horizonte deve estar entre 1 e {LONG_HORIZON} dias")
    daily = _as_pandas(daily_frame)
    future = _as_pandas(future_frame)
    _check_future_grid(future, as_of, horizon_days)
    prediction = _daily_total_model(
        daily, future, horizon_days, progress=progress, stage="sem_agenda"
    )
    prediction["previsao_final"] = (
        prediction["previsao"].clip(lower=0).round(0).astype("int64")
    )
    return _finalize(
        prediction, as_of, "sem_agenda", with_agenda=False, horizon_days=horizon_days
    )


def _short_with_agenda(
    panel: pd.DataFrame,
    future: pd.DataFrame,
    as_of,
    horizon_days: int,
    *,
    progress: ProgressCallback | None = None,
) -> pd.DataFrame:
    short_horizon = min(SHORT_HORIZON, horizon_days)
    selected = future.loc[
        future["ds"] < pd.Timestamp(as_of + timedelta(days=short_horizon))
    ].copy()
    selected["feature_dfu"] = selected["dfu"].map(
        {name: index + 1 for index, name in enumerate(DFUS)}
    )
    panel["feature_dfu"] = panel["dfu"].map(
        {name: index + 1 for index, name in enumerate(DFUS)}
    )
    training_by_lead = {
        int(lead): group.copy()
        for lead, group in panel.groupby("lead", sort=False)
    }
    exog_by_lead = {}
    for lead in range(1, short_horizon + 1):
        exog = selected.loc[
            selected["ds"] <= pd.Timestamp(as_of + timedelta(days=lead - 1))
        ].copy()
        exog["unique_id"] = exog["dfu"]
        exog_by_lead[lead] = exog
    pieces = []
    fit_cols = ["unique_id", "ds", "y", "agendamentos", *CALENDAR_FEATURES]
    x_cols = ["unique_id", "ds", "agendamentos", *CALENDAR_FEATURES]
    for lead in range(1, short_horizon + 1):
        training = training_by_lead[lead].copy()
        training["unique_id"] = training["dfu"]
        training["y"] = training["y"] - training["agendamentos"]

        # Assim como a referência, há um modelo global e um local por DFU
        # para cada antecedência. A seleção usa o maior LightGBM previsto.
        global_model = _forecast(SHORT_LAGS)
        global_model.fit(
            training[fit_cols + ["feature_dfu"]].sort_values(["unique_id", "ds"]),
            static_features=["feature_dfu"],
        )
        exog = exog_by_lead[lead]
        global_pred = global_model.predict(
            h=lead, X_df=exog[x_cols].sort_values(["unique_id", "ds"])
        )
        forecast_date = pd.Timestamp(as_of + timedelta(days=lead - 1))
        global_pred = global_pred.loc[global_pred["ds"] == forecast_date].set_index("unique_id")

        for dfu in DFUS:
            local = training.loc[training["dfu"] == dfu, fit_cols].copy()
            local_model = _forecast(SHORT_LAGS)
            local_model.fit(local.sort_values("ds"), static_features=[])
            local_exog = exog.loc[exog["dfu"] == dfu, x_cols]
            local_pred = local_model.predict(h=lead, X_df=local_exog.sort_values("ds"))
            local_value = float(
                local_pred.loc[local_pred["ds"] == forecast_date, "LGBMRegressor"].iloc[0]
            )
            global_value = float(global_pred.loc[dfu, "LGBMRegressor"])
            scheduled = int(
                selected.loc[
                    (selected["dfu"] == dfu) & (selected["ds"] == forecast_date),
                    "agendamentos",
                ].iloc[0]
            )
            total = max(global_value, local_value) + scheduled
            pieces.append(
                {"ds": forecast_date, "dfu": dfu, "agendamentos": scheduled,
                 "previsao": max(total, scheduled, 0)}
            )
        if progress and (lead % 10 == 0 or lead == short_horizon):
            progress(f"com_agenda: leads curtos 1–{lead}/{short_horizon} concluídos")
    return pd.DataFrame(pieces)


def predict_with_agenda(
    panel_frame, daily_frame, future_frame, as_of, horizon_days: int = LONG_HORIZON,
    *, progress: ProgressCallback | None = None,
) -> pd.DataFrame:
    if not 1 <= horizon_days <= LONG_HORIZON:
        raise ValueError(f"Horizonte deve estar entre 1 e {LONG_HORIZON} dias")
    panel = _as_pandas(panel_frame)
    daily = _as_pandas(daily_frame)
    future = _as_pandas(future_frame)
    _check_future_grid(future, as_of, horizon_days)
    short_horizon = min(SHORT_HORIZON, horizon_days)
    short = _short_with_agenda(
        panel, future, as_of, horizon_days, progress=progress
    )

    if horizon_days > short_horizon:
        # O longo prazo parte do histórico real e dos valores previstos no
        # trecho curto, mantendo o comportamento do forecast operacional.
        extension = short[["ds", "dfu", "previsao"]].rename(columns={"previsao": "y"})
        extension = extension.merge(
            future[["ds", "dfu", *CALENDAR_FEATURES]], on=["ds", "dfu"], how="left",
            validate="one_to_one",
        )
        training = pd.concat([daily, extension], ignore_index=True)
        long_future = future.loc[
            future["ds"] >= pd.Timestamp(as_of + timedelta(days=short_horizon))
        ].copy()
        long_prediction = _daily_total_model(
            training, long_future, horizon_days - short_horizon,
            progress=progress, stage="com_agenda longo",
        )
        long_prediction = long_prediction.merge(
            long_future[["ds", "dfu", "agendamentos"]],
            on=["ds", "dfu"], how="left", validate="one_to_one",
        )
        long_prediction["previsao"] = long_prediction[
            ["previsao", "agendamentos"]
        ].max(axis=1).clip(lower=0)
        combined = pd.concat(
            [short[["ds", "dfu", "agendamentos", "previsao"]],
             long_prediction[["ds", "dfu", "agendamentos", "previsao"]]],
            ignore_index=True,
        )
    else:
        combined = short[["ds", "dfu", "agendamentos", "previsao"]].copy()
    combined["previsao_final"] = combined["previsao"].round(0).astype("int64")
    return _finalize(
        combined, as_of, "com_agenda", with_agenda=True, horizon_days=horizon_days
    )


def _finalize(
    prediction: pd.DataFrame, as_of, variant: str, *, with_agenda: bool,
    horizon_days: int = LONG_HORIZON,
) -> pd.DataFrame:
    result = prediction.copy()
    result["hub"] = "Curitiba"
    result["variante"] = variant
    result["data_processamento"] = as_of
    result["distancia_previsao"] = (result["ds"].dt.date - as_of).map(lambda d: d.days + 1)
    columns = ["ds", "dfu", "previsao_final"]
    if with_agenda:
        columns.append("agendamentos")
    columns += ["hub", "variante", "data_processamento", "distancia_previsao"]
    result = result[columns].sort_values(["ds", "dfu"]).reset_index(drop=True)
    if result.duplicated(["ds", "dfu"]).any():
        raise ValueError(f"Previsões duplicadas na variante {variant}")
    if result["previsao_final"].isna().any() or (result["previsao_final"] < 0).any():
        raise ValueError(f"Previsão inválida na variante {variant}")
    if len(result) != horizon_days * len(DFUS):
        raise ValueError(f"Horizonte incompleto na variante {variant}: {len(result)} linhas")
    if with_agenda and (result["previsao_final"] < result["agendamentos"]).any():
        raise ValueError("Previsão com agenda inferior ao volume agendado")
    return result
