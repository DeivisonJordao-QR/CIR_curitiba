"""Contrato da variante que usa agenda conhecida em cada corte."""

from forecast import predict_with_agenda


def run(
    historical_panel, daily_history, future_with_agenda, as_of,
    horizon_days=365, *, progress=None,
):
    required_panel = {"ds", "dfu", "lead", "y", "agendamentos"}
    required_future = {"ds", "dfu", "agendamentos"}
    if not required_panel.issubset(historical_panel.columns):
        raise ValueError("Painel histórico com agenda incompleto")
    if not required_future.issubset(future_with_agenda.columns):
        raise ValueError("Agenda futura incompleta")
    return predict_with_agenda(
        historical_panel, daily_history, future_with_agenda, as_of,
        horizon_days=horizon_days, progress=progress,
    )
