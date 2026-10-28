"""Contrato da variante que prevê diretamente o total diário."""

from forecast import predict_without_agenda


def run(daily_history, future_calendar, as_of, horizon_days=365, *, progress=None):
    required = {"ds", "dfu", "y", "feriado", "periodo_baixa"}
    if not required.issubset(daily_history.columns):
        raise ValueError("Histórico diário incompleto")
    forbidden = {"agendamentos", "lead", "dt_agendamento", "dt_agenda"}
    if forbidden.intersection(daily_history.columns) or forbidden.intersection(future_calendar.columns):
        raise ValueError("Variante sem agenda recebeu features de agenda")
    return predict_without_agenda(
        daily_history, future_calendar, as_of, horizon_days=horizon_days,
        progress=progress,
    )
