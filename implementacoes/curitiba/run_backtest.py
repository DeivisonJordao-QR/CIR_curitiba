# Databricks notebook source
# MAGIC %pip install mlforecast==1.1.0 lightgbm==4.7.0

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import sys
from pathlib import Path

from pyspark.sql import SparkSession

try:
    context_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    relative_path = context_path.removeprefix("/Workspace/").lstrip("/")
    notebook_dir = (Path("/Workspace") / relative_path).parent.resolve()
except (AttributeError, NameError):
    notebook_dir = Path.cwd().resolve()
if str(notebook_dir) not in sys.path:
    sys.path.insert(0, str(notebook_dir))

from backtest import parse_period_start, parse_variants, run_backtest

spark_session = SparkSession.getActiveSession()
if spark_session is None:
    raise RuntimeError("Execute este notebook em uma sessão Spark do Databricks")

dbutils.widgets.text(
    "data_inicio_periodo", "01/01/2026", "Início do intervalo previsto (DD/MM/AAAA)"
)
dbutils.widgets.text("dias_previsao", "90", "Dias previstos")
dbutils.widgets.text("variantes", "sem_agenda", "Variantes: sem_agenda, com_agenda ou ambas")

period_start = parse_period_start(dbutils.widgets.get("data_inicio_periodo"))
forecast_days = int(dbutils.widgets.get("dias_previsao"))
variants = parse_variants(dbutils.widgets.get("variantes"))
run_backtest(spark_session, period_start, forecast_days, variants)
