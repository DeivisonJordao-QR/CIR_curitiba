# Databricks notebook source
"""Executa as duas variantes de previsão diária na pasta pessoal do usuário.

Não cria tabelas, jobs nem logs compartilhados. As saídas são arquivos pequenos no
workspace pessoal do usuário autenticado.
"""

# COMMAND ----------
# MAGIC %pip install mlforecast==1.1.0 lightgbm==4.7.0

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------

import json
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession


# COMMAND ----------

try:
    context_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # type: ignore[name-defined]
    relative_path = context_path.removeprefix("/Workspace/").lstrip("/")
    notebook_dir = (Path("/Workspace") / relative_path).parent.resolve()
except (AttributeError, NameError):
    notebook_dir = Path.cwd().resolve()
if str(notebook_dir) not in sys.path:
    sys.path.insert(0, str(notebook_dir))

from common import (  # noqa: E402
    AGENDA_TABLE,
    CALENDAR_TABLE,
    ESTABLISHMENT_CODE,
    ESTABLISHMENT_NAME,
    HUB,
    PEOPLE_TABLE,
    SURGERIES_TABLE,
    load_inputs,
    personal_experiment_dir,
)
from com_agenda.implementation import run as run_with_agenda  # noqa: E402
from sem_agenda.implementation import run as run_without_agenda  # noqa: E402


def _save_variant(root: Path, variant: str, run_id: str, result, manifest: dict) -> Path:
    directory = (root / variant / run_id).resolve()
    if root not in directory.parents:
        raise RuntimeError(f"Destino fora da pasta pessoal: {directory}")
    directory.mkdir(parents=True, exist_ok=False)
    result.to_csv(directory / "previsao_diaria.csv", index=False, date_format="%Y-%m-%d")
    (directory / "manifest.json").write_text(
        json.dumps({**manifest, "variante": variant, "linhas": len(result)},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return directory


def main() -> dict[str, str]:
    spark_session = SparkSession.getActiveSession()
    if spark_session is None:
        raise RuntimeError("Execute este notebook em uma sessão Spark do Databricks")

    root = personal_experiment_dir(spark_session)
    if notebook_dir != root and root not in notebook_dir.parents:
        raise RuntimeError(
            f"O notebook deve estar no experimento pessoal {root}; atual: {notebook_dir}"
        )

    as_of, daily, historical_panel, future_grid, future_with_agenda, audit = load_inputs(
        spark_session
    )
    # Materialização única: as variantes usam exatamente os mesmos painéis nesta execução.
    daily_pd = daily.toPandas()
    panel_pd = historical_panel.toPandas()
    future_pd = future_grid.toPandas()
    future_agenda_pd = future_with_agenda.toPandas()
    if daily_pd.empty or daily_pd["y"].sum() == 0:
        raise ValueError(
            "Nenhuma cirurgia eletiva elegível no histórico de Curitiba para "
            f"{ESTABLISHMENT_NAME} (cd_estabelecimento={ESTABLISHMENT_CODE})"
        )

    without_agenda = run_without_agenda(daily_pd, future_pd, as_of)
    with_agenda = run_with_agenda(panel_pd, daily_pd, future_agenda_pd, as_of)

    # Apenas depois de validar as duas saídas iniciamos qualquer gravação.
    run_id = f"run_{as_of:%Y%m%d}_{uuid4().hex[:10]}"
    manifest = {
        "run_id": run_id,
        "hub": HUB,
        "estabelecimento": {
            "codigo": ESTABLISHMENT_CODE,
            "nome": ESTABLISHMENT_NAME,
        },
        "data_processamento": as_of.isoformat(),
        "criado_em": datetime.now().isoformat(timespec="seconds"),
        "fontes_somente_leitura": [
            SURGERIES_TABLE, AGENDA_TABLE, PEOPLE_TABLE, CALENDAR_TABLE,
        ],
        "regra_eletividade": "dt_inicio_real - min(dt_agendamento) >= 24h por hub+nr_cirurgia",
        "sem_conversao_de_fuso": True,
        "gravacao_em_tabelas": False,
        "auditoria_eletividade": audit,
    }
    saved = {
        "sem_agenda": str(_save_variant(root, "sem_agenda", run_id, without_agenda, manifest)),
        "com_agenda": str(_save_variant(root, "com_agenda", run_id, with_agenda, manifest)),
    }
    print(json.dumps(saved, ensure_ascii=False, indent=2))
    return saved


# COMMAND ----------

paths = main()
