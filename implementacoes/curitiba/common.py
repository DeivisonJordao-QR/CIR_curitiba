"""Fontes e contrato compartilhados pelos dois experimentos de Curitiba.

Este módulo lê tabelas existentes e não grava em Unity Catalog. As únicas gravações
permitidas pelos runners são arquivos sob /Workspace/Users/<usuário>/.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


HUB = "Curitiba"
ESTABLISHMENT_CODE = "1"
ESTABLISHMENT_NAME = "Hospital Nossa Senhora do Pilar S.A."
START_DATE = "2023-01-01"
SHORT_HORIZON = 50
LONG_HORIZON = 365
SURGERIES_TABLE = "bi_dev.gold.hc_cirurgia"
AGENDA_TABLE = "bi_dev.gold.hc_agenda_cirurgica"
PEOPLE_TABLE = "bi_dev.gold.hc_pessoa_fisica"
CALENDAR_TABLE = "planejamento_integrado.gold.calendario_curitiba"
DFUS = (
    "Robótica",
    "Infantil",
    "Ortopedia",
    "Ginecologia e Obstetrícia",
    "Cirurgia Geral",
    "Outros",
)


def personal_experiment_dir(spark: SparkSession) -> Path:
    """Resolve e valida o único destino persistente do experimento."""
    user = spark.sql("SELECT current_user() AS user").first()["user"]
    if not user or "/" in user or "\\" in user or user in (".", ".."):
        raise ValueError("Identidade Databricks inválida para um caminho pessoal")
    user_home = (Path("/Workspace/Users") / user).resolve()
    root = (user_home / "experimentos" / "previsao_eletivas_curitiba").resolve()
    if user_home not in root.parents or not user_home.is_dir():
        raise RuntimeError(f"Workspace pessoal indisponível: {user_home}")
    return root


def require_columns(frame: DataFrame, source: str, columns: set[str]) -> None:
    missing = sorted(columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{source}: colunas ausentes: {', '.join(missing)}")


def require_timestamp(frame: DataFrame, source: str, column: str) -> None:
    actual = frame.schema[column].dataType.simpleString()
    if actual not in ("timestamp", "timestamp_ntz"):
        raise TypeError(
            f"{source}.{column} deve conter data e hora; tipo encontrado: {actual}"
        )


def _ages(spark: SparkSession) -> DataFrame:
    people = spark.table(PEOPLE_TABLE).where(F.col("hub") == HUB)
    require_columns(people, PEOPLE_TABLE, {"hub", "cd_pessoa_fisica", "nr_anos"})
    # Uma linha por paciente impede multiplicação acidental do número de cirurgias.
    return people.groupBy("hub", "cd_pessoa_fisica").agg(F.max("nr_anos").alias("nr_anos"))


def _dfu(specialty_col: str):
    pediatric = [
        "Cirurgia Pediátrica", "Otorrinolaringologia Pediátrica", "Pediatria",
        "Medicina Intensiva Pediátrica", "Neonatologia", "Odontopediatria",
        "Cardiologia Pediatrica", "Endocrinologia Pediátrica",
    ]
    general = [
        "Urologia", "Cirurgia Geral", "Radiologia", "Cirurgia Oncológica",
        "Cancerologia", "Cancerologia Cirúrgica", "Mastologia",
    ]
    specialty = F.lower(
        F.regexp_replace(F.trim(F.col(specialty_col)), r"\s+", " ")
    )
    general_normalized = [name.casefold() for name in general]
    return (
        F.when(F.col("fl_robotica") == 1, "Robótica")
        .when(F.col("nr_anos") <= 14, "Infantil")
        .when(specialty.isin([name.casefold() for name in pediatric]), "Infantil")
        .when(
            specialty.isin("ortopedia / traumatologia", "ortopedia e traumatologia"),
            "Ortopedia",
        )
        .when(
            specialty.isin("ginecologia", "ginecologia e obstetrícia"),
            "Ginecologia e Obstetrícia",
        )
        .when(specialty.isin(general_normalized), "Cirurgia Geral")
        .otherwise("Outros")
    )


def _source_surgeries(spark: SparkSession, ages: DataFrame) -> DataFrame:
    base = (
        spark.table(SURGERIES_TABLE)
        .where(F.col("hub") == HUB)
        .where(F.trim(F.col("cd_estabelecimento").cast("string")) == ESTABLISHMENT_CODE)
    )
    require_columns(base, SURGERIES_TABLE, {
        "hub", "nr_cirurgia", "nr_atendimento", "cd_medico_cirurgiao",
        "cd_pessoa_fisica", "dt_inicio_real", "ds_sala", "ds_procedimento",
        "ds_status_cirurgia", "ds_especialidade_cirurgiao", "cd_estabelecimento",
    })
    require_timestamp(base, SURGERIES_TABLE, "dt_inicio_real")
    robotic_keys = [
        "hub", "nr_atendimento", "cd_medico_cirurgiao", "cd_pessoa_fisica",
        "data_cirurgia", "ds_sala",
    ]
    robot = (
        base.where(F.lower("ds_procedimento").like("%robótica%"))
        .withColumn("data_cirurgia", F.to_date("dt_inicio_real"))
        .select(*robotic_keys).distinct()
        .withColumn("fl_robotica", F.lit(1))
    )
    rows = (
        base.where(~F.lower("ds_procedimento").like("%robótica%"))
        .where(F.col("ds_status_cirurgia") == "Realizada")
        .where(F.col("dt_inicio_real").isNotNull())
        .where(F.col("nr_cirurgia").isNotNull())
        .withColumn("nr_cirurgia", F.col("nr_cirurgia").cast("string"))
        .withColumn("data_cirurgia", F.to_date("dt_inicio_real"))
        .join(robot, robotic_keys, "left")
        .fillna(0, subset=["fl_robotica"])
        .join(ages, ["hub", "cd_pessoa_fisica"], "left")
        .withColumn("dfu", _dfu("ds_especialidade_cirurgiao"))
    )
    # nr_cirurgia é a unidade de contagem, inclusive quando a fonte tem várias linhas.
    w = Window.partitionBy("hub", "nr_cirurgia").orderBy(
        F.col("dt_inicio_real").asc(), F.col("dfu").asc()
    )
    return rows.withColumn("_rank", F.row_number().over(w)).where("_rank = 1").drop("_rank")


def _source_agenda(spark: SparkSession, ages: DataFrame) -> DataFrame:
    base = (
        spark.table(AGENDA_TABLE)
        .where(F.col("hub") == HUB)
        .where(
            F.lower(F.trim(F.col("ds_estabelecimento")))
            == ESTABLISHMENT_NAME.casefold()
        )
    )
    require_columns(base, AGENDA_TABLE, {
        "hub", "nr_cirurgia", "cd_medico", "cd_pessoa_fisica", "dt_agenda",
        "dt_agendamento", "ds_agenda", "ds_procedimento", "ds_especialidade",
        "dt_cancelamento", "ds_motivo_cancelamento", "cd_motivo_cancelamento",
        "ds_status_agenda", "ds_estabelecimento",
    })
    require_timestamp(base, AGENDA_TABLE, "dt_agenda")
    require_timestamp(base, AGENDA_TABLE, "dt_agendamento")
    robotic_keys = ["hub", "cd_medico", "cd_pessoa_fisica", "data_agenda", "ds_agenda"]
    robot = (
        base.where(F.lower("ds_procedimento").like("%robótica%"))
        .withColumn("data_agenda", F.to_date("dt_agenda"))
        .select(*robotic_keys).distinct()
        .withColumn("fl_robotica", F.lit(1))
    )
    rows = (
        base.where(~F.lower("ds_procedimento").like("%robótica%"))
        .where(F.col("nr_cirurgia").isNotNull())
        .withColumn("nr_cirurgia", F.col("nr_cirurgia").cast("string"))
        .withColumn("data_agenda", F.to_date("dt_agenda"))
        .join(robot, robotic_keys, "left")
        .fillna(0, subset=["fl_robotica"])
        .join(ages, ["hub", "cd_pessoa_fisica"], "left")
        .withColumn("dfu", _dfu("ds_especialidade"))
    )
    return rows


def _first_creation(agenda: DataFrame) -> DataFrame:
    return agenda.groupBy("hub", "nr_cirurgia").agg(
        F.min("dt_agendamento").alias("first_created")
    )


def _eligible_surgeries(surgeries: DataFrame, first: DataFrame) -> DataFrame:
    return (
        surgeries.join(first, ["hub", "nr_cirurgia"], "inner")
        .where(F.col("first_created").isNotNull())
        .where(F.expr("dt_inicio_real >= first_created + INTERVAL 24 HOURS"))
    )


def _eligibility_audit(surgeries: DataFrame, agenda: DataFrame, first: DataFrame) -> dict:
    linked = surgeries.join(first, ["hub", "nr_cirurgia"], "left")
    flags = linked.agg(
        F.count("*").alias("cirurgias_realizadas_com_chave"),
        F.sum(F.when(F.col("first_created").isNull(), 1).otherwise(0)).alias("sem_criacao"),
        F.sum(
            F.when(
                F.col("first_created").isNotNull()
                & F.expr("dt_inicio_real < first_created + INTERVAL 24 HOURS"),
                1,
            ).otherwise(0)
        ).alias("criadas_menos_de_24h_antes"),
        F.sum(
            F.when(
                F.col("first_created").isNotNull()
                & F.expr("dt_inicio_real >= first_created + INTERVAL 24 HOURS"),
                1,
            ).otherwise(0)
        ).alias("eletivas_elegiveis"),
    ).first().asDict()
    appointments_per_surgery = agenda.groupBy("hub", "nr_cirurgia").agg(
        F.count("*").alias("n")
    )
    flags["cirurgias_com_multiplos_agendamentos"] = (
        surgeries.join(appointments_per_surgery, ["hub", "nr_cirurgia"], "inner")
        .where(F.col("n") > 1).count()
    )
    tied = agenda.groupBy("hub", "nr_cirurgia", "dt_agendamento").count().where(
        F.col("count") > 1
    ).select("hub", "nr_cirurgia").distinct()
    flags["cirurgias_com_empate_na_criacao"] = surgeries.join(
        tied, ["hub", "nr_cirurgia"], "inner"
    ).count()
    return {key: int(value or 0) for key, value in flags.items()}


def _calendar(spark: SparkSession) -> DataFrame:
    calendar = spark.table(CALENDAR_TABLE)
    require_columns(calendar, CALENDAR_TABLE, {"data", "feriado"})
    if not calendar.limit(1).count():
        raise ValueError(f"Calend?rio vazio: {CALENDAR_TABLE}")
    return (
        calendar.groupBy(F.col("data").cast("date").alias("ds"))
        .agg(F.max(F.when(F.col("feriado").isNotNull(), 1).otherwise(0)).alias("feriado"))
    )

def _add_calendar(frame: DataFrame, calendar: DataFrame) -> DataFrame:
    return (
        frame.join(calendar, "ds", "left")
        .fillna(0, subset=["feriado"])
        .withColumn(
            "periodo_baixa",
            F.when((F.month("ds") == 12) & (F.dayofmonth("ds") >= 22), 1)
            .when((F.month("ds") == 1) & (F.dayofmonth("ds") <= 5), 1)
            .otherwise(0),
        )
    )


def _daily_grid(spark: SparkSession, start, end) -> DataFrame:
    dates = spark.range(1).select(
        F.explode(F.sequence(F.lit(start).cast("date"), F.lit(end).cast("date"))).alias("ds")
    )
    dfus = spark.createDataFrame([(item,) for item in DFUS], ["dfu"])
    return dates.crossJoin(dfus)


def _daily_history(spark: SparkSession, eligible: DataFrame, calendar: DataFrame, as_of) -> DataFrame:
    totals = (
        eligible.where(F.col("data_cirurgia") < F.lit(as_of).cast("date"))
        .groupBy(F.col("data_cirurgia").alias("ds"), "dfu")
        .agg(F.countDistinct("nr_cirurgia").alias("y"))
    )
    grid = _daily_grid(spark, START_DATE, as_of - timedelta(days=1))
    return _add_calendar(
        grid.join(totals, ["ds", "dfu"], "left").fillna(0, subset=["y"]), calendar
    )


def _agenda_rows(agenda: DataFrame, first: DataFrame) -> DataFrame:
    timestamp_type = agenda.schema["dt_agendamento"].dataType.simpleString()
    cancellation_text = F.to_date(
        F.regexp_extract("ds_motivo_cancelamento", r"em (\d{2}/\d{2}/\d{4})", 1),
        "dd/MM/yyyy",
    ).cast(timestamp_type)
    rows = (
        agenda.where(~F.col("ds_agenda").like("%VIRTUAL%"))
        .where(~F.col("ds_agenda").isin("Ressonância", "Tomografia"))
        .where(F.col("dt_agenda").isNotNull())
        .join(first, ["hub", "nr_cirurgia"], "inner")
        .where(F.col("first_created").isNotNull())
        .where(F.expr("dt_agenda >= first_created + INTERVAL 24 HOURS"))
        .withColumn(
            "cancelled_at",
            F.when(
                F.col("cd_motivo_cancelamento").isNotNull(),
                F.coalesce(F.col("dt_cancelamento").cast(timestamp_type), cancellation_text),
            ),
        )
    )
    return rows


def _historical_agenda_panel(
    spark: SparkSession, rows: DataFrame, daily: DataFrame
) -> DataFrame:
    leads = spark.range(1, SHORT_HORIZON + 1).withColumnRenamed("id", "lead")
    grid = daily.select("ds", "dfu").crossJoin(leads)
    agenda = rows.select(
        "nr_cirurgia", "dfu", "dt_agendamento", "cancelled_at",
        F.to_date("dt_agenda").alias("ds"),
    )
    joined = grid.join(agenda, ["ds", "dfu"], "left")
    # Lead 1 é o dia da cirurgia; lead 2 é o dia anterior, sempre às 00:00.
    timestamp_type = rows.schema["dt_agendamento"].dataType.simpleString()
    cutoff = F.expr(
        f"CAST(date_sub(ds, CAST(lead AS INT) - 1) AS {timestamp_type})"
    )
    active = (
        (F.col("dt_agendamento") <= cutoff)
        & (F.col("cancelled_at").isNull() | (F.col("cancelled_at") > cutoff))
    )
    counts = joined.groupBy("ds", "dfu", "lead").agg(
        F.countDistinct(F.when(active, F.col("nr_cirurgia"))).alias("agendamentos")
    )
    return daily.join(counts, ["ds", "dfu"], "inner")


def _future_agenda(rows: DataFrame, future_grid: DataFrame) -> DataFrame:
    current = (
        rows.where(F.col("dt_cancelamento").isNull())
        .where(F.col("ds_status_agenda") != "Cancelada")
        .groupBy(F.to_date("dt_agenda").alias("ds"), "dfu")
        .agg(F.countDistinct("nr_cirurgia").alias("agendamentos"))
    )
    return future_grid.join(current, ["ds", "dfu"], "left").fillna(0, subset=["agendamentos"])


def load_inputs(spark: SparkSession):
    """Entrega ambas as variantes a partir do mesmo conjunto lido na execução."""
    as_of = spark.sql("SELECT current_date() AS ds").first()["ds"]
    ages = _ages(spark)
    surgeries = _source_surgeries(spark, ages)
    agenda = _source_agenda(spark, ages)
    timestamp_types = {
        surgeries.schema["dt_inicio_real"].dataType.simpleString(),
        agenda.schema["dt_agenda"].dataType.simpleString(),
        agenda.schema["dt_agendamento"].dataType.simpleString(),
    }
    if len(timestamp_types) != 1:
        raise TypeError(
            "Os três horários usam tipos Spark diferentes; não haverá conversão implícita de fuso"
        )
    first = _first_creation(agenda)
    audit = _eligibility_audit(surgeries, agenda, first)
    eligible = _eligible_surgeries(surgeries, first)
    calendar = _calendar(spark)
    daily = _daily_history(spark, eligible, calendar, as_of)
    future_grid = _add_calendar(
        _daily_grid(spark, as_of, as_of + timedelta(days=LONG_HORIZON - 1)),
        calendar,
    )
    agenda_rows = _agenda_rows(agenda, first)
    historical_panel = _historical_agenda_panel(spark, agenda_rows, daily)
    future_with_agenda = _future_agenda(agenda_rows, future_grid)
    return as_of, daily, historical_panel, future_grid, future_with_agenda, audit


def load_backtest_inputs(spark: SparkSession):
    """Carrega o painel histórico e as linhas de agenda para cortes temporais."""
    as_of = spark.sql("SELECT current_date() AS ds").first()["ds"]
    ages = _ages(spark)
    surgeries = _source_surgeries(spark, ages)
    agenda = _source_agenda(spark, ages)
    timestamp_types = {
        surgeries.schema["dt_inicio_real"].dataType.simpleString(),
        agenda.schema["dt_agenda"].dataType.simpleString(),
        agenda.schema["dt_agendamento"].dataType.simpleString(),
    }
    if len(timestamp_types) != 1:
        raise TypeError(
            "Os três horários usam tipos Spark diferentes; não haverá conversão implícita de fuso"
        )
    first = _first_creation(agenda)
    audit = _eligibility_audit(surgeries, agenda, first)
    eligible = _eligible_surgeries(surgeries, first)
    calendar = _calendar(spark)
    daily = _daily_history(spark, eligible, calendar, as_of)
    agenda_rows = _agenda_rows(agenda, first)
    return as_of, daily, agenda_rows, audit
