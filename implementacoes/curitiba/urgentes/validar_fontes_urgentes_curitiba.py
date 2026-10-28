# Databricks notebook source
# MAGIC %md
# MAGIC # Validação das fontes — cirurgias urgentes e emergentes, Curitiba
# MAGIC
# MAGIC Execute célula por célula. Este notebook lê as fontes e os CSVs do seu Workspace pessoal; não cria tabelas, arquivos ou logs persistentes.
# MAGIC O CSV padrão é o backtest experimental de PS associado ao relatório com Hospital total. A saída mostra se há dados suficientes para adaptar uma **cópia** do notebook `06. Previsão Cirurgias Urgentes e Emergentes.ipynb`.

# COMMAND ----------

from pathlib import Path
from datetime import timedelta
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

HUB = "Curitiba"
ESTABELECIMENTO = "1"
USUARIO_ESPERADO = "deivison.jordao-dhauz@hospitalcare.com.br"
TABELA_CIRURGIAS = "bi_dev.gold.hc_cirurgia"
TABELA_AGENDA = "bi_dev.gold.hc_agenda_cirurgica"
TABELA_PS = "bi_dev.gold.hc_atend_senha_medico_ps_v2"
TABELA_CALENDARIO = "planejamento_integrado.gold.calendario_curitiba"
RAIZ_PESSOAL = Path("/Workspace/Users") / USUARIO_ESPERADO
BACKTEST_PADRAO = RAIZ_PESSOAL / "previsao_ps_curitiba/artifacts/4_backtests/comparacao_previsao_realizado_20260925_131522.csv"

dbutils.widgets.text("csv_backtest_ps", str(BACKTEST_PADRAO), "CSV do backtest de PS")
dbutils.widgets.text("csv_previsao_ps_futura", "", "CSV futuro de PS (opcional)")
usuario_atual = spark.sql("SELECT current_user() AS usuario").first()["usuario"]
hoje = spark.sql("SELECT current_date() AS hoje").first()["hoje"]
assert usuario_atual.lower() == USUARIO_ESPERADO.lower(), f"Workspace pessoal inesperado: {usuario_atual}"

def arquivo_pessoal(valor):
    caminho = Path(valor).resolve()
    if caminho != RAIZ_PESSOAL.resolve() and RAIZ_PESSOAL.resolve() not in caminho.parents:
        raise ValueError(f"Arquivo fora do Workspace pessoal: {caminho}")
    return caminho

print("Usuário:", usuario_atual)
print("Fuso da sessão Spark:", spark.conf.get("spark.sql.session.timeZone"))
print("CSV do backtest:", dbutils.widgets.get("csv_backtest_ps"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Esquemas e colunas obrigatórias
# MAGIC A cirurgia usa a mesma população e a regra inversa de 24 horas da implementação de eletivas de Curitiba. `ds_carater` é exibido apenas para auditoria. A fonte PS é a V2 usada no backtest experimental do Pilar.

# COMMAND ----------

fontes = {
    TABELA_CIRURGIAS: {"hub", "cd_estabelecimento", "dt_inicio_real", "ds_status_cirurgia", "ds_carater", "nr_cirurgia", "ds_procedimento"},
    TABELA_AGENDA: {"hub", "nr_cirurgia", "dt_agendamento", "ds_estabelecimento", "ds_procedimento"},
    TABELA_PS: {"hub", "ds_estabelecimento", "dt_entrada", "dt_cancelamento", "ds_setor_atendimento", "cid", "cid_titulo"},
    TABELA_CALENDARIO: {"data", "feriado", "ferias_escolares"},
}
quadros = {}
for tabela, obrigatorias in fontes.items():
    quadro = spark.table(tabela)
    quadros[tabela] = quadro
    faltantes = sorted(obrigatorias - set(quadro.columns))
    print(f"\n{tabela}; faltantes: {faltantes or 'nenhuma'}")
    display(spark.createDataFrame(
        [(c.name, c.dataType.simpleString(), c.nullable) for c in quadro.schema.fields],
        ["coluna", "tipo", "aceita_nulo"],
    ))
    if faltantes:
        raise ValueError(f"Colunas obrigatórias ausentes em {tabela}: {faltantes}")
for tabela, coluna in ((TABELA_CIRURGIAS, "dt_inicio_real"), (TABELA_AGENDA, "dt_agendamento")):
    tipo = quadros[tabela].schema[coluna].dataType.simpleString()
    if tipo not in ("timestamp", "timestamp_ntz"):
        raise TypeError(f"{tabela}.{coluna} precisa preservar data e hora; tipo encontrado: {tipo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. População de cirurgias do Pilar
# MAGIC Confira volumes por estabelecimento, caráter e status. Como na implementação de eletivas, excluímos as linhas marcadas como procedimento robótico antes da deduplicação por `hub + nr_cirurgia`. A classificação usa o primeiro horário de criação da agenda do mesmo hospital. Ausência de agenda ou criação sem horário válido pertence ao complemento não eletivo, mas aparece em auditoria própria.

# COMMAND ----------

cirurgias_hub = quadros[TABELA_CIRURGIAS].where(F.col("hub") == HUB)
display(cirurgias_hub.groupBy("cd_estabelecimento").agg(
    F.count("*").alias("registros"),
    F.min(F.to_date("dt_inicio_real")).alias("primeira_data"),
    F.max(F.to_date("dt_inicio_real")).alias("ultima_data"),
).orderBy(F.desc("registros")))

cirurgias_pilar = cirurgias_hub.where(F.trim(F.col("cd_estabelecimento").cast("string")) == ESTABELECIMENTO)
display(cirurgias_pilar.groupBy("ds_status_cirurgia", "ds_carater").count().orderBy(F.desc("count")))

cirurgias_base = (cirurgias_pilar
    .where(~F.lower(F.col("ds_procedimento")).like("%robótica%"))
    .where(F.col("ds_status_cirurgia") == "Realizada")
    .where(F.col("dt_inicio_real").isNotNull())
    .where(F.col("nr_cirurgia").isNotNull())
    .where(F.col("dt_inicio_real") < F.current_date())
    .withColumn("nr_cirurgia", F.col("nr_cirurgia").cast("string")))
print("Linhas de cirurgias realizadas antes da deduplicação:", cirurgias_base.count())
ordem_cirurgia = Window.partitionBy("hub", "nr_cirurgia").orderBy(F.col("dt_inicio_real").asc(), F.col("ds_procedimento").asc())
cirurgias_unicas = cirurgias_base.withColumn("_ordem", F.row_number().over(ordem_cirurgia)).where(F.col("_ordem") == 1).drop("_ordem")

agenda_pilar = (quadros[TABELA_AGENDA]
    .where(F.col("hub") == HUB)
    .where(F.lower(F.trim(F.col("ds_estabelecimento"))) == "hospital nossa senhora do pilar s.a.")
    .where(~F.lower(F.col("ds_procedimento")).like("%robótica%"))
    .where(F.col("nr_cirurgia").isNotNull())
    .withColumn("nr_cirurgia", F.col("nr_cirurgia").cast("string")))
primeira_criacao = agenda_pilar.groupBy("hub", "nr_cirurgia").agg(F.min("dt_agendamento").alias("first_created"))
cirurgias_classificadas = cirurgias_unicas.join(primeira_criacao, ["hub", "nr_cirurgia"], "left")
cirurgias_classificadas = cirurgias_classificadas.withColumn(
    "eletiva_24h",
    F.when(F.col("first_created").isNull(), F.lit(False))
    .otherwise(F.expr("dt_inicio_real >= first_created + INTERVAL 24 HOURS")),
).withColumn("nao_eletiva_24h", ~F.col("eletiva_24h"))
cirurgias_alvo = cirurgias_classificadas.where(F.col("nao_eletiva_24h"))

resumo_cirurgias = cirurgias_alvo.agg(
    F.count("*").alias("cirurgias_nao_eletivas"),
    F.min(F.to_date("dt_inicio_real")).alias("inicio"),
    F.max(F.to_date("dt_inicio_real")).alias("fim"),
).first().asDict()
print("Alvo cirúrgico:", resumo_cirurgias)
auditoria = cirurgias_classificadas.agg(
    F.count("*").alias("populacao_realizada_unica"),
    F.sum(F.col("eletiva_24h").cast("int")).alias("eletivas_24h"),
    F.sum(F.col("nao_eletiva_24h").cast("int")).alias("nao_eletivas_24h"),
    F.sum(F.col("first_created").isNull().cast("int")).alias("sem_criacao_valida"),
    F.sum((F.col("first_created").isNotNull() & F.expr("dt_inicio_real < first_created + INTERVAL 24 HOURS")).cast("int")).alias("criadas_menos_de_24h_antes"),
    F.sum((F.col("first_created").isNotNull() & F.expr("dt_inicio_real < first_created")).cast("int")).alias("criacao_apos_inicio_real"),
    F.sum((F.col("first_created").isNotNull() & F.expr("dt_inicio_real = first_created + INTERVAL 24 HOURS")).cast("int")).alias("exatamente_24h_eletivas"),
).first().asDict()
print("Partição por regra de 24 horas:", auditoria)
if auditoria["populacao_realizada_unica"] == 0:
    raise RuntimeError("Nenhuma cirurgia realizada na população compartilhada de Curitiba/Pilar")
assert auditoria["populacao_realizada_unica"] == auditoria["eletivas_24h"] + auditoria["nao_eletivas_24h"]
assert auditoria["nao_eletivas_24h"] == auditoria["sem_criacao_valida"] + auditoria["criadas_menos_de_24h_antes"]
particao_diaria = cirurgias_classificadas.groupBy(F.to_date("dt_inicio_real").alias("data_cirurgia")).agg(
    F.count("*").alias("total"),
    F.sum(F.col("eletiva_24h").cast("int")).alias("eletivas"),
    F.sum(F.col("nao_eletiva_24h").cast("int")).alias("nao_eletivas"),
)
print("Dias com falha na partição:", particao_diaria.where(F.col("total") != F.col("eletivas") + F.col("nao_eletivas")).count())
display(particao_diaria.orderBy(F.desc("data_cirurgia")).limit(30))
display(cirurgias_classificadas.groupBy("eletiva_24h", "ds_carater").count().orderBy("eletiva_24h", F.desc("count")))
display(cirurgias_alvo.groupBy(F.to_date("dt_inicio_real").alias("data_cirurgia")).count().orderBy(F.desc("data_cirurgia")).limit(30))
print("Fonte chega a D-1:", resumo_cirurgias["fim"] == hoje - timedelta(days=1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. PS do Hospital Pilar e calendário
# MAGIC O filtro de estabelecimento reproduz `classify_curitiba_ps` do experimento: nome contendo `hospital pilar`, sem depender do setor físico. A previsão de PS e o realizado precisam compartilhar o mesmo recorte.

# COMMAND ----------

ps_hub = quadros[TABELA_PS].where(F.col("hub") == HUB)
display(ps_hub.groupBy("ds_estabelecimento").count().orderBy(F.desc("count")).limit(30))
ps_pilar = ps_hub.where(F.lower(F.trim(F.coalesce(F.col("ds_estabelecimento"), F.lit("")))).like("%hospital pilar%"))
resumo_ps = ps_pilar.agg(
    F.count("*").alias("atendimentos"),
    F.min(F.to_date("dt_entrada")).alias("inicio"),
    F.max(F.to_date("dt_entrada")).alias("fim"),
).first().asDict()
print("PS Pilar:", resumo_ps)
display(ps_pilar.groupBy(F.to_date("dt_entrada").alias("data_ps"))
    .count().orderBy(F.desc("data_ps")).limit(30))

calendario = quadros[TABELA_CALENDARIO]
if "cidade" in calendario.columns:
    calendario = calendario.where(F.col("cidade") == HUB)
resumo_cal = calendario.agg(
    F.count("*").alias("linhas"),
    F.countDistinct(F.to_date("data")).alias("dias_distintos"),
    F.min(F.to_date("data")).alias("inicio"),
    F.max(F.to_date("data")).alias("fim"),
).first().asDict()
print("Calendário Curitiba:", resumo_cal)
print("Calendário sem datas duplicadas:", resumo_cal["linhas"] == resumo_cal["dias_distintos"])
print("Calendário cobre próximos 365 dias:", resumo_cal["fim"] >= hoje + timedelta(days=364))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3A. Agrupamento diário do PS histórico
# MAGIC Esta checagem reproduz as regras clínicas do experimento de PS e soma Digestivo e Lesões em Geral. Ginecológica/Obstétrica já pertence a Geral na origem. Compare as seis séries resultantes com o total de atendimentos elegíveis.

# COMMAND ----------

ps_elegivel = ps_pilar.where(F.col("dt_cancelamento").isNull() & F.col("dt_entrada").isNotNull() & (F.col("dt_entrada") < F.current_date()))
setor = F.coalesce(F.col("ds_setor_atendimento"), F.lit(""))
categoria_8 = (F.when(F.col("cid") == "A90", "Doencas_Infecciosas_Parasitarias")
    .when(F.col("cid").isin("R509", "R05") | (F.col("cid_titulo") == "Doenças do aparelho respiratório."), "Respiratorio_Tosse_Febre")
    .when(F.col("cid_titulo") == "Algumas doenças infecciosas e parasitárias.", "Doencas_Infecciosas_Parasitarias")
    .when(F.col("cid_titulo") == "Doenças do aparelho digestivo.", "Digestivo")
    .when(F.col("cid_titulo") == "Lesões, envenenamentos e algumas outras consequências de causas externas.", "Lesoes_Causas_Externas")
    .when(F.col("cid_titulo") == "Doenças do aparelho geniturinário.", "Geniturinario")
    .when((F.col("cid_titulo") == "Doenças do aparelho circulatório.") | F.col("cid").isin("R074", "R073", "R002", "R55", "R000", "R001", "R071", "R072"), "Cardiologia")
    .when(setor.like("%Orto%"), "Ortopedia")
    .otherwise("Geral"))
ps_classificado = ps_elegivel.withColumn("categoria_8", categoria_8)
ps_classificado = ps_classificado.withColumn("categoria_6", F.when(F.col("categoria_8").isin("Digestivo", "Lesoes_Causas_Externas"), "Geral").otherwise(F.col("categoria_8")))
print("Atendimentos elegíveis:", ps_elegivel.count())
display(ps_classificado.groupBy("categoria_8", "categoria_6").count().orderBy(F.desc("count")))
display(ps_classificado.groupBy(F.to_date("dt_entrada").alias("data_ps"), "categoria_6")
    .count().orderBy(F.desc("data_ps"), "categoria_6").limit(180))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Contrato do backtest de PS usado no relatório
# MAGIC O relatório de oito DFUs e Hospital total foi gerado a partir do experimento de 25/09/2026. `Hospital total` é uma soma derivada, não uma nona covariável. O arquivo contém previsões retrospectivas em cinco horizontes e não substitui uma previsão futura contínua.

# COMMAND ----------

caminho_backtest = arquivo_pessoal(dbutils.widgets.get("csv_backtest_ps"))
if not caminho_backtest.is_file():
    raise FileNotFoundError(f"CSV de backtest não encontrado: {caminho_backtest}")
backtest = pd.read_csv(caminho_backtest)
obrigatorias_backtest = {"data_evento", "distancia_previsao", "dfu", "previsao", "realizado"}
faltantes = sorted(obrigatorias_backtest - set(backtest.columns))
if faltantes:
    raise ValueError(f"Colunas ausentes no CSV de backtest: {faltantes}")
backtest["data_evento"] = pd.to_datetime(backtest["data_evento"], errors="raise").dt.normalize()
resumo_dfu = backtest.groupby("dfu", dropna=False).agg(
    linhas=("previsao", "size"),
    dias=("data_evento", "nunique"),
    primeira_data=("data_evento", "min"),
    ultima_data=("data_evento", "max"),
).reset_index()
display(resumo_dfu)
print("Horizontes:", sorted(backtest["distancia_previsao"].dropna().unique().tolist()))
print("Duplicatas por data, horizonte e DFU:", int(backtest.duplicated(["data_evento", "distancia_previsao", "dfu"]).sum()))
print("Linhas:", len(backtest), "| datas:", backtest["data_evento"].nunique(), "| DFUs:", backtest["dfu"].nunique())

dfus_do_relatorio = set(backtest["dfu"].dropna())
dfus_esperadas = {"Cardiologia", "Digestivo", "Doencas_Infecciosas_Parasitarias", "Geniturinario", "Geral", "Lesoes_Causas_Externas", "Ortopedia", "Respiratorio_Tosse_Febre"}
print("O CSV corresponde às oito DFUs experimentais:", dfus_do_relatorio == dfus_esperadas)
print("Ginecológica/Obstétrica é série própria:", "Ginecologica_Obstetrica" in dfus_do_relatorio)
print("Última data prevista no backtest:", backtest["data_evento"].max().date())
backtest["categoria_6"] = backtest["dfu"].replace({"Digestivo": "Geral", "Lesoes_Causas_Externas": "Geral"})
backtest_6 = backtest.groupby(["data_evento", "distancia_previsao", "categoria_6"], as_index=False)[["previsao", "realizado"]].sum()
print("Séries após agregação:", sorted(backtest_6["categoria_6"].unique().tolist()))
print("Soma da previsão preservada:", abs(backtest_6["previsao"].sum() - backtest["previsao"].sum()) < 1e-6)
display(backtest_6.groupby("categoria_6").agg(dias=("data_evento", "nunique"), linhas=("previsao", "size")).reset_index())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pré-validação da covariável futura de PS
# MAGIC Informe no widget um CSV **de previsão futura** produzido com a mesma configuração do backtest. A cópia do notebook de cirurgia precisa de uma linha por data e por grupo para 365 dias a partir do corte. Conforme a decisão do projeto, Ginecológica/Obstétrica permanece dentro de Geral, pois o backtest não a prevê como série própria.

# COMMAND ----------

valor_futuro = dbutils.widgets.get("csv_previsao_ps_futura").strip()
if not valor_futuro:
    print("PENDENTE: informe o CSV futuro de PS no widget e execute esta célula novamente.")
else:
    caminho_futuro = arquivo_pessoal(valor_futuro)
    if not caminho_futuro.is_file():
        raise FileNotFoundError(f"CSV futuro não encontrado: {caminho_futuro}")
    futuro = pd.read_csv(caminho_futuro)
    print("Colunas:", futuro.columns.tolist())
    coluna_data = "data_evento" if "data_evento" in futuro.columns else "dt_referencia"
    coluna_valor = "previsao" if "previsao" in futuro.columns else "qtd_pred"
    faltantes = [c for c in (coluna_data, "dfu", coluna_valor) if c not in futuro.columns]
    if faltantes:
        raise ValueError(f"CSV futuro incompatível; faltam: {faltantes}")
    futuro[coluna_data] = pd.to_datetime(futuro[coluna_data], errors="raise").dt.normalize()
    inicio = pd.Timestamp(hoje)
    fim = inicio + pd.Timedelta(days=364)
    janela = futuro.loc[futuro[coluna_data].between(inicio, fim)].copy()
    print("Janela requerida:", inicio.date(), "a", fim.date())
    print("Datas distintas:", janela[coluna_data].nunique(), "de 365")
    print("Duplicatas por data e DFU:", int(janela.duplicated([coluna_data, "dfu"]).sum()))
    print("DFUs presentes:", sorted(janela["dfu"].dropna().unique().tolist()))
    print("Oito DFUs experimentais completas:", set(janela["dfu"].dropna()) == dfus_esperadas and len(janela) == 365 * 8)
    print("Valores negativos/nulos:", int((pd.to_numeric(janela[coluna_valor], errors="coerce") < 0).sum()), int(janela[coluna_valor].isna().sum()))
    display(janela.groupby("dfu").agg(dias=(coluna_data, "nunique"), inicio=(coluna_data, "min"), fim=(coluna_data, "max")).reset_index())
    janela["categoria_6"] = janela["dfu"].replace({"Digestivo": "Geral", "Lesoes_Causas_Externas": "Geral"})
    janela_6 = janela.groupby([coluna_data, "categoria_6"], as_index=False)[coluna_valor].sum()
    print("Seis séries por dia após agregação:", len(janela_6) == 365 * 6 and janela_6["categoria_6"].nunique() == 6)
    print("Soma preservada:", abs(janela_6[coluna_valor].sum() - janela[coluna_valor].sum()) < 1e-6)
    print("Cobertura diária completa:", janela[coluna_data].nunique() == 365)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretação
# MAGIC A adaptação só deve ser executada depois de confirmar: recorte do Pilar, partição eletiva/não eletiva pela regra de 24 horas, série cirúrgica até D-1, calendário sem duplicatas e covariável futura completa. `ds_carater` é apenas uma comparação diagnóstica. O backtest de PS de 180 dias serve como referência de **modelo/taxonomia** e auditoria; ele não cobre os 365 dias futuros. `Digestivo` e `Lesoes_Causas_Externas` serão somados a `Geral`. Ginecológica/Obstétrica permanece em `Geral`, conforme a decisão de manter o modelo do backtest; portanto, serão seis covariáveis clínicas distintas, além das variáveis de calendário.
