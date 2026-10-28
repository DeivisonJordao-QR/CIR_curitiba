# Experimento de eletivas - Curitiba

**Escopo vigente:** Curitiba, estabelecimento `cd_estabelecimento = 1` — Hospital Nossa Senhora do Pilar S.A. Cirurgias e agenda são filtradas para essa unidade antes da classificação e da modelagem.

## Conteudo

- `common.py`: filtros de hub e estabelecimento, DFUs, vínculo por `hub + nr_cirurgia`, regra de 24 horas e painéis diários.
- `com_agenda/implementation.py`: variante que usa agenda como sinal preditivo.
- `sem_agenda/implementation.py`: variante sem features nem pos-processamento de agenda.
- `forecast.py`: modelos LightGBM, lags, calendario e horizontes de 50 e 365 dias.
- `run_experimentos.py`: notebook Databricks que executa ambas as variantes e grava resultados na pasta pessoal.
- `backtest.py` e `run_backtest.py`: backtest temporal pareado e notebook Databricks de execução.
- `job_backtest_eletivas_curitiba.json`: configuração local do job de backtest, separado do forecast futuro.
- `validar_fontes_curitiba.ipynb`: notebook somente leitura para conferir schema, timestamps e cardinalidade.
- `job_eletivas_curitiba.json`: configuracao local do job sob demanda.

## Job e resultados

O job usa o cluster existente `HC_Cluster_PlanIntegrado` (`0505-184220-djyvd4t5`), pois a conta nao tem entitlement para serverless nem permissao para criar clusters. As dependencias MLForecast e LightGBM sao instaladas com versoes fixadas em escopo do notebook. O job nao tem agenda recorrente.

Os arquivos desta execucao estao em:

- `/Users/deivison.jordao-dhauz@hospitalcare.com.br/experimentos/previsao_eletivas_curitiba/sem_agenda/run_20261020_c283185592/`
- `/Users/deivison.jordao-dhauz@hospitalcare.com.br/experimentos/previsao_eletivas_curitiba/com_agenda/run_20261020_c283185592/`

Cada pasta contem `previsao_diaria.csv` e `manifest.json`. A previsao cobre todos os dias do horizonte e DFUs definidos; os checks do codigo impediram duplicatas, negativos e previsao com agenda abaixo da quantidade agendada.

As fontes Unity Catalog foram lidas. Nenhuma tabela foi escrita. Não há quebra horária neste escopo.

## Backtest rolling

- O escopo operacional é `hub = Curitiba` e estabelecimento `cd_estabelecimento = 1` (`Hospital Nossa Senhora do Pilar S.A.`). Cirurgias são filtradas pelo código; agendas, por `ds_estabelecimento`, usando o nome indicado. A tabela de pacientes é usada apenas para idade/DFU e não tem esse filtro de estabelecimento.
- `data_inicio_periodo` define a primeira data prevista; `dias_previsao` define quantos dias consecutivos entram na avaliação (1 a 365).
- Para cada data realizada do intervalo, são geradas previsões D1, D2, D7, D30 e D90. O corte de cada previsão é móvel e depende do horizonte: o modelo inicia em `data_prevista - (horizonte - 1)` e treina somente com observações anteriores a esse início.
- Assim, para uma mesma data realizada, D1, D2, D7, D30 e D90 usam cinco cortes de dados distintos. As duas variantes recebem os mesmos cortes e são comparadas com o realizado da mesma data e DFU.
- Métricas descritivas por horizonte, variante e DFU: MAE, RMSE, viés (previsão menos realizado) e WAPE. Não há limite de aprovação nem escolha automática da variante.
- A variante `com_agenda` reconstrói a agenda conhecida em cada corte usando os horários registrados, sem conversão de fuso. A fonte atual não é um snapshot versionado de `dt_agenda`, portanto a reconstrução histórica da agenda é aproximada.
- A classificação de Curitiba normaliza `Ortopedia / Traumatologia` junto de `Ortopedia e Traumatologia` para `Ortopedia`, e `Ginecologia` junto de `Ginecologia e Obstetrícia` para a DFU existente `Ginecologia e Obstetrícia`. `Cancerologia` e `Cirurgia Oncológica` convergem para o grupo atual `Cirurgia Geral`.
- Previsões rolling, métricas e manifesto são salvos como arquivos sob `experimentos/previsao_eletivas_curitiba/backtest/<run_id>/` no Workspace pessoal. Nenhuma tabela é escrita.
- Os logs do backtest registram leitura/preparação, início e fim do treino/inferência por variante, andamento por origem, média móvel e ETA. O detalhamento interno dos modelos aparece na primeira origem e depois a cada dez origens para manter o volume de logs legível.

### Execução validada

O job `backtest_eletivas_curitiba_experimento` (ID `1067775063187706`) recebe `data_inicio_periodo`, `dias_previsao` e `variantes` (`sem_agenda`, `com_agenda` ou ambas). Os runs rolling `340656153280799` e `167392647767059` foram cancelados antes da gravação de resultados; os runs antigos usam o desenho de corte único e não devem ser usados nesta comparação.

- Smoke test: run `937525752433021`, sucesso em 84 segundos; sete dias, variante `sem_agenda`, horizontes D1/D2/D7 e 126 previsões (7 × 6 DFUs × 3 horizontes).
- Backtest principal: run `197346402116066` (task `556693545965771`), sucesso em 9m53s; período previsto de 2026-01-01 a 2026-03-31, 90 dias, variante `sem_agenda` e 2.700 previsões (90 × 6 DFUs × 5 horizontes). Foram avaliadas 179 origens móveis; os logs registraram progresso e ETA.
- Artefatos: `/Users/deivison.jordao-dhauz@hospitalcare.com.br/experimentos/previsao_eletivas_curitiba/backtest/backtest_rolling_20260101_90d_81b63d55/`, incluindo `previsoes_rolling.csv`, `metricas.csv`, `manifest.json` e `relatorio_backtest_eletivas_curitiba.html`.
- O HTML local com gráficos interativos está em `experimentos/backtest_eletivas_curitiba/grafico_comparativo.html`; o relatório formatado está em `experimentos/backtest_eletivas_curitiba/relatorio_backtest_eletivas_curitiba.html`.
- Nenhuma tabela foi escrita. A variante `com_agenda` não foi executada neste ciclo. Não há job em execução.
- Os resultados e o HTML do run `backtest_rolling_20251020_360d_11bf2911` foram produzidos antes do filtro por estabelecimento e agregam as unidades do hub Curitiba. Não representam exclusivamente o Hospital Nossa Senhora do Pilar e devem ser tratados como referência histórica até a execução de um novo backtest com este filtro.
