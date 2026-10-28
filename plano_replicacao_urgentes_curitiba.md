# Plano de replicação — cirurgias urgentes e emergentes, Hospital Pilar

**Estado (28/10/2026):** análise da referência concluída; notebook de validação e código do backtest de não eletivas preparados. O alvo segue o complemento da regra de eletivas de Curitiba. O backtest usa o intervalo de eventos do PS (27/06/2026–24/09/2026) e os horizontes D+1, D+2, D+7, D+30 e D+90. Nenhuma tabela foi alterada.

## Objetivo e método

Criar uma **cópia** de `referencia/06. Previsão Cirurgias Urgentes e Emergentes.ipynb` no Workspace pessoal `/Users/deivison.jordao-dhauz@hospitalcare.com.br/`, preservando ordem das células, modelos, backtest, horizonte de 365 dias, arredondamento e gráficos onde as alterações não forem necessárias. Registrar um diff por célula. A referência original permanece intacta.

As fontes governadas serão somente leitura. Código, diagnósticos, previsões e métricas serão arquivos exclusivamente sob o Workspace pessoal. Não usar `write_to_table`, `saveAsTable`, `MERGE`, Volumes, tabelas de log ou jobs compartilhados. A função `utils.start_pipeline_log/end_pipeline_log` precisa ser removida ou substituída por mensagens em memória, pois sua implementação pode persistir logs fora do espaço pessoal.

## Fontes e contrato

| Uso | Referência Campinas | Curitiba/Pilar a validar |
|---|---|---|
| Cirurgias realizadas | `campinas_hvc_dev.gold.cirurgia`; `hub = Campinas`; `cd_estabelecimento = 2` | `bi_dev.gold.hc_cirurgia`; `hub = Curitiba`; `cd_estabelecimento = 1` (Hospital Nossa Senhora do Pilar S.A.). Aplicar a mesma população deduplicada usada nas eletivas de Curitiba. |
| Primeira criação de agenda | Não usada para classificar urgências na referência | `bi_dev.gold.hc_agenda_cirurgica`; mesmo hub e Hospital Pilar; vincular por `hub + nr_cirurgia` e usar `min(dt_agendamento)`, como na implementação de eletivas. |
| Histórico de PS | `campinas_hvc_dev.gold.hc_atend_senha_medico_ps` e internação de `bi_dev.gold.hc_atendimento_paciente` | `bi_dev.gold.hc_atend_senha_medico_ps_v2`; hub Curitiba e nome contendo `hospital pilar`, como no experimento de PS. Conferir se a coluna `internacao` é realmente usada pelo modelo; ela é calculada na referência, mas a pivotagem usa apenas `qtd`. |
| Calendário | `{catalog}.gold.calendario` com `cidade = Campinas` | `planejamento_integrado.gold.calendario_curitiba`; verificar cobertura histórica/futura e duplicidade por data. |
| PS futuro | `{catalog}.ds_pipeline.previsao_demanda_ps` da data corrente | CSV novo de previsão futura de PS gerado no Workspace pessoal com o **mesmo modelo, parâmetros e taxonomia** do backtest experimental; não ler nem gravar a tabela de pipeline. |

O relatório `relatorio_backtest_ps_curitiba_dfu_experimentais_com_hospital_total.html#slide-4` deriva do CSV `comparacao_previsao_realizado_20260925_131522.csv`, disponível em `/Users/deivison.jordao-dhauz@hospitalcare.com.br/previsao_ps_curitiba/artifacts/4_backtests/`. A cópia local do CSV tem 8 DFUs, 180 datas de 29/03/2026 a 24/09/2026 e cinco horizontes (D+1, D+2, D+7, D+30 e D+90). `Hospital total` é a soma dessas DFUs no relatório. Esse CSV é evidência retrospectiva, **não** a covariável para os 365 dias futuros. O código do backtest instancia `ParallelDayForecaster(..., forecast_horizon=365)`, de modo que a inferência futura completa é tecnicamente prevista pelo contrato, mas ainda precisa ser produzida e verificada.

## Regra do alvo cirúrgico aprovada

Usar **exatamente a mesma população base** da implementação de eletivas em `implementacoes/curitiba/common.py`: cirurgia realizada do Pilar, `dt_inicio_real` válido, `nr_cirurgia` válido, exclusão das linhas de procedimento robótico conforme a rotina existente e uma cirurgia por `hub + nr_cirurgia`. Para a série histórica, considerar somente eventos anteriores à data de processamento. A rotina de eletivas obtém o menor `dt_agendamento` da agenda do mesmo hub e estabelecimento e classifica como eletiva quando `dt_inicio_real >= first_created + INTERVAL 24 HOURS`.

O alvo não eletivo é o **complemento nessa população base**: `first_created` ausente/nulo **ou** `dt_inicio_real < first_created + INTERVAL 24 HOURS`. A igualdade em 24 horas é eletiva. Cirurgias sem agenda, com agenda sem criação válida ou com criação posterior ao início real ficam no complemento e devem aparecer em auditorias separadas; o último caso é possível inconsistência temporal. `ds_carater` fica só para confronto e não determina a classificação. Conferir que `eletivas + não eletivas = população base` sem sobreposição, por dia e no total. Esta definição operacional de não eletiva não prova, por si só, que cada cirurgia tenha caráter clínico de urgência ou emergência.

## Backtest cirúrgico preparado

O código usa o Prophet da previsão final do notebook de referência. Em cada origem cirúrgica diária entre 27/06 e 24/09, treina com cirurgias não eletivas até D-1 e prevê até o fim da janela. Para as covariáveis futuras, lê em `previsoes_d01.csv` a `previsoes_d90.csv` o valor de cada DFU emitido naquela mesma origem (`data_processamento = origem`) para cada data prevista. Isso evita usar realizado futuro do PS. O histórico de PS vem de `prepared_data_20260925_125725`; Digestivo e Lesões/Causas Externas são somadas a Geral. O Prophet também recebe `ferias_escolares` e o calendário brasileiro interno, como na referência.

A janela fixa de eventos do PS tem 90 dias. Por isso, a cobertura cirúrgica varia por horizonte: D+1 = 90 casos, D+2 = 89, D+7 = 84, D+30 = 61 e D+90 = 1. D+90 valida o encadeamento dos dados, mas não sustenta uma conclusão de desempenho. Para 90 casos comparáveis em cada horizonte, será necessário um backtest de PS com uma janela de eventos maior (179 dias para 90 origens e 90 dias de covariáveis futuras em cada origem).

Arquivos preparados em `implementacoes/curitiba/urgentes/backtest_nao_eletivas.py` e `executar_backtest_nao_eletivas_curitiba.ipynb`. O notebook mostra progresso/tempo decorrido/ETA por origem na saída da célula e mantém `execucao.log` e um checkpoint CSV após cada origem em `/Users/deivison.jordao-dhauz@hospitalcare.com.br/experimentos/previsao_urgentes_curitiba/backtests/<run_id>/`. Ao final grava também `previsoes_horizontes.csv`, `metricas_por_horizonte.csv`, `auditoria_origens.csv` e `manifest.json`. Não escreve tabelas. A avaliação e as métricas ainda dependem da execução no compute Databricks.

## Decisão sobre os agrupamentos de PS

O usuário escolheu **manter o modelo do backtest**. O backtest experimental separa `Digestivo` e `Lesoes_Causas_Externas`, e incorpora Ginecológica/Obstétrica a `Geral`. Para a covariável cirúrgica, somar `Digestivo + Lesoes_Causas_Externas + Geral` em `Geral`, tanto no histórico quanto no futuro. Preservar como séries próprias Cardiologia, Geniturinário, Respiratório/Tosse/Febre, Infecciosas/Parasitárias e Ortopedia. Isso gera **seis** séries clínicas distintas. O sétimo agrupamento solicitado, Ginecológica/Obstétrica, permanece dentro de Geral; não atribuir zero a ele nem reconstruí-lo artificialmente. Documentar essa diferença em toda saída.

## Mapa de alterações mínimas na cópia da referência

| Células da referência | Alteração |
|---|---|
| 7 | Configurar catálogo/caminhos pessoais e data de corte de forma reproduzível. |
| 8, 11 | Trocar fonte e filtros de cirurgia/PS Campinas por Curitiba/Pilar. Na célula 11, substituir o filtro por `ds_carater` pelo complemento da regra de 24 horas da implementação de eletivas, com vínculo à agenda por `hub + nr_cirurgia`, primeiro `dt_agendamento` e deduplicação da mesma população base. Manter `Realizada` e data anterior ao corte. Remover a consulta redundante de PS da célula 8 se não for consumida. |
| 10–12, 17–18, 25, 30, 35, 37, 40, 44 | Substituir observabilidade `utils` por saída em memória, mantendo o miolo de cada transformação. |
| 17–19 | Usar a regra clínica do backtest do Pilar no histórico; agregar por dia e aplicar o agrupamento de seis séries decidido acima. Validar datas, ausências e equivalência de nomes. |
| 25, 35 | Usar o calendário Curitiba e alinhar exatamente as colunas de covariáveis históricas e futuras. Ler o CSV futuro pessoal de PS, validar 365 datas sem duplicatas e montar o mesmo `TimeSeries` usado na referência. |
| 30, 37 | Substituir o preflight da tabela de PS por checagem do arquivo pessoal. Preservar modelos, avaliação e Prophet futuro. Conferir que o target termina em D-1. |
| 43–45 | Substituir `write_to_table` e a leitura de volta da tabela por CSV e manifesto versionados no Workspace pessoal; manter o DataFrame final e `display`. |

## Ordem de execução e critérios de avanço

1. Executar `implementacoes/curitiba/urgentes/validar_fontes_urgentes_curitiba.ipynb` no Workspace pessoal. Conferir esquemas, estabelecimento, vínculo cirurgia–agenda, partição eletiva/não eletiva pela regra de 24 horas, divergências com `ds_carater`, série até D-1, calendário e CSV retrospectivo.
2. Executar `executar_backtest_nao_eletivas_curitiba.ipynb`. Conferir as contagens por horizonte, principalmente a amostra única de D+90, antes de interpretar métricas.
3. Para uma comparação estável em D+90, gerar depois um novo backtest de PS com janela de eventos ampliada para 179 dias, mantendo o snapshot, o modelo `v1_prod`, o retreino semanal e as seis DFUs.
4. Copiar o notebook de referência e aplicar somente as alterações da tabela acima. Comparar célula a célula com o original e documentar cada diferença.
5. Fazer uma execução exploratória da cópia em pasta pessoal. Verificar que nenhuma célula grava tabela/log fora do Workspace, que a previsão final contém 365 dias únicos e valores não negativos, e que os artefatos têm manifesto de proveniência.

O notebook de validação é somente leitura e está pronto para conferência. Ele ainda não foi executado em compute Databricks nesta etapa; os resultados de volume, frescor e esquema dependem dessa execução.
