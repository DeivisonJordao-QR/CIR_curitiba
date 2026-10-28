# Plano de replicação da previsão de cirurgias eletivas — hub Curitiba

**Estado:** fontes validadas em notebook somente leitura; job `previsao_eletivas_curitiba_experimento` criado e primeira execucao concluida com sucesso (job ID `270569916807221`, run ID `327138555382030`). As previsoes futuras das duas variantes foram salvas no workspace pessoal em 2026-10-20. O backtest vira depois. Escopo diario; horarios comparados como armazenados; `dt_agendamento` e a criacao original; `nr_cirurgia` faz o vinculo; sem gravacao em tabelas.
**Referências lidas:** `referencia/05. Previsão Eletivas.ipynb` e `documentacao_modelagem_previsao_cirurgias.html`.

## Objetivo e limite das mudanças

Replicar **somente a previsão diária** de eletivas para o hub Curitiba em duas implementações comparáveis: uma que usa a agenda como sinal preditivo e outra que não usa features de agenda. Manter, na medida possível, a taxonomia DFU, o calendário, as defasagens, os modelos, os horizontes de 50 e 365 dias e o pós-processamento do notebook de referência. As mudanças deliberadas são: (1) escopo geográfico/hospitalar, (2) definição de eletividade por antecedência mínima de 24 horas, (3) separação das duas variantes e (4) substituição da gravação em tabelas por artefatos no workspace pessoal. Qualquer outro ajuste necessário para executar ou avaliar a réplica deve ser documentado como correção isolada, sem alterar silenciosamente a lógica de Campinas.

O escopo confirmado é **todos os estabelecimentos do hub Curitiba**. O pedido de mudança pontual nos filtros de hub encontra dois filtros adicionais específicos de Campinas no notebook: `cd_estabelecimento = 2` no histórico e `ds_estabelecimento = 'Hospital Vera Cruz S/a'` na agenda. Esses filtros de estabelecimento devem ser removidos nas duas fontes; manter qualquer um deles restringiria indevidamente o hub. Trocar `Campinas` por `Curitiba` nos filtros de `hub` e validar o valor exato existente nas fontes. O calendário também filtra `cidade = 'Campinas'` e deve usar Curitiba.

O notebook `05. Previsão Eletivas.ipynb` produz uma linha **por dia e DFU** com `previsao_final` e `agendamentos`. Sua célula final grava em `{catalog}.ds_pipeline.previsao_eletivas`; **não calcula distribuição por hora**. A documentação descreve a quebra horária como uma etapa separada, no notebook `07. Quebra horária - Cirurgias`, que consome a previsão diária, preserva agendamentos na hora marcada e distribui o restante pelas proporções históricas. Essa etapa e as integrações consumidoras ficam fora desta experimentação.

## Estrutura de trabalho

```text
implementacoes/curitiba/
├── com_agenda/       # alvo residual e features de agenda disponíveis no corte
└── sem_agenda/       # alvo total; sem features, piso ou previsão auxiliar de agenda
```

Na implementação, manter uma extração compartilhada da população elegível e das séries por DFU, com configuração única dos filtros Curitiba. Cada variante terá seu próprio ponto de entrada/configuração e artefatos de backtest. A variante sem agenda consultará o registro de agendamento **somente para determinar se uma cirurgia histórica satisfaz a regra de eletividade**, por meio de `dt_agendamento`; isso não autoriza usar quantidades ou atributos de agenda como features. Registrar essa dependência no contrato de dados.

No Databricks, notebooks, codigo, configuracao do job e resultados ficam na pasta pessoal `/Users/deivison.jordao-dhauz@hospitalcare.com.br/experimentos/previsao_eletivas_curitiba`. O job sob demanda foi criado a pedido do usuario. Nenhuma tabela, schema, bundle, notebook compartilhado ou destino dev/prod deve ser criado ou alterado. As fontes existentes sao somente leitura.

## Contrato de eletividade e população

1. Uma cirurgia realizada é eletiva quando `dt_inicio_real - dt_agendamento >= 24 horas`, conforme confirmado pelo usuário. A igualdade em 24 horas entra; menos de 24 horas não entra. `ds_carater` deixa de ser o teste de eletividade, mas pode ser mantido apenas para auditoria/comparação.
2. `dt_agendamento` contém a hora original de criação e preserva esse valor após reagendamentos, conforme confirmado pelo usuário. O notebook o converte para `date`, o que não comprova 24 horas exatas. Comparar os timestamps completos **exatamente como estão armazenados**, sem converter fuso nem aplicar deslocamento de horas, e não substituir por diferença de datas. Vincular agenda e cirurgia realizada por `nr_cirurgia`, com `hub` no pareamento para manter os registros de Curitiba isolados.
3. Sem agendamento correspondente, ou com `dt_agendamento` ausente em todos os registros vinculados, a cirurgia **não é eletiva** neste projeto. Havendo vários agendamentos para a mesma cirurgia, usar o menor `dt_agendamento` (primeira criação) para a regra de 24 horas e contar a cirurgia apenas uma vez. Em empate exato, qualquer registro serve; usar um desempate determinístico disponível apenas para tornar a reexecução reproduzível, sem exigir um novo identificador. Registrar contagens de ausência, multiplicidade e empate.
4. Para a agenda futura da variante `com_agenda`, aplicar o mesmo limiar usando `dt_agenda - dt_agendamento >= 24 horas`, conforme confirmado pelo usuário, sujeito à verificação de que `dt_agenda` contém hora. A qualificação definitiva do realizado usa `dt_inicio_real`. Conferir os casos em que o horário previsto difere do real e medir o impacto.
5. Manter filtros operacionais existentes de realização, agenda ativa, cancelamento, agenda virtual, imagem e marcador robótico quando fizerem sentido em Curitiba. Validar que a mesma população e a mesma regra DFU são usadas no alvo, nas features e na previsão futura. Auditar separadamente exclusões por cada filtro.
6. Para o histórico de agenda, reconstruir somente o estado conhecido em cada data/hora de corte: criação até o corte, cancelamento depois do corte e valores de horário/DFU válidos naquele corte. O notebook trabalha com antecedências inteiras de 1 a 50 dias e extrai parte do cancelamento de texto; verificar se essas fontes permitem reconstrução temporal fiel. Se não permitirem, marcar o backtest com agenda como aproximado e não usar visão atual da agenda para simular o passado sem essa ressalva.
7. Fixar a hora de cada corte de previsão na mesma convenção registrada nas fontes e derivar dela os 50 níveis de antecedência; a comparação em horas deve usar timestamps completos, sem conversão de fuso. Documentar a convenção de `dias_antecedencia` e `distancia_previsao` para evitar deslocamento de um dia entre treino, backtest e inferência.

## Mapa de alterações no notebook de referência

| Trecho | Situação encontrada | Alteração planejada |
|---|---|---|
| Célula 3, data de processamento | `datetime.utcnow() - timedelta(hours=3)` fixa deslocamento de três horas | Definir data/hora de corte na mesma convenção dos valores lidos, sem converter `dt_agenda`, `dt_inicio_real` ou `dt_agendamento` nem aplicar deslocamento fixo a eles. |
| Célula 7, extração | `hub = 'Campinas'` em cirurgia e agenda | Substituir por filtro Curitiba único e verificável; manter joins por `hub`. |
| Célula 8, calendário | `cidade = 'Campinas'` | Usar calendário de Curitiba e checar cobertura diária. |
| Células 9 e 18, população | `hub = 'Campinas'`, estabelecimento `2`/`Hospital Vera Cruz S/a`, `ds_carater like '%Eletiva%'` | Filtrar `hub = 'Curitiba'` após validar o valor exato, remover os filtros de estabelecimento e substituir o critério de eletividade pela regra de 24 horas. |
| Célula 9, série histórica | Agenda por `datediff(date, date)` e alvo de realizadas por `ds_carater` | Vincular cirurgia e agenda por `hub` + `nr_cirurgia`; selecionar o menor `dt_agendamento`, excluir realizadas sem agendamento e aplicar 24 horas exatas; manter scaffold diário/DFU/antecedência na variante com agenda. |
| Células 18–35 e 54, curto prazo | Treino de `y - agendamentos`, inferência com agenda e soma de volta; global/local LightGBM | Preservar na variante `com_agenda`, após corrigir população e disponibilidade temporal. Na `sem_agenda`, treinar diretamente `y` por DFU sem `agendamentos` ou dimensão de antecedência. |
| Células 40–64, longo prazo e saída | 365 dias; combinação da previsão curta com a longa; piso pela agenda | Preservar horizonte e calendário; na `sem_agenda`, projetar o total por DFU para todo o horizonte sem previsão curta baseada em agenda nem piso por agenda. |
| Células 68–69, gravação | `apply_schema_from_table` consulta o esquema da tabela e `write_to_table` grava em `{catalog}.ds_pipeline.previsao_eletivas` | Não executar essas células. Salvar previsões diárias e métricas das duas variantes em arquivos separados no workspace pessoal, com `hub`, `variante` e data de corte nos metadados. |
| Células 6–9, 18, 23, 30, 50, 54, 64 e 69, observabilidade | `start_pipeline_log`/`end_pipeline_log` podem persistir logs fora do notebook | Verificar a implementação de `utils`; no experimento, substituir por logs/arquivos locais ao workspace pessoal ou desativar essas chamadas. |

## Sequência de execução

1. **Validar fontes e escopo.** Confirmar o valor exato de `hub = Curitiba` nas fontes, sem restringir estabelecimento; conferir os tipos de `dt_agendamento`, `dt_agenda`, `dt_inicio_real` e `nr_cirurgia` nas duas fontes. Verificar nulos, multiplicidade e cardinalidade de `hub` + `nr_cirurgia`. Comparar os horários registrados sem conversão de fuso. Levantar cobertura desde 2023, valores nulos/duplicados, cancelamentos e DFUs presentes. Registrar contagens por etapa de filtro e por mês.
2. **Construir uma base elegível comum.** Vincular cada cirurgia realizada aos seus agendamentos por `hub` + `nr_cirurgia`, excluir as sem vínculo, escolher o primeiro `dt_agendamento` e gerar a contagem diária por `data_cirurgia` e DFU com a regra de 24 horas. Testar casos de 23h59, 24h00, 24h01, reagendamento, nulos, empate de criação e mudança de horário. Congelar a mesma base/versão para comparar as duas variantes.
3. **Implementar `com_agenda`.** Reproduzir o painel histórico de antecedência de 1–50 dias, contando apenas agenda ativa e elegível no corte. Treinar o residual `realizadas - agendamentos`, somar a agenda na inferência e preservar a previsão final `>= agendamentos`. Usar o longo prazo da referência para dias 51–365. Gerar o calendário futuro até o dia 365 mesmo que a última cirurgia agendada seja anterior: no notebook, o scaffold termina em `max(dt_agenda)`.
4. **Implementar `sem_agenda`.** Partir do mesmo alvo elegível, mas treinar a série diária total por DFU só com histórico do alvo, calendário e defasagens disponíveis no corte. Prever 1–365 dias; remover contagem de agenda, alvo residual, dimensão de antecedência, concatenação de previsões curtas baseadas em agenda e piso pela agenda. Preservar arredondamento e limite inferior zero.
5. **Backtest posterior das variantes.** Depois de gerar e conferir a primeira previsão futura, usar os mesmos cortes, janelas de treino, horizonte e alvo final nas duas variantes; registrar erros, viés e cobertura de agenda. Datas de corte, período histórico mínimo, métricas e tolerâncias serão definidos nessa fase, sem bloquear a preparação da réplica agora.
6. **Verificar o experimento.** A primeira previsao futura foi executada pelo job sob demanda `previsao_eletivas_curitiba_experimento` no cluster existente; job ID `270569916807221`, run ID `327138555382030`. Ambas as variantes produziram 365 dias por DFU e salvaram CSV e manifesto no workspace pessoal. Nenhuma tabela foi gravada.

## Critérios de aceite propostos

- A regra de 24 horas é aplicada a 100% dos registros com timestamps válidos, com política explícita para nulos e vínculos ambíguos; as contagens de exclusão são auditáveis.
- As duas variantes usam a mesma série de realizadas elegíveis e o mesmo conjunto de cortes do backtest.
- `sem_agenda` não tem `agendamentos` nem derivados como covariável, pós-processamento ou entrada da previsão longa.
- `com_agenda` usa apenas informação disponível no corte; sua previsão final é pelo menos a agenda ativa elegível do respectivo corte.
- Os arquivos das duas variantes ficam somente no workspace pessoal e não colidem entre si; previsões cobrem todos os dias 1–365 e DFUs definidos, sem duplicatas ou valores negativos.
- A execucao usa o job solicitado e somente le fontes; resultados e codigo ficam na pasta pessoal. Nenhuma tabela, bundle ou notebook compartilhado foi modificado.
- A escolha de uma variante ficará para uma fase posterior de avaliação; este plano não fixa datas de corte nem tolerâncias de erro.

## Chave de vínculo definida

Por decisão do usuário, usar `nr_cirurgia` para vincular `hc_cirurgia` a `hc_agenda_cirurgica`, sempre dentro do mesmo `hub`. O notebook de referência seleciona `nr_cirurgia` em `base_agendamentos`, mas atualmente junta apenas contagens por `data_cirurgia` e `dfu`; por isso o vínculo individual é uma mudança necessária para aplicar a regra de 24 horas. Conferir tipos, nulos e cardinalidade da chave na execução; múltiplos agendamentos e empates já têm tratamento definido acima.

**Estado da primeira execucao:** job do forecast futuro criado a pedido do usuario e run concluido com sucesso. Tabelas nao foram alteradas.

## Parametros atuais do backtest

O desenho trimestral descrito acima foi substituido por parametros escolhidos em cada execucao:

- `data_corte`: aceita `DD/MM/AAAA` ou `AAAA-MM-DD`. O treino termina no dia anterior; a previsao comeca na data de corte.
- `dias_previsao`: quantidade de dias corridos previstos, de 1 a 365.
- Horizontes avaliados no relatorio: somente D1, D2, D7, D30 e D90 que caibam no total de dias previsto. Por exemplo, para 180 dias, o relatorio compara esses cinco pontos.
- Valores iniciais do job: `01/01/2026` e `180` dias. O minimo de 730 dias de treino e preservado.
- O job permite substituir os valores padrao a cada execucao; seus parametros de notebook sao obtidos com widgets Databricks.

## Backtest implementado

- Duas variantes avaliadas lado a lado nos mesmos cortes e no horizonte de 365 dias.
- Cortes trimestrais em janeiro, abril, julho e outubro, desde que tenham pelo menos 730 dias de treino e 365 dias completos de realizado. Para a data de execução, os cortes elegíveis são calculados a partir de `START_DATE` e `current_date()`.
- A série-alvo é a contagem diária por DFU das cirurgias realizadas elegíveis pela regra de 24 horas. O treino de cada corte termina no dia anterior ao corte.
- Métricas descritivas: MAE, RMSE, viés com sinal (previsão menos realizado) e WAPE, por corte, DFU e faixa de antecedência. Não há tolerâncias ou decisão automática de promoção; a escolha entre variantes fica para análise posterior.
- A variante sem agenda usa a agenda apenas no rótulo histórico, conforme a definição de eletividade. A variante com agenda usa o painel por antecedência e previsão mínima igual à agenda estimada no corte.
- Resultados gravados como CSV e manifesto em arquivos no Workspace pessoal; não são criadas ou alteradas tabelas.
- **Limitação temporal:** a fonte atual não é um snapshot versionado de `dt_agenda`. Os filtros de criação e cancelamento reduzem vazamento, porém não reconstroem com garantia alterações do horário feitas depois de cada corte. Portanto, o resultado da variante com agenda deve ser interpretado como aproximado até haver histórico point-in-time confiável.

## Execução parametrizada concluída

- Job `backtest_eletivas_curitiba_experimento`, ID `1067775063187706`, criado no workspace pessoal com os parâmetros `data_corte` e `dias_previsao`.
- Run final corrigido `564215717571535`, task run `910067642183550`, terminou com sucesso. Parâmetros usados: corte `01/01/2026`, previsão de 180 dias.
- O relatório contém D1, D2, D7, D30 e D90. O arquivo final tem 60 previsões (30 por variante, seis DFUs em cada um dos cinco horizontes) e métricas separadas por variante.
- Artefatos finais em `/Users/deivison.jordao-dhauz@hospitalcare.com.br/experimentos/previsao_eletivas_curitiba/backtest/backtest_20260101_180d_78fef270/`.
- A primeira execução `298324597990674` teve um erro de agregação que misturava métricas das variantes; ela foi substituída pelo run corrigido. Nenhuma tabela foi gravada.
