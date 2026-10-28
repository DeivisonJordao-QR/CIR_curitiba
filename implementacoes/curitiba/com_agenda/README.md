# Curitiba — previsão com features de agenda

O código está em `../common.py` e `../forecast.py`, executado por `../run_experimentos.py`.
Esta variante reproduz modelos LightGBM global e local por antecedência (1–50 dias),
prevê o residual de cirurgias ainda não agendadas e soma a agenda elegível no corte.
Para 51–365 dias, treina a série diária por DFU com o histórico e a previsão curta.
Os arquivos gerados ficam somente no workspace pessoal do usuário Databricks.
