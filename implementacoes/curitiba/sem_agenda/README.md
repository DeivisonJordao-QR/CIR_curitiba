# Curitiba — previsão sem features de agenda

O código está em `../common.py` e `../forecast.py`, executado por `../run_experimentos.py`.
Esta variante usa `dt_agendamento` apenas para determinar quais cirurgias históricas
são eletivas. O LightGBM prevê diretamente o total diário por DFU para 1–365 dias,
sem quantidades ou atributos de agenda como features ou piso de previsão. Os
arquivos gerados ficam somente no workspace pessoal do usuário Databricks.
