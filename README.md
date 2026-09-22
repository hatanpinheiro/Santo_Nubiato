# Documentação da Aplicação Docker: ETL Camada Bronze (Web Edition)

Consulte o documento principal completo em: [../README.md](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/README.md)

---

## 🚀 Como Iniciar Rápido

```bash
docker-compose up -d --build
```

Acesse no navegador: **[http://localhost:5000](http://localhost:5000)**

Para conectar ao PostgreSQL rodando no Windows (fora do Docker):
- **Host**: `host.docker.internal`
- **Porta**: `5432`
- **Banco**: `bronze` (ou o nome do seu banco)
- **Schema**: `public`

## 🌟 Novas Funcionalidades (Atualização v0.2)

- **Monitoramento de Processo & Estagnação (PID):** O sistema captura a execução de background (via `psutil`), se um upload (`ogr2ogr`) travar por mais de 60 segundos o processo é abortado em nível de sistema e reiniciado automaticamente (até 3 retentativas).
- **Controle Total (Pausar/Retomar/Parar):** A interface web agora permite Pausar, Retomar ou Parar o processamento através do isolamento de PID da tarefa.
- **Percentual de Ingestão Ao Vivo:** O console web passou a exibir atualizações em tempo real com as porcentagens completadas.
- **Prevenção de Duplicidade (Base IBGE):** Antes da ingestão iniciar, o arquivo `GPKG` é inspecionado. Se o código do Município (da camada *limite_perimetro_urbano*) já estiver populado na base de dados (cartografia_bronze), a ingestão é abortada para evitar dados repetidos.

---

## 🛠️ Arquivos desta pasta (`docker-app/`)

- [Dockerfile](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/Dockerfile): Imagem base com GDAL nativo (`ogr2ogr`/`ogrinfo`), Python 3.11 e bibliotecas C do PostGIS.
- [docker-compose.yml](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/docker-compose.yml): Configuração de inicialização, portas e volumes.
- [requirements.txt](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/requirements.txt): Dependências Python (Flask, Flask-SocketIO, psycopg2-binary, psutil).
- [etl_engine.py](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/etl_engine.py): Motor central com a lógica de negócio, captura de `%` de progresso, prevenção de duplicidade (IBGE) e funções de *Stop/Pause* via *psutil*.
- [app.py](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/app.py): Servidor Flask com WebSockets para streaming de logs.
- [templates/index.html](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/templates/index.html): Interface web moderna, atualizada com os novos botões de controle de processos e preenchimento automático.
- [static/css/style.css](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/static/css/style.css): Estilização Dark Theme e glassmorphism.
- [static/js/app.js](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/static/js/app.js): Gerenciamento do cliente WebSocket, com os *event listeners* para os novos botões (Pausar/Retomar/Parar).
