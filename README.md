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

---

## 🛠️ Arquivos desta pasta (`docker-app/`)

- [Dockerfile](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/Dockerfile): Imagem base com GDAL nativo (`ogr2ogr`/`ogrinfo`), Python 3.11 e bibliotecas C do PostGIS.
- [docker-compose.yml](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/docker-compose.yml): Configuração de inicialização, portas e volumes.
- [requirements.txt](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/requirements.txt): Dependências Python (Flask, Flask-SocketIO, psycopg2-binary).
- [etl_engine.py](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/etl_engine.py): Motor central com 100% da lógica de negócio e validação da camada Bronze.
- [app.py](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/app.py): Servidor Flask com WebSockets para streaming de logs em tempo real e upload em lote.
- [templates/index.html](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/templates/index.html): Interface web moderna com Drag & Drop e console de logs interativo.
- [static/css/style.css](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/static/css/style.css): Estilização Dark Theme e glassmorphism.
- [static/js/app.js](file:///c:/Users/hatan/OneDrive/Programacao/01_aplicacoes/ETL_RF/docker-app/static/js/app.js): Gerenciamento do cliente WebSocket e uploads.
