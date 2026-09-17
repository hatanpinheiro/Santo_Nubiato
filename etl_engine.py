"""
================================================================================
PROJETO: Automacao de Carga - Base BRONZE (Web Edition)
ENGINE: ETL Engine - Logica de negocio extraida do script original v4.0
OBJETIVO: Realizar a ingestao inicial/atualizacao de dados geograficos do GPKG
           para o PostgreSQL/PostGIS (Base BRONZE), com validacao/criacao da
           infraestrutura, auditoria e log via callback.
--------------------------------------------------------------------------------
VERSAO: 4.0.0-web
NOTA: Toda a logica de negocio foi preservada integralmente do script original
      Atualizacao_Importacao_Dados_Camada_Bronze_v4.py.
      A unica diferenca e o mecanismo de log (callback em vez de Tkinter).
================================================================================
"""

import os
import re
import subprocess
import psycopg2
from psycopg2 import sql


SQL_INFRAESTRUTURA_BRONZE = r'''
--Criar a tabela de edificação
CREATE TABLE IF NOT EXISTS edificacao (
id  serial PRIMARY KEY,
edif_area_m2  numeric NOT NULL,
confidence  numeric(5,4) NOT NULL,
cd_uf  varchar(2) NULL,
nm_mun  varchar(254) NULL,
cd_mun  varchar(7) NOT NULL,
fonte   varchar(350) NULL,
geometria  geometry(Point, 4674) NOT NULL,
data_carga  timestamp DEFAULT now() NOT NULL,
arquivo_origem  varchar(255) NOT NULL,
responsavel_carga  varchar(150) DEFAULT current_user NOT NULL,
versao_modelo  int DEFAULT 1 NOT null);

-- Criação do Índice Espacial (GIST) para a coluna GEOMETRIA
CREATE INDEX IF NOT EXISTS  edificacao_geometria_idx
ON edificacao 
USING GIST (geometria);

-- Trigger de auditoria
CREATE TRIGGER trg_audit_bronze AFTER
DELETE
    OR
UPDATE
    ON
    edificacao for each row execute function public.fn_audit_log();

-- Comentário da Tabela
COMMENT ON TABLE edificacao  IS 'Camada de itendificação das edificações';

-- Comentários das Colunas (Dicionário de Dados)
COMMENT ON COLUMN edificacao.data_carga IS 'Data/hora da inserção original';
COMMENT ON COLUMN edificacao.arquivo_origem IS 'Identificação do arquivo de origme';
COMMENT ON COLUMN edificacao.responsavel_carga IS 'Nome do operador que realizou a carga';
COMMENT ON COLUMN edificacao.versao_modelo IS 'Número da versão do arquivo de origem';
COMMENT ON COLUMN edificacao.edif_area_m2 IS 'Área em metros quadrados da edificação';
COMMENT ON COLUMN edificacao.confidence IS 'Grau de acurácia da identificação da edificação';
COMMENT ON COLUMN edificacao.id IS 'Identificador primário';
COMMENT ON COLUMN edificacao.geometria IS 'Geometria primitiva da camada';
COMMENT ON COLUMN edificacao.cd_uf IS 'Código da Unidade da Federação conforme definição do IBGE';
COMMENT ON COLUMN edificacao.nm_mun IS 'Nome do município';
COMMENT ON COLUMN edificacao.cd_mun IS 'Código do Município';
COMMENT ON COLUMN edificacao.fonte IS 'Fonte de origem da edificação';

-- Atualização de estatísticas
ANALYZE edificacao;

-- Criar a tabela eixo_curto
CREATE TABLE IF NOT EXISTS eixo_curto(
id  serial PRIMARY KEY,
tip_log  varchar(50) NULL,
nm_log  varchar(254) NULL,
id_osm  bigint NULL,
sup_via  varchar(50) NULL,
cd_mun  varchar(7) NULL,
nm_mun  varchar(254) NULL,
geometria  geometry(MultiLinestring, 4674) NOT NULL,
data_carga  timestamp DEFAULT now() NOT NULL,
arquivo_origem  varchar(255) NOT NULL,
responsavel_carga  varchar(150) DEFAULT current_user NOT NULL,
versao_modelo  int DEFAULT 1 NOT NULL);

-- Criação do Índice Espacial (GIST) para a coluna GEOMETRIA
CREATE INDEX IF NOT EXISTS  eixo_curto_idx
ON eixo_curto 
USING GIST (geometria);

-- Trigger de auditoria
CREATE TRIGGER trg_audit_bronze AFTER
DELETE
    OR
UPDATE
    ON
    eixo_curto for each row execute function public.fn_audit_log();

-- Comentário da Tabela
COMMENT ON TABLE eixo_curto  IS 'Camada de identificação dos eixos curtos';

-- Comentários das Colunas (Dicionário de Dados)
COMMENT ON COLUMN eixo_curto.tip_log IS 'Tipo da logradouro';
COMMENT ON COLUMN eixo_curto.nm_log IS 'Nome do logradouro';
COMMENT ON COLUMN eixo_curto.id_osm IS 'Identificador primário oriundo do OSM';
COMMENT ON COLUMN eixo_curto.sup_via IS 'Tipo de material construtivo da superfície viária';
COMMENT ON COLUMN eixo_curto.id IS 'Identificador interno da feição na base geoespacial (gerado pelo sistema SIG)';
COMMENT ON COLUMN eixo_curto.cd_mun IS 'Código do Município ';
COMMENT ON COLUMN eixo_curto.nm_mun IS 'Nome do Município';
COMMENT ON COLUMN eixo_curto.geometria IS 'Geometria primitiva da camada';
COMMENT ON COLUMN eixo_curto.data_carga IS 'Data/hora da inserção original';
COMMENT ON COLUMN eixo_curto.arquivo_origem IS 'Identificação do arquivo de origme';
COMMENT ON COLUMN eixo_curto.responsavel_carga IS 'Nome do operador que realizou a carga';
COMMENT ON COLUMN eixo_curto.versao_modelo IS 'Número da versão do arquivo de origem';

-- Atualização de estatísticas
ANALYZE eixo_curto;


-- Criar a tabela eixo_residual
CREATE TABLE IF NOT EXISTS eixo_residual(
id  serial PRIMARY KEY,
tip_log  varchar(50) NULL,
nm_log  varchar(254) NULL,
id_osm  bigint NULL,
sup_via  varchar(50) NULL,
cd_mun  varchar(7) NULL,
nm_mun  varchar(254) NULL,
geometria  geometry(MultiLinestring, 4674) NOT NULL,
data_carga  timestamp DEFAULT now() NOT NULL,
arquivo_origem  varchar(255) NOT NULL,
responsavel_carga  varchar(150) DEFAULT current_user NOT NULL,
versao_modelo  int DEFAULT 1 NOT NULL);

-- Criação do Índice Espacial (GIST) para a coluna GEOMETRIA
CREATE INDEX IF NOT EXISTS  eixo_residual_idx
ON eixo_residual 
USING GIST (geometria);

-- Trigger de auditoria
CREATE TRIGGER trg_audit_bronze AFTER
DELETE
    OR
UPDATE
    ON
    eixo_residual for each row execute function public.fn_audit_log();

-- Comentário da Tabela
COMMENT ON TABLE eixo_residual  IS 'Camada de identificação dos eixos curtos';

-- Comentários das Colunas (Dicionário de Dados)
COMMENT ON COLUMN eixo_residual.tip_log IS 'Tipo da logradouro';
COMMENT ON COLUMN eixo_residual.nm_log IS 'Nome do logradouro';
COMMENT ON COLUMN eixo_residual.id_osm IS 'Identificador primário oriundo do OSM';
COMMENT ON COLUMN eixo_residual.sup_via IS 'Tipo de material construtivo da superfície viária';
COMMENT ON COLUMN eixo_residual.id IS 'Identificador interno da feição na base geoespacial (gerado pelo sistema SIG)';
COMMENT ON COLUMN eixo_residual.cd_mun IS 'Código do Município ';
COMMENT ON COLUMN eixo_residual.nm_mun IS 'Nome do Município';
COMMENT ON COLUMN eixo_residual.geometria IS 'Geometria primitiva da camada';
COMMENT ON COLUMN eixo_residual.data_carga IS 'Data/hora da inserção original';
COMMENT ON COLUMN eixo_residual.arquivo_origem IS 'Identificação do arquivo de origme';
COMMENT ON COLUMN eixo_residual.responsavel_carga IS 'Nome do operador que realizou a carga';
COMMENT ON COLUMN eixo_residual.versao_modelo IS 'Número da versão do arquivo de origem';

-- Atualização de estatísticas
ANALYZE eixo_residual;

-- Criar a tabela face_logradouro
CREATE TABLE IF NOT EXISTS face_logradouro(
id  serial PRIMARY KEY,
cd_face  varchar(20) NULL,
cd_mun  varchar(7) NULL,
cd_quadra  varchar(20) NULL,
cd_setor  varchar(15) NULL,
nm_log  varchar(254) NULL,
nm_mun  varchar(254) NULL,
nm_tip_log  varchar(30) NULL,
nm_tit_log  varchar(30) NULL,
qtd_imov_log  integer NULL,
qtd_res_log  integer NULL,
geometria  geometry(MultiLinestring, 4674) NOT NULL,
data_carga  timestamp DEFAULT now() NOT NULL,
arquivo_origem  varchar(255) NOT NULL,
responsavel_carga  varchar(150) DEFAULT current_user NOT NULL,
versao_modelo  int DEFAULT 1 NOT NULL);

-- Criação do Índice Espacial (GIST) para a coluna GEOMETRIA
CREATE INDEX IF NOT EXISTS  face_logradouro_idx
ON face_logradouro 
USING GIST (geometria);

-- Trigger de auditoria
CREATE TRIGGER trg_audit_bronze AFTER
DELETE
    OR
UPDATE
    ON
    face_logradouro for each row execute function public.fn_audit_log();

-- Comentário da Tabela
COMMENT ON TABLE face_logradouro  IS 'Camada de identificação das faces de logradouros';

-- Comentários das Colunas (Dicionário de Dados)
COMMENT ON COLUMN face_logradouro.cd_face IS 'Código da face de logradouro';
COMMENT ON COLUMN face_logradouro.cd_mun IS 'Código do município';
COMMENT ON COLUMN face_logradouro.cd_quadra IS 'Código da quadra';
COMMENT ON COLUMN face_logradouro.cd_setor IS 'Geocódigo de Setor Censitário';
COMMENT ON COLUMN face_logradouro.id IS 'Identificador primário';
COMMENT ON COLUMN face_logradouro.geometria IS 'Geometria primitiva da camada';
COMMENT ON COLUMN face_logradouro.nm_log IS 'Nome do Logradouro';
COMMENT ON COLUMN face_logradouro.nm_mun IS 'Nome do município';
COMMENT ON COLUMN face_logradouro.nm_tip_log IS 'Nome do tipo de logradouro';
COMMENT ON COLUMN face_logradouro.nm_tit_log IS 'Nome do título de logradouro';
COMMENT ON COLUMN face_logradouro.qtd_imov_log IS 'Quantidade de imóveis da face de logradouro';
COMMENT ON COLUMN face_logradouro.qtd_res_log IS 'Quantidade de imóveis residenciais da face de logradouro';
COMMENT ON COLUMN face_logradouro.data_carga IS 'Data/hora da inserção original';
COMMENT ON COLUMN face_logradouro.arquivo_origem IS 'Identificação do arquivo de origme';
COMMENT ON COLUMN face_logradouro.responsavel_carga IS 'Nome do operador que realizou a carga';
COMMENT ON COLUMN face_logradouro.versao_modelo IS 'Número da versão do arquivo de origem';

-- Atualização de estatísticas
ANALYZE face_logradouro;

-- Criar a tabela limite_perimetro_urbano
CREATE TABLE IF NOT EXISTS limite_perimetro_urbano (
id  serial PRIMARY KEY,
nm_mun  varchar(254) NULL,
cd_mun  varchar(7) NULL,
geometria  geometry(MultiPolygon, 4674) NOT NULL,
data_carga  timestamp DEFAULT now() NOT NULL,
arquivo_origem  varchar(255) NOT NULL,
responsavel_carga  varchar(150) DEFAULT current_user NOT NULL,
versao_modelo  int DEFAULT 1 NOT NULL);

-- Criação do Índice Espacial (GIST) para a coluna GEOMETRIA
CREATE INDEX IF NOT EXISTS  limite_perimetro_urbano_idx
ON limite_perimetro_urbano 
USING GIST (geometria);

-- Trigger de auditoria
CREATE TRIGGER trg_audit_bronze AFTER
DELETE
    OR
UPDATE
    ON
    limite_perimetro_urbano for each row execute function public.fn_audit_log();

-- Comentário da Tabela
COMMENT ON TABLE limite_perimetro_urbano IS 'Camada de identificação do limite urbano';


-- Comentários das Colunas (Dicionário de Dados)
COMMENT ON COLUMN limite_perimetro_urbano.data_carga IS 'Data/hora da inserção original';
COMMENT ON COLUMN limite_perimetro_urbano.arquivo_origem IS 'Identificação do arquivo de origme';
COMMENT ON COLUMN limite_perimetro_urbano.responsavel_carga IS 'Nome do operador que realizou a carga';
COMMENT ON COLUMN limite_perimetro_urbano.versao_modelo IS 'Número da versão do arquivo de origem';
COMMENT ON COLUMN limite_perimetro_urbano.cd_mun IS 'Código do Município';
COMMENT ON COLUMN limite_perimetro_urbano.geometria IS 'Geometria primitiva da camada';
COMMENT ON COLUMN limite_perimetro_urbano.id IS 'Identificador interno da feição na base geoespacial (gerado pelo sistema SIG).';
COMMENT ON COLUMN limite_perimetro_urbano.nm_mun IS 'Nome do Município';

-- Atualização de estatísticas
ANALYZE limite_perimetro_urbano;


-- Criar a tabela linha_geral
CREATE TABLE IF NOT EXISTS linha_geral (
id  serial PRIMARY KEY,
tip_log  varchar(50) NULL,
nm_log  varchar(254) NULL,
id_osm  bigint NULL,
sup_via  varchar(50) NULL,
cd_mun  varchar(7) NULL,
nm_mun  varchar(254) NULL,
geometria  geometry(MultiLinestring, 4674) NOT NULL,
data_carga  timestamp DEFAULT now() NOT NULL,
arquivo_origem  varchar(255) NOT NULL,
responsavel_carga  varchar(150) DEFAULT current_user NOT NULL,
versao_modelo  int DEFAULT 1 NOT null);

-- Criação do Índice Espacial (GIST) para a coluna GEOMETRIA
CREATE INDEX IF NOT EXISTS  linha_geral_idx
ON linha_geral 
USING GIST (geometria);

-- Trigger de auditoria
CREATE TRIGGER trg_audit_bronze AFTER
DELETE
    OR
UPDATE
    ON
    linha_geral for each row execute function public.fn_audit_log();

-- Comentário da Tabela
COMMENT ON TABLE linha_geral  IS 'Camada de identificação das linhas gerais da OSM';

-- Comentários das Colunas (Dicionário de Dados)
COMMENT ON COLUMN linha_geral.tip_log IS 'Tipo da logradouro';
COMMENT ON COLUMN linha_geral.nm_log IS 'Nome do logradouro';
COMMENT ON COLUMN linha_geral.id_osm IS 'Identificador primário oriundo do OSM';
COMMENT ON COLUMN linha_geral.sup_via IS 'Tipo de material construtivo da superfície viária';
COMMENT ON COLUMN linha_geral.id IS 'Identificador interno da feição na base geoespacial (gerado pelo sistema SIG)';
COMMENT ON COLUMN linha_geral.cd_mun IS 'Código do Município ';
COMMENT ON COLUMN linha_geral.nm_mun IS 'Nome do Município';
COMMENT ON COLUMN linha_geral.geometria IS 'Geometria primitiva da camada';
COMMENT ON COLUMN linha_geral.data_carga IS 'Data/hora da inserção original';
COMMENT ON COLUMN linha_geral.arquivo_origem IS 'Identificação do arquivo de origme';
COMMENT ON COLUMN linha_geral.responsavel_carga IS 'Nome do operador que realizou a carga';
COMMENT ON COLUMN linha_geral.versao_modelo IS 'Número da versão do arquivo de origem';

-- Atualização de estatísticas
ANALYZE linha_geral;

-- Criar a tabela logradouro
CREATE TABLE IF NOT EXISTS logradouro (
id  serial PRIMARY KEY,
nm_log  varchar(254) NULL,
cd_mun  varchar(7) NULL,
nm_mun  varchar(254) NULL,
tip_log  varchar(50) NULL,
id_osm  bigint NULL,
sup_via  varchar(50) NULL,
geometria  geometry(MultiLinestring, 4674) NOT NULL,
data_carga  timestamp DEFAULT now() NOT NULL,
arquivo_origem  varchar(255) NOT NULL,
responsavel_carga  varchar(150) DEFAULT current_user NOT NULL,
versao_modelo  int DEFAULT 1 NOT null);

-- Criação do Índice Espacial (GIST) para a coluna GEOMETRIA
CREATE INDEX IF NOT EXISTS  logradouro_idx
ON logradouro 
USING GIST (geometria);

-- Trigger de auditoria
CREATE TRIGGER trg_audit_bronze AFTER
DELETE
    OR
UPDATE
    ON
    logradouro for each row execute function public.fn_audit_log();

-- Comentário da Tabela
COMMENT ON TABLE logradouro  IS 'Camada de identificação dos logradouros';

-- Comentários das Colunas (Dicionário de Dados)
COMMENT ON COLUMN logradouro.cd_mun IS 'Código do Município ';
COMMENT ON COLUMN logradouro.geometria IS 'Geometria primitiva da camada';
COMMENT ON COLUMN logradouro.tip_log IS 'Tipo da logradouro';
COMMENT ON COLUMN logradouro.id IS 'Identificador interno da feição na base geoespacial (gerado pelo sistema SIG)';
COMMENT ON COLUMN logradouro.nm_log IS 'Nome do logradouro';
COMMENT ON COLUMN logradouro.nm_mun IS 'Nome do Município';
COMMENT ON COLUMN logradouro.id_osm IS 'Identificador primário oriundo do OSM';
COMMENT ON COLUMN logradouro.sup_via IS 'Tipo de material construtivo da superfície viária';
COMMENT ON COLUMN logradouro.data_carga IS 'Data/hora da inserção original';
COMMENT ON COLUMN logradouro.arquivo_origem IS 'Identificação do arquivo de origme';
COMMENT ON COLUMN logradouro.responsavel_carga IS 'Nome do operador que realizou a carga';
COMMENT ON COLUMN logradouro.versao_modelo IS 'Número da versão do arquivo de origem';

-- Atualização de estatísticas
ANALYZE logradouro;

-- Criar a tabela setor_censitario
CREATE TABLE IF NOT EXISTS setor_censitario (
id  serial PRIMARY KEY,
setor_area_km2  numeric NULL,
cd_aglom  varchar(12) NULL,
cd_bairro  varchar(10) NULL,
cd_concurb  varchar(7) NULL,
cd_dist  varchar(9) NULL,
cd_fcu  varchar(11) NULL,
cd_mun  varchar(7) NULL,
cd_nu  varchar(10) NULL,
cd_regiao  varchar(1) NULL,
cd_rgi  varchar(6) NULL,
cd_rgint  varchar(4) NULL,
cd_setor  varchar(15) UNIQUE,
cd_sit  varchar(1) NULL,
cd_subdist  varchar(11) NULL,
cd_tipo  varchar(1) NULL,
cd_uf  varchar(2) NULL,
nm_aglom  varchar(254) NULL,
nm_bairro  varchar(254) NULL,
nm_concurb  varchar(254) NULL,
nm_dist  varchar(50) NULL,
nm_fcu  varchar(254) NULL,
nm_mun  varchar(254) NULL,
nm_nu  varchar(254) NULL,
nm_regiao  varchar(15) NULL,
nm_rgi  varchar(254) NULL,
nm_rgint  varchar(254) NULL,
nm_subdist  varchar(50) NULL,
nm_uf  varchar(20) NULL,
situacao  varchar(6) NULL,
geometria  public.geometry(MultiPolygon, 4674) NOT NULL,
data_carga  timestamp DEFAULT now() NOT NULL,
arquivo_origem  varchar(255) NOT NULL,
responsavel_carga  varchar(150) DEFAULT current_user NOT NULL,
versao_modelo  int DEFAULT 1 NOT NULL);

-- Criação do Índice Espacial (GIST) para a coluna GEOMETRIA
CREATE INDEX IF NOT EXISTS  setor_censitario_idx
ON setor_censitario 
USING GIST (geometria);

-- Trigger de auditoria
CREATE TRIGGER trg_audit_bronze AFTER
DELETE
    OR
UPDATE
    ON
    setor_censitario for each row execute function public.fn_audit_log();

-- Comentário da Tabela
COMMENT ON TABLE setor_censitario  IS 'Camada de identificação dos setores censitários';

-- Comentários das Colunas (Dicionário de Dados)
COMMENT ON COLUMN setor_censitario.setor_area_km2 IS 'Área em quilômetros quadrados';
COMMENT ON COLUMN setor_censitario.cd_aglom IS 'Código do Aglomerado';
COMMENT ON COLUMN setor_censitario.cd_bairro IS 'Código do Bairro';
COMMENT ON COLUMN setor_censitario.cd_concurb IS 'Código da Concentração Urbana';
COMMENT ON COLUMN setor_censitario.cd_dist IS 'Código do Distrito';
COMMENT ON COLUMN setor_censitario.cd_fcu IS 'Código da Favela ou Comunidade Urbana';
COMMENT ON COLUMN setor_censitario.cd_mun IS 'Código do Município';
COMMENT ON COLUMN setor_censitario.cd_nu IS 'Código do Núcleo Urbano';
COMMENT ON COLUMN setor_censitario.cd_regiao IS 'Código das Grandes Regiões (Regiões Geográficas)';
COMMENT ON COLUMN setor_censitario.cd_rgi IS 'Código da Região Geográfica Imediata';
COMMENT ON COLUMN setor_censitario.cd_rgint IS 'Código da Região Geográfica Intermediária';
COMMENT ON COLUMN setor_censitario.cd_setor IS 'Geocódigo de Setor Censitário';
COMMENT ON COLUMN setor_censitario.cd_sit IS 'Código da Situação do Setor Censitário';
COMMENT ON COLUMN setor_censitario.cd_subdist IS 'Código do Subdistrito';
COMMENT ON COLUMN setor_censitario.cd_tipo IS 'Código do Tipo do Setor Censitário';
COMMENT ON COLUMN setor_censitario.cd_uf IS 'Código da Unidade da Federação conforme definição do IBGE';
COMMENT ON COLUMN setor_censitario.geometria IS 'Geometria primitiva da camada';
COMMENT ON COLUMN setor_censitario.id IS 'Identificador interno da feição na base geoespacial (gerado pelo sistema SIG).';
COMMENT ON COLUMN setor_censitario.nm_aglom IS 'Nome do Aglomerado';
COMMENT ON COLUMN setor_censitario.nm_bairro IS 'Nome do Bairro';
COMMENT ON COLUMN setor_censitario.nm_concurb IS 'Nome da Concentração Urbana';
COMMENT ON COLUMN setor_censitario.nm_dist IS 'Nome do Distrito';
COMMENT ON COLUMN setor_censitario.nm_fcu IS 'Nome da Favela ou Comunidade Urbana';
COMMENT ON COLUMN setor_censitario.nm_mun IS 'Nome do Município';
COMMENT ON COLUMN setor_censitario.nm_nu IS 'Nome do Núcleo Urbano';
COMMENT ON COLUMN setor_censitario.nm_regiao IS 'Nome da Grande Região Geográfica do IBGE';
COMMENT ON COLUMN setor_censitario.nm_rgi IS 'Nome da Região Geográfica Imediata';
COMMENT ON COLUMN setor_censitario.nm_rgint IS 'Nome da Região Geográfica Intermediária';
COMMENT ON COLUMN setor_censitario.nm_subdist IS 'Nome do Subdistrito';
COMMENT ON COLUMN setor_censitario.nm_uf IS 'Nome da Unidade da Federação';
COMMENT ON COLUMN setor_censitario.situacao IS 'Situação do Setor Censitário';
COMMENT ON COLUMN setor_censitario.data_carga IS 'Data/hora da inserção original';
COMMENT ON COLUMN setor_censitario.arquivo_origem IS 'Identificação do arquivo de origme';
COMMENT ON COLUMN setor_censitario.responsavel_carga IS 'Nome do operador que realizou a carga';
COMMENT ON COLUMN setor_censitario.versao_modelo IS 'Número da versão do arquivo de origem';

-- Atualização de estatísticas
ANALYZE setor_censitario;

-- Criar a tabela setor_censitario_urbano
CREATE TABLE IF NOT EXISTS setor_censitario_urbano (
id  serial PRIMARY KEY, 
setor_area_km2  numeric NULL, 
cd_aglom  varchar(12) NULL, 
cd_bairro  varchar(10) NULL, 
cd_concurb  varchar(7) NULL, 
cd_dist  varchar(9) NULL, 
cd_fcu  varchar(11) NULL, 
cd_mun  varchar(7) NULL, 
cd_nu  varchar(10) NULL, 
cd_regiao  varchar(1) NULL, 
cd_rgi  varchar(6) NULL, 
cd_rgint  varchar(4) NULL, 
cd_setor  varchar(15) UNIQUE, 
cd_sit  varchar(1) NULL, 
cd_subdist  varchar(11) NULL, 
cd_tipo  varchar(1) NULL, 
cd_uf  varchar(2) NULL, 
nm_aglom  varchar(254) NULL, 
nm_bairro  varchar(254) NULL, 
nm_concurb  varchar(254) NULL, 
nm_dist  varchar(50) NULL, 
nm_fcu  varchar(254) NULL, 
nm_mun  varchar(254) NULL, 
nm_nu  varchar(254) NULL, 
nm_regiao  varchar(15) NULL, 
nm_rgi  varchar(254) NULL, 
nm_rgint  varchar(254) NULL, 
nm_subdist  varchar(50) NULL, 
nm_uf  varchar(20) NULL, 
situacao  varchar(6) NULL,
geometria  public.geometry(MultiPolygon, 4674) NOT NULL, 
data_carga  timestamp DEFAULT now() NOT NULL, 
arquivo_origem  varchar(255) NOT NULL, 
responsavel_carga  varchar(150) DEFAULT current_user NOT NULL, 
versao_modelo  int DEFAULT 1 NOT null);

-- Criação do Índice Espacial (GIST) para a coluna GEOMETRIA
CREATE INDEX IF NOT EXISTS  setor_censitario_urbano_idx
ON setor_censitario_urbano 
USING GIST (geometria);

-- Trigger de auditoria
CREATE TRIGGER trg_audit_bronze AFTER
DELETE
    OR
UPDATE
    ON
    setor_censitario_urbano for each row execute function public.fn_audit_log();

-- Comentário da Tabela
COMMENT ON TABLE setor_censitario_urbano  IS 'Camada de identificação dos setores censitários urbanos';

-- Comentários das Colunas (Dicionário de Dados)
COMMENT ON COLUMN setor_censitario_urbano.setor_area_km2 IS 'Área em quilômetros quadrados';
COMMENT ON COLUMN setor_censitario_urbano.data_carga IS 'Data/hora da inserção original';
COMMENT ON COLUMN setor_censitario_urbano.arquivo_origem IS 'Identificação do arquivo de origme';
COMMENT ON COLUMN setor_censitario_urbano.responsavel_carga IS 'Nome do operador que realizou a carga';
COMMENT ON COLUMN setor_censitario_urbano.versao_modelo IS 'Número da versão do arquivo de origem';
COMMENT ON COLUMN setor_censitario_urbano.cd_aglom IS 'Código do Aglomerado';
COMMENT ON COLUMN setor_censitario_urbano.cd_bairro IS 'Código do Bairro';
COMMENT ON COLUMN setor_censitario_urbano.cd_concurb IS 'Código da Concentração Urbana';
COMMENT ON COLUMN setor_censitario_urbano.cd_dist IS 'Código do Distrito';
COMMENT ON COLUMN setor_censitario_urbano.cd_fcu IS 'Código da Favela ou Comunidade Urbana';
COMMENT ON COLUMN setor_censitario_urbano.cd_mun IS 'Código do Município';
COMMENT ON COLUMN setor_censitario_urbano.cd_nu IS 'Código do Núcleo Urbano';
COMMENT ON COLUMN setor_censitario_urbano.cd_regiao IS 'Código das Grandes Regiões (Regiões Geográficas)';
COMMENT ON COLUMN setor_censitario_urbano.cd_rgi IS 'Código da Região Geográfica Imediata';
COMMENT ON COLUMN setor_censitario_urbano.cd_rgint IS 'Código da Região Geográfica Intermediária';
COMMENT ON COLUMN setor_censitario_urbano.cd_setor IS 'Geocódigo de Setor Censitário';
COMMENT ON COLUMN setor_censitario_urbano.cd_sit IS 'Código da Situação do Setor Censitário';
COMMENT ON COLUMN setor_censitario_urbano.cd_subdist IS 'Código do Subdistrito';
COMMENT ON COLUMN setor_censitario_urbano.cd_tipo IS 'Código do Tipo do Setor Censitário';
COMMENT ON COLUMN setor_censitario_urbano.cd_uf IS 'Código da Unidade da Federação conforme definição do IBGE';
COMMENT ON COLUMN setor_censitario_urbano.geometria IS 'Geometria primitiva da camada';
COMMENT ON COLUMN setor_censitario_urbano.id IS 'Identificador interno da feição na base geoespacial (gerado pelo sistema SIG).';
COMMENT ON COLUMN setor_censitario_urbano.nm_aglom IS 'Nome do Aglomerado';
COMMENT ON COLUMN setor_censitario_urbano.nm_bairro IS 'Nome do Bairro';
COMMENT ON COLUMN setor_censitario_urbano.nm_concurb IS 'Nome da Concentração Urbana';
COMMENT ON COLUMN setor_censitario_urbano.nm_dist IS 'Nome do Distrito';
COMMENT ON COLUMN setor_censitario_urbano.nm_fcu IS 'Nome da Favela ou Comunidade Urbana';
COMMENT ON COLUMN setor_censitario_urbano.nm_mun IS 'Nome do Município';
COMMENT ON COLUMN setor_censitario_urbano.nm_nu IS 'Nome do Núcleo Urbano';
COMMENT ON COLUMN setor_censitario_urbano.nm_regiao IS 'Nome da Grande Região Geográfica do IBGE';
COMMENT ON COLUMN setor_censitario_urbano.nm_rgi IS 'Nome da Região Geográfica Imediata';
COMMENT ON COLUMN setor_censitario_urbano.nm_rgint IS 'Nome da Região Geográfica Intermediária';
COMMENT ON COLUMN setor_censitario_urbano.nm_subdist IS 'Nome do Subdistrito';
COMMENT ON COLUMN setor_censitario_urbano.nm_uf IS 'Nome da Unidade da Federação';
COMMENT ON COLUMN setor_censitario_urbano.situacao IS 'Situação do Setor Censitário';

-- Atualização de estatísticas
ANALYZE setor_censitario_urbano;

-- Criar a tabela subsetor_urbano
CREATE TABLE IF NOT EXISTS subsetor_urbano (
id  serial PRIMARY KEY,
cd_setor  varchar(15) NOT NULL,
cd_subsetor  varchar(20) NOT NULL,
cd_uf  varchar(2) NOT NULL,
cd_mun  varchar(7) NOT NULL,
geometria  geometry(MultiPolygon, 4674) NOT NULL,
data_carga  timestamp DEFAULT now() NOT NULL,
arquivo_origem  varchar(255) NOT NULL,
responsavel_carga  varchar(150) DEFAULT current_user NOT NULL,
versao_modelo  int DEFAULT 1 NOT null);

-- Criação do Índice Espacial (GIST) para a coluna GEOMETRIA
CREATE INDEX IF NOT EXISTS  subsetor_urbano_idx
ON subsetor_urbano 
USING GIST (geometria);

-- Trigger de auditoria
CREATE TRIGGER trg_audit_bronze AFTER
DELETE
    OR
UPDATE
    ON
    subsetor_urbano for each row execute function public.fn_audit_log();

-- Comentário da Tabela
COMMENT ON TABLE subsetor_urbano  IS 'Camada de identificação dos subsetores urbanos';

-- Comentários das Colunas (Dicionário de Dados)
COMMENT ON COLUMN subsetor_urbano.id IS 'Código de identificação do subsetor';
COMMENT ON COLUMN subsetor_urbano.cd_setor IS 'Código de identificação do setor censitário';
COMMENT ON COLUMN subsetor_urbano.cd_subsetor IS 'Código de identificação do subsetor devivado do setor';
COMMENT ON COLUMN subsetor_urbano.geometria IS 'Geometria primitiva da camada sendo multiplos polígonos';
COMMENT ON COLUMN subsetor_urbano.cd_uf IS 'Código da UF';
COMMENT ON COLUMN subsetor_urbano.cd_mun IS 'Código do município';
COMMENT ON COLUMN subsetor_urbano.data_carga IS 'Data/hora da inserção original';
COMMENT ON COLUMN subsetor_urbano.arquivo_origem IS 'Identificação do arquivo de origme';
COMMENT ON COLUMN subsetor_urbano.responsavel_carga IS 'Nome do operador que realizou a carga';
COMMENT ON COLUMN subsetor_urbano.versao_modelo IS 'Número da versão do arquivo de origem';

-- Atualização de estatísticas
ANALYZE subsetor_urbano;

-- Criar a tabela subsetor_urbano
CREATE TABLE IF NOT EXISTS impermeabilidade (
id  serial PRIMARY KEY, 
area_edif_total  numeric NULL, 
area_liq_setor  numeric NULL, 
area_setor_m2  numeric NULL, 
cd_mun  varchar(7) NULL, 
cd_setor  varchar(15) NOT NULL, 
edif_maximo  numeric NULL, 
edif_media  numeric NULL, 
edif_mediana  numeric NULL, 
edif_minimo  numeric NULL, 
impermeab  numeric NULL, 
geometria  public.geometry(multipolygon, 4674) NOT NULL, 
nm_mun  varchar(250) NULL, 
data_carga  timestamp DEFAULT now() NOT NULL, 
arquivo_origem  varchar(255) NOT NULL, 
responsavel_carga  varchar(150) DEFAULT current_user NOT NULL, 
versao_modelo  int DEFAULT 1 NOT null);

-- Criação do Índice Espacial (GIST) para a coluna GEOMETRIA
CREATE INDEX IF NOT EXISTS  impermeabilidadeidx
ON impermeabilidade 
USING GIST (geometria);

-- Trigger de auditoria
CREATE TRIGGER trg_audit_bronze AFTER
DELETE
    OR
UPDATE
    ON
    impermeabilidade for each row execute function public.fn_audit_log();

-- Comentário da Tabela
COMMENT ON TABLE impermeabilidade  IS 'Camada de impermeabilidade e área edificada por setor censitário';

-- Comentários das Colunas (Dicionário de Dados)
COMMENT ON COLUMN impermeabilidade.area_edif_total IS 'Área edificada total do setor censitário';
COMMENT ON COLUMN impermeabilidade.area_liq_setor IS 'Área líquida do setor censitário (85% da área total)';
COMMENT ON COLUMN impermeabilidade.area_setor_m2 IS 'Área do setor censitário em metros quadrados';
COMMENT ON COLUMN impermeabilidade.cd_mun IS 'Código do município ';
COMMENT ON COLUMN impermeabilidade.cd_setor IS 'Código do setor censitário';
COMMENT ON COLUMN impermeabilidade.edif_maximo IS 'Maior área de uma edificação do setor censitário';
COMMENT ON COLUMN impermeabilidade.edif_media IS 'Média da área das edificações do setor censitário';
COMMENT ON COLUMN impermeabilidade.edif_mediana IS 'Mediana  da área das edificações do setor censitário';
COMMENT ON COLUMN impermeabilidade.edif_minimo IS 'Menor área de uma edificação do setor censitário';
COMMENT ON COLUMN impermeabilidade.id IS 'Identificador interno da feição na base geoespacial (gerado pelo sistema SIG).';
COMMENT ON COLUMN impermeabilidade.geometria IS 'Geometria de representação do setor censitário';
COMMENT ON COLUMN impermeabilidade.impermeab IS 'Taxa de impermeabilidade calculada';
COMMENT ON COLUMN impermeabilidade.nm_mun IS 'Nome do município';
COMMENT ON COLUMN impermeabilidade.data_carga IS 'Data/hora da inserção original';
COMMENT ON COLUMN impermeabilidade.arquivo_origem IS 'Identificação do arquivo de origme';
COMMENT ON COLUMN impermeabilidade.responsavel_carga IS 'Nome do operador que realizou a carga';
COMMENT ON COLUMN impermeabilidade.versao_modelo IS 'Número da versão do arquivo de origem';

-- Atualização de estatísticas
ANALYZE impermeabilidade;
'''


class ETLEngine:
    """
    Motor ETL com toda a logica de negocio do script original.
    Usa um callback emit_log(mensagem) para enviar logs em vez de Tkinter.
    """

    def __init__(self, emit_log=None, emit_progress=None):
        self.emit_log = emit_log or (lambda msg: print(msg))
        self.emit_progress = emit_progress or (lambda current, total: None)

        self.mapeamento_camadas = {
            'limite_perimetro_urbano': 'limite_perimetro_urbano',
            'setor_censitario': 'setor_censitario',
            'setor_censitario_urbano': 'setor_censitario_urbano',
            'subsetor_urbano': 'subsetor_urbano',
            'impermeabilidade': 'impermeabilidade',
            'logradouro': 'logradouro',
            'linha_geral': 'linha_geral',
            'eixo_residual': 'eixo_residual',
            'eixo_curto': 'eixo_curto',
            'face_logradouro': 'face_logradouro',
            'edificacao': 'edificacao'
        }

        self.colunas_controle = {
            'id', 'geometria', 'geom', 'fid', 'ogc_fid',
            'data_carga', 'arquivo_origem', 'responsavel_carga', 'versao_modelo'
        }

    def log(self, mensagem):
        self.emit_log(mensagem)

    def obter_schema_validado(self, schema):
        schema = schema.strip()
        if not schema:
            raise ValueError("Informe o schema destino.")
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", schema):
            raise ValueError("O nome do schema deve conter apenas letras, numeros e underscore, iniciando por letra ou underscore.")
        return schema

    def extrair_blocos_tabelas(self, texto_sql):
        texto = texto_sql.replace("\r\n", "\n")
        matches = list(re.finditer(r"(?im)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*\(", texto))
        blocos = []
        for idx, match in enumerate(matches):
            nome_tabela = match.group(1)
            inicio = match.start()
            fim = matches[idx + 1].start() if idx + 1 < len(matches) else len(texto)
            blocos.append((nome_tabela, texto[inicio:fim].strip()))
        return blocos

    def tabela_existe(self, cur, schema, tabela):
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_name = %s
                  AND table_type = 'BASE TABLE'
            );
            """,
            (schema, tabela)
        )
        return bool(cur.fetchone()[0])

    def extrair_tabelas_do_sql(self, texto_sql):
        return re.findall(r"(?im)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][A-Za-z0-9_]*)", texto_sql)

    def garantir_infraestrutura_publica(self, cur):
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.log_ingestao_dados (
                id bigserial PRIMARY KEY,
                data_execucao timestamp DEFAULT now() NOT NULL,
                arquivo_fonte varchar(255) NOT NULL,
                formato_extensao varchar(50) NULL,
                srid_origem integer NULL,
                schema_destino varchar(100) NOT NULL,
                tabela_destino varchar(150) NOT NULL,
                registros_camada integer NULL,
                status_processamento varchar(30) NOT NULL,
                id_revisao_entrega integer NULL,
                data_producao date NULL,
                observacao_tecnica text NULL,
                usuario_execucao varchar(150) DEFAULT current_user NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.audit_log (
                id bigserial PRIMARY KEY,
                schema_name text NOT NULL,
                table_name text NOT NULL,
                operation text NOT NULL,
                old_data jsonb NULL,
                changed_at timestamp DEFAULT now() NOT NULL,
                changed_by text DEFAULT current_user NOT NULL
            );
        """)

        cur.execute("""
            CREATE OR REPLACE FUNCTION public.fn_audit_log()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $fn$
            BEGIN
                INSERT INTO public.audit_log (schema_name, table_name, operation, old_data, changed_by)
                VALUES (TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP, to_jsonb(OLD), current_user);
                RETURN OLD;
            END;
            $fn$;
        """)

    def verificar_tabelas_criadas(self, cur, schema, tabelas):
        if not tabelas:
            return [], []
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = ANY(%s)
            ORDER BY table_name;
            """,
            (schema, tabelas)
        )
        existentes = [linha[0] for linha in cur.fetchall()]
        faltantes = sorted(set(tabelas) - set(existentes))
        return existentes, faltantes

    def executar_infraestrutura(self, db_config, schema):
        try:
            schema = self.obter_schema_validado(schema)
            blocos = self.extrair_blocos_tabelas(SQL_INFRAESTRUTURA_BRONZE)
            tabelas_previstas = [nome for nome, _ in blocos]

            self.log("🔧 Iniciando verificação/criação da infraestrutura BRONZE...")
            self.log(f"   Schema destino: {schema}")
            self.log("   Origem dos comandos SQL: SQL oficial embutido no proprio script Python, sem substituições dinâmicas")
            self.log(f"   Tabelas previstas no SQL embutido: {len(tabelas_previstas)}\n")

            conn = psycopg2.connect(**db_config)
            criadas = []
            existentes_antes = []
            try:
                with conn:
                    with conn.cursor() as cur:
                        self.log("   Garantindo PostGIS, tabela de log e funcao de auditoria...")
                        self.garantir_infraestrutura_publica(cur)

                        self.log("   Criando schema quando necessario...")
                        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema)))
                        cur.execute(sql.SQL("SET search_path TO {}, public;").format(sql.Identifier(schema)))

                        for nome_tabela, bloco_sql in blocos:
                            if self.tabela_existe(cur, schema, nome_tabela):
                                existentes_antes.append(nome_tabela)
                                self.log(f"   ✅ Já existe: {schema}.{nome_tabela} - nenhuma criação realizada.")
                            else:
                                self.log(f"   🆕 Criando: {schema}.{nome_tabela}...")
                                cur.execute(bloco_sql)
                                criadas.append(nome_tabela)
                                self.log(f"      Criada com sucesso: {schema}.{nome_tabela}")

                        existentes_depois = self.verificar_tabelas_criadas(cur, schema, tabelas_previstas)[0]
                        faltantes = sorted(set(tabelas_previstas) - set(existentes_depois))

                self.log("\n✅ Infraestrutura processada com sucesso.<br>Amém.<br><br><img src='/static/img/logo.png' style='width: 60px; height: 60px; object-fit: contain; border-radius: 4px;'>")
                self.log(f"   Tabelas já existentes antes da execução: {len(existentes_antes)}")
                self.log(f"   Tabelas criadas nesta execução: {len(criadas)}")
                self.log(f"   Tabelas existentes após a execução: {len(existentes_depois)}/{len(tabelas_previstas)}")

                if criadas:
                    self.log("   Criadas: " + ", ".join(criadas))
                if existentes_antes:
                    self.log("   Mantidas sem alteracao: " + ", ".join(existentes_antes))
                if faltantes:
                    self.log("\n⚠️ Tabelas previstas não encontradas após a execução: " + ", ".join(faltantes))
                    return {"success": False, "warning": "Algumas tabelas previstas não foram encontradas."}
                else:
                    return {"success": True, "message": "Infraestrutura BRONZE verificada/criada com sucesso."}
            finally:
                conn.close()

        except Exception as e:
            self.log(f"\n❌ Erro ao criar/verificar infraestrutura: {e}")
            return {"success": False, "error": str(e)}

    def executar_comando_ogr(self, cmd):
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            shell=(os.name == 'nt')
        )

    def listar_camadas_gpkg(self, gpkg):
        try:
            cmd = ["ogrinfo", "-ro", "-q", gpkg]
            res = self.executar_comando_ogr(cmd)
            if res.returncode != 0:
                raise RuntimeError(res.stderr.strip() or res.stdout.strip())
            camadas = []
            for linha in res.stdout.splitlines():
                match = re.match(r"^\s*\d+\s*:\s*(.+?)(?:\s+\(|\s*$)", linha)
                if match:
                    camadas.append(match.group(1).strip().strip('"'))
            return camadas
        except Exception as e:
            self.log(f"  ⚠️ Não foi possivel listar camadas do GPKG: {e}")
            return []

    def camada_existe_gpkg(self, gpkg, camada):
        camadas = self.listar_camadas_gpkg(gpkg)
        camadas_lower = {c.lower(): c for c in camadas}
        return camada.lower() in camadas_lower, camadas_lower.get(camada.lower())

    def obter_info_camada_gpkg(self, gpkg, camada):
        cmd = ["ogrinfo", "-ro", "-so", gpkg, camada]
        res = self.executar_comando_ogr(cmd)
        if res.returncode != 0:
            raise RuntimeError(res.stderr.strip() or res.stdout.strip())

        tipos_ogr = (
            "Integer", "Integer64", "Real", "String", "Date", "DateTime", "Time",
            "Binary", "IntegerList", "Integer64List", "RealList", "StringList"
        )

        info = {
            "campos": [],
            "geometry_column": None,
            "geometry_type": None,
            "srid": None,
            "raw": res.stdout
        }

        for linha in res.stdout.splitlines():
            linha_limpa = linha.strip()
            if not linha_limpa:
                continue

            match_geom = re.match(r"^Geometry\s*:\s*(.+)$", linha_limpa, flags=re.IGNORECASE)
            if match_geom:
                info["geometry_type"] = match_geom.group(1).strip()
                continue

            match_geom_col = re.match(r"^Geometry\s+Column\s*(?:=|:)\s*(.+)$", linha_limpa, flags=re.IGNORECASE)
            if match_geom_col:
                info["geometry_column"] = match_geom_col.group(1).strip().strip('"')
                continue

            if info["srid"] is None:
                match_epsg = re.search(r"EPSG[^0-9]*(\d{3,6})", linha_limpa, flags=re.IGNORECASE)
                if match_epsg:
                    info["srid"] = int(match_epsg.group(1))

            if ":" not in linha_limpa:
                continue

            nome, resto = linha_limpa.split(":", 1)
            nome = nome.strip().strip('"')
            tipo_bruto = resto.strip()
            tipo = tipo_bruto.split(" ", 1)[0].strip()
            subtipo = None
            match_subtipo = re.match(r"^([A-Za-z0-9]+)\(([^)]+)\)", tipo)
            if match_subtipo:
                tipo = match_subtipo.group(1)
                subtipo = match_subtipo.group(2)

            if tipo in tipos_ogr and nome.lower() not in {"fid", "ogc_fid"}:
                info["campos"].append({
                    "name": nome,
                    "ogr_type": tipo,
                    "ogr_subtype": subtipo,
                    "raw_type": tipo_bruto
                })

        if not info["geometry_column"]:
            info["geometry_column"] = "geom"

        return info

    def obter_campos_gpkg(self, gpkg, camada):
        return [c["name"] for c in self.obter_info_camada_gpkg(gpkg, camada)["campos"]]

    def obter_contagem_gpkg(self, gpkg, camada):
        try:
            cmd = ["ogrinfo", "-so", gpkg, camada]
            res = self.executar_comando_ogr(cmd)
            match = re.search(r'Feature Count: (\d+)', res.stdout)
            return int(match.group(1)) if match else 0
        except Exception:
            return 0

    def obter_colunas_tabela(self, cur, schema, tabela):
        cur.execute(
            """
            SELECT
                column_name,
                data_type,
                udt_name,
                is_nullable,
                column_default,
                ordinal_position,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (schema, tabela)
        )
        return [
            {
                "column_name": row[0],
                "data_type": row[1],
                "udt_name": row[2],
                "is_nullable": row[3],
                "column_default": row[4],
                "ordinal_position": row[5],
                "character_maximum_length": row[6],
                "numeric_precision": row[7],
                "numeric_scale": row[8],
            }
            for row in cur.fetchall()
        ]

    def obter_geometria_tabela(self, cur, schema, tabela):
        cur.execute(
            """
            SELECT
                a.attname AS geometry_column,
                public.postgis_typmod_type(a.atttypmod) AS geometry_type,
                public.postgis_typmod_srid(a.atttypmod) AS srid
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND a.atttypid = 'public.geometry'::regtype
            ORDER BY a.attnum
            LIMIT 1;
            """,
            (schema, tabela)
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "geometry_column": row[0],
            "geometry_type": row[1],
            "srid": row[2],
        }

    def obter_campos_negocio_tabela(self, cur, schema, tabela):
        colunas = self.obter_colunas_tabela(cur, schema, tabela)
        campos = []
        for coluna in colunas:
            nome = coluna["column_name"]
            if nome.lower() not in self.colunas_controle:
                campos.append(coluna)
        return campos

    def categoria_tipo_ogr(self, tipo_ogr, subtipo_ogr=None):
        tipo = (tipo_ogr or "").lower()
        subtipo = (subtipo_ogr or "").lower()
        if subtipo == "boolean":
            return "boolean"
        if tipo in {"integer", "integer64"}:
            return "integer"
        if tipo == "real":
            return "numeric"
        if tipo == "string":
            return "text"
        if tipo == "date":
            return "date"
        if tipo in {"datetime", "time"}:
            return "datetime"
        if tipo == "binary":
            return "binary"
        return "unknown"

    def categoria_tipo_pg(self, coluna):
        data_type = (coluna.get("data_type") or "").lower()
        udt_name = (coluna.get("udt_name") or "").lower()
        if data_type in {"smallint", "integer", "bigint"} or udt_name in {"int2", "int4", "int8"}:
            return "integer"
        if data_type in {"numeric", "real", "double precision"} or udt_name in {"numeric", "float4", "float8"}:
            return "numeric"
        if data_type in {"character varying", "character", "text"} or udt_name in {"varchar", "bpchar", "text"}:
            return "text"
        if data_type == "boolean" or udt_name == "bool":
            return "boolean"
        if data_type == "date":
            return "date"
        if "timestamp" in data_type or data_type == "time without time zone" or data_type == "time with time zone":
            return "datetime"
        if data_type == "bytea":
            return "binary"
        return "unknown"

    def tipos_compativeis(self, coluna_pg, campo_gpkg):
        cat_pg = self.categoria_tipo_pg(coluna_pg)
        cat_ogr = self.categoria_tipo_ogr(campo_gpkg.get("ogr_type"), campo_gpkg.get("ogr_subtype"))

        if cat_pg == cat_ogr:
            return {"compativel": True, "conversivel": False, "tipo_pg": cat_pg, "tipo_ogr": cat_ogr, "conversao": None}

        if cat_pg == "numeric" and cat_ogr == "integer":
            return {"compativel": True, "conversivel": False, "tipo_pg": cat_pg, "tipo_ogr": cat_ogr, "conversao": None}

        if cat_pg == "integer" and cat_ogr == "numeric":
            return {"compativel": False, "conversivel": True, "tipo_pg": cat_pg, "tipo_ogr": cat_ogr, "conversao": "numeric_to_integer"}

        if cat_pg == "boolean" and cat_ogr in {"integer", "numeric"}:
            return {"compativel": False, "conversivel": True, "tipo_pg": cat_pg, "tipo_ogr": cat_ogr, "conversao": f"{cat_ogr}_to_boolean"}

        if cat_ogr == "text" and cat_pg in {"integer", "numeric", "boolean", "date", "datetime"}:
            return {"compativel": False, "conversivel": True, "tipo_pg": cat_pg, "tipo_ogr": cat_ogr, "conversao": f"text_to_{cat_pg}"}

        return {"compativel": False, "conversivel": False, "tipo_pg": cat_pg, "tipo_ogr": cat_ogr, "conversao": None}

    def expr_texto_normalizado(self, campo):
        ident = self.sql_ident_ogr(campo)
        return f"NULLIF(TRIM({ident}), '')"

    def expr_primeiro_item_texto_para_inteiro(self, campo):
        t = self.expr_texto_normalizado(campo)
        return (
            f"CASE "
            f"WHEN {t} IS NULL THEN NULL "
            f"WHEN INSTR({t}, ';') > 0 THEN NULLIF(TRIM(SUBSTR({t}, 1, INSTR({t}, ';') - 1)), '') "
            f"WHEN INSTR({t}, '|') > 0 THEN NULLIF(TRIM(SUBSTR({t}, 1, INSTR({t}, '|') - 1)), '') "
            f"WHEN INSTR({t}, ' / ') > 0 THEN NULLIF(TRIM(SUBSTR({t}, 1, INSTR({t}, ' / ') - 1)), '') "
            f"WHEN INSTR({t}, ', ') > 0 THEN NULLIF(TRIM(SUBSTR({t}, 1, INSTR({t}, ', ') - 1)), '') "
            f"ELSE {t} END"
        )

    def expr_valor_numerico_texto(self, campo, primeiro_item_inteiro=False):
        t = self.expr_primeiro_item_texto_para_inteiro(campo) if primeiro_item_inteiro else self.expr_texto_normalizado(campo)
        v = f"REPLACE({t}, ' ', '')"
        return (
            f"CASE "
            f"WHEN {t} IS NULL THEN NULL "
            f"WHEN INSTR({v}, ',') > 0 AND INSTR({v}, '.') > 0 AND INSTR({v}, ',') > INSTR({v}, '.') "
            f"THEN REPLACE(REPLACE({v}, '.', ''), ',', '.') "
            f"WHEN INSTR({v}, ',') > 0 AND INSTR({v}, '.') > 0 AND INSTR({v}, '.') > INSTR({v}, ',') "
            f"THEN REPLACE({v}, ',', '') "
            f"WHEN INSTR({v}, ',') > 0 THEN REPLACE({v}, ',', '.') "
            f"ELSE {v} END"
        )

    def limites_tipo_inteiro_pg(self, data_type_pg=None, udt_name=None):
        data_type = (data_type_pg or "").lower()
        udt = (udt_name or "").lower()
        if data_type == "smallint" or udt == "int2":
            return -32768, 32767
        if data_type == "bigint" or udt == "int8":
            return -9223372036854775808, 9223372036854775807
        return -2147483648, 2147483647

    def expr_validacao_integer_texto(self, campo, data_type_pg=None, udt_name=None):
        v = self.expr_valor_numerico_texto(campo, primeiro_item_inteiro=True)
        minimo, maximo = self.limites_tipo_inteiro_pg(data_type_pg, udt_name)
        return (
            f"({v} IS NULL OR ("
            f"({v} NOT GLOB '*[^0-9.-]*') AND "
            f"((LENGTH({v}) - LENGTH(REPLACE({v}, '.', ''))) <= 1) AND "
            f"({v} NOT IN ('-', '.', '-.')) AND "
            f"(({v} NOT GLOB '*-*') OR ({v} GLOB '-*' AND SUBSTR({v}, 2) NOT GLOB '*-*')) AND "
            f"(REPLACE(REPLACE({v}, '-', ''), '.', '') GLOB '[0-9]*') AND "
            f"(REPLACE(REPLACE({v}, '-', ''), '.', '') NOT GLOB '*[^0-9]*') AND "
            f"CAST({v} AS INTEGER) = CAST({v} AS REAL) AND "
            f"CAST({v} AS INTEGER) BETWEEN {minimo} AND {maximo}"
            f"))"
        )

    def expr_validacao_numeric_to_integer(self, campo, data_type_pg=None, udt_name=None):
        ident = self.sql_ident_ogr(campo)
        minimo, maximo = self.limites_tipo_inteiro_pg(data_type_pg, udt_name)
        return (
            f"({ident} IS NULL OR ("
            f"CAST({ident} AS INTEGER) = {ident} AND "
            f"CAST({ident} AS INTEGER) BETWEEN {minimo} AND {maximo}"
            f"))"
        )

    def expr_validacao_numeric_to_boolean(self, campo):
        ident = self.sql_ident_ogr(campo)
        return f"({ident} IS NULL OR {ident} IN (0, 1, 0.0, 1.0))"

    def expr_validacao_numeric_texto(self, campo):
        v = self.expr_valor_numerico_texto(campo)
        return (
            f"({v} IS NULL OR ("
            f"({v} NOT GLOB '*[^0-9.-]*') AND "
            f"((LENGTH({v}) - LENGTH(REPLACE({v}, '.', ''))) <= 1) AND "
            f"({v} NOT IN ('-', '.', '-.')) AND "
            f"(({v} NOT GLOB '*-*') OR ({v} GLOB '-*' AND SUBSTR({v}, 2) NOT GLOB '*-*')) AND "
            f"(REPLACE(REPLACE({v}, '-', ''), '.', '') GLOB '[0-9]*') AND "
            f"(REPLACE(REPLACE({v}, '-', ''), '.', '') NOT GLOB '*[^0-9]*')"
            f"))"
        )

    def expr_validacao_boolean_texto(self, campo):
        v = f"LOWER({self.expr_texto_normalizado(campo)})"
        return (
            f"({self.expr_texto_normalizado(campo)} IS NULL OR "
            f"{v} IN ('true','false','t','f','yes','no','y','n','sim','nao','não','s','0','1'))"
        )

    def expr_validacao_date_texto(self, campo):
        v = self.expr_texto_normalizado(campo)
        return (
            f"({v} IS NULL OR "
            f"(({v} GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]' AND DATE({v}) IS NOT NULL) OR "
            f"({v} GLOB '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]' AND "
            f"DATE(SUBSTR({v},7,4)||'-'||SUBSTR({v},4,2)||'-'||SUBSTR({v},1,2)) IS NOT NULL)))"
        )

    def expr_validacao_datetime_texto(self, campo):
        v = self.expr_texto_normalizado(campo)
        return (
            f"({v} IS NULL OR DATETIME({v}) IS NOT NULL OR "
            f"({v} GLOB '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]*' AND "
            f"DATETIME(SUBSTR({v},7,4)||'-'||SUBSTR({v},4,2)||'-'||SUBSTR({v},1,2)||SUBSTR({v},11)) IS NOT NULL))"
        )

    def expr_conversao_campo(self, campo_origem, campo_destino, conversao):
        destino = self.sql_ident_ogr(campo_destino)
        origem = self.sql_ident_ogr(campo_origem)

        if conversao is None:
            return f"{origem} AS {destino}"

        if conversao == "text_to_integer":
            expr = f"CAST({self.expr_valor_numerico_texto(campo_origem, primeiro_item_inteiro=True)} AS INTEGER)"
        elif conversao == "numeric_to_integer":
            expr = f"CAST({origem} AS INTEGER)"
        elif conversao == "text_to_numeric":
            expr = f"CAST({self.expr_valor_numerico_texto(campo_origem)} AS REAL)"
        elif conversao in {"integer_to_boolean", "numeric_to_boolean"}:
            expr = (
                f"CASE "
                f"WHEN {origem} IS NULL THEN NULL "
                f"WHEN {origem} = 1 THEN 1 "
                f"WHEN {origem} = 0 THEN 0 "
                f"ELSE NULL END"
            )
        elif conversao == "text_to_boolean":
            v = f"LOWER({self.expr_texto_normalizado(campo_origem)})"
            expr = (
                f"CASE "
                f"WHEN {self.expr_texto_normalizado(campo_origem)} IS NULL THEN NULL "
                f"WHEN {v} IN ('true','t','yes','y','sim','s','1') THEN 1 "
                f"WHEN {v} IN ('false','f','no','n','nao','não','0') THEN 0 "
                f"ELSE NULL END"
            )
        elif conversao == "text_to_date":
            v = self.expr_texto_normalizado(campo_origem)
            expr = (
                f"CASE "
                f"WHEN {v} IS NULL THEN NULL "
                f"WHEN {v} GLOB '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]' "
                f"THEN DATE(SUBSTR({v},7,4)||'-'||SUBSTR({v},4,2)||'-'||SUBSTR({v},1,2)) "
                f"ELSE DATE({v}) END"
            )
        elif conversao == "text_to_datetime":
            v = self.expr_texto_normalizado(campo_origem)
            expr = (
                f"CASE "
                f"WHEN {v} IS NULL THEN NULL "
                f"WHEN {v} GLOB '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9]*' "
                f"THEN DATETIME(SUBSTR({v},7,4)||'-'||SUBSTR({v},4,2)||'-'||SUBSTR({v},1,2)||SUBSTR({v},11)) "
                f"ELSE DATETIME({v}) END"
            )
        else:
            expr = origem

        return f"{expr} AS {destino}"

    def expr_validacao_conversao(self, campo, conversao, conv=None):
        conv = conv or {}
        if conversao == "text_to_integer":
            return self.expr_validacao_integer_texto(campo, conv.get("data_type_pg"), conv.get("udt_name_pg"))
        if conversao == "numeric_to_integer":
            return self.expr_validacao_numeric_to_integer(campo, conv.get("data_type_pg"), conv.get("udt_name_pg"))
        if conversao == "text_to_numeric":
            return self.expr_validacao_numeric_texto(campo)
        if conversao == "text_to_boolean":
            return self.expr_validacao_boolean_texto(campo)
        if conversao in {"integer_to_boolean", "numeric_to_boolean"}:
            return self.expr_validacao_numeric_to_boolean(campo)
        if conversao == "text_to_date":
            return self.expr_validacao_date_texto(campo)
        if conversao == "text_to_datetime":
            return self.expr_validacao_datetime_texto(campo)
        return "1=1"

    def validar_conversoes_gpkg(self, gpkg, camada, conversoes):
        erros = []
        for conv in conversoes:
            campo = conv["origem"]
            conversao = conv["conversao"]
            expr_valida = self.expr_validacao_conversao(campo, conversao, conv)
            sql_validacao = (
                f"SELECT COUNT(*) AS qtd_invalidos "
                f"FROM {self.sql_ident_ogr(camada)} "
                f"WHERE NOT ({expr_valida})"
            )
            cmd = ["ogrinfo", "-ro", gpkg, "-dialect", "SQLite", "-sql", sql_validacao]
            res = self.executar_comando_ogr(cmd)
            if res.returncode != 0:
                erros.append(f"{campo}: falha na validação da conversão ({res.stderr.strip() or res.stdout.strip()})")
                continue

            match = re.search(r"qtd_invalidos\s*\([^)]*\)\s*=\s*(\d+)", res.stdout, flags=re.IGNORECASE)
            if not match:
                match = re.search(r"qtd_invalidos\s*=\s*(\d+)", res.stdout, flags=re.IGNORECASE)
            qtd_invalidos = int(match.group(1)) if match else None
            if qtd_invalidos is None:
                erros.append(f"{campo}: não foi possível ler a quantidade de valores inválidos")
            elif qtd_invalidos > 0:
                erros.append(f"{campo}: {qtd_invalidos} valor(es) não conversível(is) para {conv['tipo_pg']}")
        return erros

    def normalizar_tipo_geometria(self, valor):
        if valor is None:
            return None
        texto = str(valor).strip().lower()

        # Se contiver parênteses com a especificação OGC/WKT (ex: "Multi Polygon (MULTIPOLYGON)")
        match_parenteses = re.search(r"\(([^)]+)\)", texto)
        if match_parenteses:
            texto = match_parenteses.group(1).strip()

        # Remover espaços, sublinhados e hífens
        texto = re.sub(r"[\s_\-]", "", texto)

        if "multipolygon" in texto:
            return "multipolygon"
        if "multilinestring" in texto:
            return "multilinestring"
        if "multipoint" in texto:
            return "multipoint"
        if "polygon" in texto:
            return "polygon"
        if "linestring" in texto:
            return "linestring"
        if "point" in texto:
            return "point"
        if "geometrycollection" in texto:
            return "geometrycollection"
        if "geometry" in texto:
            return "geometry"

        return texto

    def geometria_compativel(self, tipo_gpkg, tipo_pg):
        src = self.normalizar_tipo_geometria(tipo_gpkg)
        dst = self.normalizar_tipo_geometria(tipo_pg)
        if not src or not dst:
            return False
        if src == dst or dst == "geometry" or src == "geometry":
            return True
        pares_promocao = {
            ("polygon", "multipolygon"),
            ("linestring", "multilinestring"),
            ("point", "multipoint"),
        }
        return (src, dst) in pares_promocao

    def obter_nlt_ogr(self, tipo_pg):
        tipo = self.normalizar_tipo_geometria(tipo_pg)
        if tipo in {"multipoint", "multilinestring", "multipolygon"}:
            return "PROMOTE_TO_MULTI"
        mapa = {
            "point": "POINT",
            "linestring": "LINESTRING",
            "polygon": "POLYGON",
        }
        return mapa.get(tipo, "PROMOTE_TO_MULTI")

    def validar_campos_camada(self, cur, schema, tabela, info_gpkg):
        campos_tabela = self.obter_campos_negocio_tabela(cur, schema, tabela)
        geometria_tabela = self.obter_geometria_tabela(cur, schema, tabela)

        campos_gpkg = info_gpkg.get("campos", [])
        gpkg_por_lower = {
            c["name"].lower(): c
            for c in campos_gpkg
            if c["name"].lower() not in self.colunas_controle
        }
        tabela_por_lower = {c["column_name"].lower(): c for c in campos_tabela}

        faltantes = [c["column_name"] for c in campos_tabela if c["column_name"].lower() not in gpkg_por_lower]
        extras = [c["name"] for chave, c in gpkg_por_lower.items() if chave not in tabela_por_lower]

        divergencias_tipo = []
        conversoes = []
        campos_select = []

        for coluna in campos_tabela:
            chave = coluna["column_name"].lower()
            if chave not in gpkg_por_lower:
                continue
            campo = gpkg_por_lower[chave]
            classificacao = self.tipos_compativeis(coluna, campo)
            if classificacao["compativel"]:
                campos_select.append({
                    "origem": campo["name"],
                    "destino": coluna["column_name"],
                    "conversao": None,
                })
            elif classificacao["conversivel"]:
                item = {
                    "origem": campo["name"],
                    "destino": coluna["column_name"],
                    "conversao": classificacao["conversao"],
                    "tipo_pg": classificacao["tipo_pg"],
                    "tipo_ogr": classificacao["tipo_ogr"],
                    "data_type_pg": coluna.get("data_type"),
                    "udt_name_pg": coluna.get("udt_name"),
                    "raw_type_gpkg": campo.get("raw_type"),
                }
                conversoes.append(item)
                campos_select.append(item)
            else:
                divergencias_tipo.append(
                    f"{coluna['column_name']} tabela={classificacao['tipo_pg']}/{coluna.get('data_type')} x "
                    f"gpkg={classificacao['tipo_ogr']}/{campo.get('raw_type')}"
                )

        erros_geometria = []
        geom_origem = info_gpkg.get("geometry_column")
        tipo_geom_origem = info_gpkg.get("geometry_type")
        srid_origem = info_gpkg.get("srid")

        if not geom_origem:
            erros_geometria.append("campo geometrico nao identificado no GPKG")
        if not tipo_geom_origem:
            erros_geometria.append("tipo geometrico nao identificado no GPKG")
        if not geometria_tabela:
            erros_geometria.append("coluna geometry nao encontrada na tabela destino")
        else:
            if geometria_tabela["geometry_column"].lower() != "geometria":
                erros_geometria.append(
                    f"coluna geometrica destino esperada como geometria, encontrada {geometria_tabela['geometry_column']}"
                )
            if tipo_geom_origem and not self.geometria_compativel(tipo_geom_origem, geometria_tabela.get("geometry_type")):
                erros_geometria.append(
                    f"tipo geometrico incompativel: GPKG={tipo_geom_origem} x PostGIS={geometria_tabela.get('geometry_type')}"
                )
            pass

        valido = not faltantes and not divergencias_tipo and not erros_geometria
        return {
            "valido": valido,
            "campos_tabela": [c["column_name"] for c in campos_tabela],
            "campos_select": campos_select,
            "faltantes": faltantes,
            "extras": extras,
            "divergencias_tipo": divergencias_tipo,
            "conversoes": conversoes,
            "erros_geometria": erros_geometria,
            "geometria_origem": geom_origem,
            "geometria_destino": geometria_tabela["geometry_column"] if geometria_tabela else None,
            "tipo_geometria_origem": tipo_geom_origem,
            "tipo_geometria_destino": geometria_tabela.get("geometry_type") if geometria_tabela else None,
            "srid_origem": srid_origem,
            "srid_destino": geometria_tabela.get("srid") if geometria_tabela else None,
            "reprojetar_srid": bool(
                srid_origem and geometria_tabela and geometria_tabela.get("srid")
                and int(srid_origem) != int(geometria_tabela.get("srid"))
            ),
            "nlt_ogr": self.obter_nlt_ogr(geometria_tabela.get("geometry_type")) if geometria_tabela else "PROMOTE_TO_MULTI",
        }

    def sql_literal(self, valor):
        return str(valor).replace("'", "''")

    def sql_ident_ogr(self, identificador):
        return '"' + str(identificador).replace('"', '""') + '"'

    def montar_parametros_reprojecao_ogr(self, validacao):
        if not validacao.get("reprojetar_srid"):
            return []
        srid_origem = validacao.get("srid_origem")
        srid_destino = validacao.get("srid_destino")
        if not srid_origem or not srid_destino:
            return []
        return ["-s_srs", f"EPSG:{int(srid_origem)}", "-t_srs", f"EPSG:{int(srid_destino)}"]

    def montar_sql_query_ogr(self, camada_origem, campos_select, campo_geometria_origem, campo_geometria_destino, nome_arq, revisao_num):
        campos_sql = [
            self.expr_conversao_campo(item["origem"], item["destino"], item.get("conversao"))
            for item in campos_select
        ]
        campos_sql.append(f"{self.sql_ident_ogr(campo_geometria_origem)} AS {self.sql_ident_ogr(campo_geometria_destino)}")
        campos_sql.append(f"'{self.sql_literal(nome_arq)}' AS arquivo_origem")
        campos_sql.append(f"{int(revisao_num)} AS versao_modelo")
        return f"SELECT {', '.join(campos_sql)} FROM {self.sql_ident_ogr(camada_origem)}"

    def extrair_data_producao_nome_arquivo(self, nome_arq):
        match_data = re.search(r'(\d{4})[-_](\d{2})[-_](\d{2})', nome_arq)
        if not match_data:
            return None
        ano, mes, dia = match_data.groups()
        try:
            from datetime import date
            return date(int(ano), int(mes), int(dia)).isoformat()
        except ValueError:
            return None

    def registrar_log_banco(self, conn, schema, tabela, qtd, status, path, obs=""):
        try:
            nome_arq = os.path.basename(path)
            match_rev = re.search(r'r(\d{3})', nome_arq, flags=re.IGNORECASE)
            revisao_num = int(match_rev.group(1)) if match_rev else 0
            data_prod = self.extrair_data_producao_nome_arquivo(nome_arq)

            cur = conn.cursor()
            sql_insert = """
                INSERT INTO public.log_ingestao_dados
                (arquivo_fonte, formato_extensao, srid_origem, schema_destino, tabela_destino,
                 registros_camada, status_processamento, id_revisao_entrega, data_producao, observacao_tecnica)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(sql_insert, (nome_arq, 'GeoPackage', 4674, schema, tabela,
                                     qtd, status, revisao_num, data_prod, obs[:1000] if obs else obs))
            conn.commit()
            cur.close()
        except Exception as e:
            self.log(f"  ⚠️ Erro ao gravar log no banco: {e}")

    def executar_ingestao(self, db_config, schema, gpkg_path):
        if not gpkg_path:
            return {"success": False, "error": "Nenhum arquivo GPKG informado."}
        if not os.path.exists(gpkg_path):
            return {"success": False, "error": f"Arquivo GPKG nao encontrado: {gpkg_path}"}

        conn = None
        try:
            schema = self.obter_schema_validado(schema)
            conn = psycopg2.connect(**db_config)
            nome_arq = os.path.basename(gpkg_path)
            match_rev = re.search(r'r(\d{3})', nome_arq)
            revisao_num = int(match_rev.group(1)) if match_rev else 0

            pg_dsn = f"PG:host={db_config['host']} port={db_config['port']} dbname={db_config['dbname']} user={db_config['user']} password={db_config['password']}"

            total = len(self.mapeamento_camadas)

            self.log("🚀 Iniciando atualização das camadas BRONZE...")
            self.log(f"   Schema destino: {schema}")
            self.log(f"   GeoPackage: {gpkg_path}")
            self.log("   Validacoes antes da carga: camada no GPKG, tabela no PostGIS e campos compativeis.\n")

            camadas_gpkg = self.listar_camadas_gpkg(gpkg_path)
            self.log(f"   Camadas encontradas no GPKG: {len(camadas_gpkg)}")

            resumo = {"concluidas": 0, "ignoradas": 0, "erros": 0}

            for i, (origem, destino) in enumerate(self.mapeamento_camadas.items(), 1):
                self.log(f"\n📦 Camada ({i}/{total}): {origem} -> {schema}.{destino}")
                self.emit_progress(i - 1, total)

                try:
                    existe_camada, nome_real_camada = self.camada_existe_gpkg(gpkg_path, origem)
                    if not existe_camada:
                        msg = f"Camada de origem ausente no GPKG: {origem}"
                        self.log(f"  ⚠️ {msg}. Carga ignorada.")
                        self.registrar_log_banco(conn, schema, destino, 0, 'IGNORADO', gpkg_path, msg)
                        resumo["ignoradas"] += 1
                        self.emit_progress(i, total)
                        continue

                    with conn.cursor() as cur:
                        if not self.tabela_existe(cur, schema, destino):
                            msg = f"Tabela destino ausente no PostGIS: {schema}.{destino}. Execute a criação de infraestrutura antes da carga."
                            self.log(f"  ❌ {msg}")
                            self.registrar_log_banco(conn, schema, destino, 0, 'ERRO', gpkg_path, msg)
                            resumo["erros"] += 1
                            self.emit_progress(i, total)
                            continue

                        info_gpkg = self.obter_info_camada_gpkg(gpkg_path, nome_real_camada or origem)
                        validacao = self.validar_campos_camada(
                            cur, schema, destino, info_gpkg
                        )

                    qtd_campos_gpkg = len([c for c in info_gpkg.get("campos", []) if c["name"].lower() not in self.colunas_controle])
                    self.log(f"  🔎 Campos esperados na tabela: {len(validacao['campos_tabela'])}")
                    self.log(f"  🔎 Campos encontrados no GPKG: {qtd_campos_gpkg}")
                    self.log(
                        f"  🧭 Geometria GPKG: {validacao['geometria_origem']} "
                        f"({validacao['tipo_geometria_origem']}, SRID={validacao['srid_origem'] or 'não informado'})"
                    )
                    self.log(
                        f"  🧭 Geometria PostGIS: {validacao['geometria_destino']} "
                        f"({validacao['tipo_geometria_destino']}, SRID={validacao['srid_destino']})"
                    )
                    if validacao.get("reprojetar_srid"):
                        self.log(
                            f"  🗺️ Transformação de SRID será aplicada na carga: "
                            f"EPSG:{validacao['srid_origem']} -> EPSG:{validacao['srid_destino']}"
                        )

                    if validacao["extras"]:
                        self.log(
                            "  ⚠️ Campos extras no GPKG serão ignorados na carga: "
                            + ", ".join(validacao["extras"])
                        )

                    if not validacao["valido"]:
                        partes = []
                        if validacao["faltantes"]:
                            partes.append("faltantes no GPKG: " + ", ".join(validacao["faltantes"]))
                        if validacao["divergencias_tipo"]:
                            partes.append("tipos incompatíveis sem conversão segura: " + " ; ".join(validacao["divergencias_tipo"]))
                        if validacao["erros_geometria"]:
                            partes.append("geometria incompatível: " + " ; ".join(validacao["erros_geometria"]))
                        msg = "Incompatibilidade de estrutura - " + " | ".join(partes)
                        self.log(f"  ❌ {msg}")
                        self.registrar_log_banco(conn, schema, destino, 0, 'ERRO', gpkg_path, msg)
                        resumo["erros"] += 1
                        self.emit_progress(i, total)
                        continue

                    if validacao.get("conversoes"):
                        descr = [f"{c['origem']} -> {c['destino']} ({c['raw_type_gpkg']} -> {c['data_type_pg']})" for c in validacao["conversoes"]]
                        self.log("  🔁 Conversões controladas necessárias: " + "; ".join(descr))
                        erros_conversao = self.validar_conversoes_gpkg(gpkg_path, nome_real_camada or origem, validacao["conversoes"])
                        if erros_conversao:
                            msg = "Valores não conversíveis no GPKG - " + " ; ".join(erros_conversao)
                            self.log(f"  ❌ {msg}")
                            self.registrar_log_banco(conn, schema, destino, 0, 'ERRO', gpkg_path, msg)
                            resumo["erros"] += 1
                            self.emit_progress(i, total)
                            continue
                        self.log("  ✅ Valores validados para conversão controlada.")

                    qtd = self.obter_contagem_gpkg(gpkg_path, nome_real_camada or origem)
                    sql_query = self.montar_sql_query_ogr(
                        nome_real_camada or origem,
                        validacao["campos_select"],
                        validacao["geometria_origem"],
                        validacao["geometria_destino"],
                        nome_arq,
                        revisao_num
                    )

                    self.log(f"  ✅ Validação concluída. Registros previstos: {qtd}")
                    self.log("  ⏳ Executando ogr2ogr...")

                    parametros_reprojecao = self.montar_parametros_reprojecao_ogr(validacao)
                    if parametros_reprojecao:
                        self.log(
                            f"  🗺️ SRID diferente identificado. Reprojetando no ogr2ogr: "
                            f"EPSG:{validacao['srid_origem']} -> EPSG:{validacao['srid_destino']}"
                        )

                    cmd_ogr = [
                        "ogr2ogr", "-append", "-f", "PostgreSQL", pg_dsn, gpkg_path,
                        *parametros_reprojecao,
                        "-nln", f"{schema}.{destino}",
                        "-dialect", "SQLite",
                        "-sql", sql_query,
                        "-nlt", validacao["nlt_ogr"],
                        "--config", "PG_USE_COPY", "YES"
                    ]

                    res_ogr = self.executar_comando_ogr(cmd_ogr)

                    if res_ogr.returncode == 0:
                        self.registrar_log_banco(conn, schema, destino, qtd, 'CONCLUIDO', gpkg_path,
                                                 'Atualização via script com validação previa de camada/tabela/campos/geometria, reprojecao SRID quando necessaria e conversões controladas e tratamento de primeiro valor em textos multivalorados para inteiros')
                        self.log(f"  ✅ Sucesso: {qtd} registros.")
                        resumo["concluidas"] += 1
                    else:
                        erro = res_ogr.stderr.strip() or res_ogr.stdout.strip()
                        self.registrar_log_banco(conn, schema, destino, qtd, 'ERRO', gpkg_path, erro[:1000])
                        self.log(f"  ❌ Erro no ogr2ogr: {erro[:500]}")
                        resumo["erros"] += 1

                except Exception as e:
                    msg = f"Erro na camada {origem}: {e}"
                    self.log(f"  ❌ {msg}")
                    self.registrar_log_banco(conn, schema, destino, 0, 'ERRO', gpkg_path, msg)
                    resumo["erros"] += 1

                self.emit_progress(i, total)

            self.log("\n✅ PROCESSAMENTO FINALIZADO")
            self.log(f"   Camadas concluidas: {resumo['concluidas']}")
            self.log(f"   Camadas ignoradas: {resumo['ignoradas']}")
            self.log(f"   Camadas com erro: {resumo['erros']}")

            return {
                "success": resumo["erros"] == 0,
                "resumo": resumo,
                "message": "Processamento finalizado." if resumo["erros"] == 0 else "Processamento finalizado com erros."
            }

        except Exception as e:
            self.log(f"\n❌ Erro fatal: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if conn is not None:
                conn.close()

