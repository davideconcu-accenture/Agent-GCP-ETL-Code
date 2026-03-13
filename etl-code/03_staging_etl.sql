-- ============================================================
-- FILE: 03_staging_etl.sql
-- PROGETTO: ETL Bancario - Banca Italiana S.p.A.
-- LAYER: STAGING - Pulizia, deduplicazione e normalizzazione
-- ============================================================
--
-- Questo layer ha il compito di:
--   1. Deduplicare i record duplicati
--   2. Normalizzare formati (trim, case, date)
--   3. Filtrare record non validi
--   4. Calcolare campi derivati di base
--   5. Aggiungere colonne di audit (_loaded_at, _source)
--
-- ============================================================


-- ============================================================
-- TABLE: banca_staging.stg_clienti
-- Deduplicazione su id_cliente; si mantiene la riga con
-- data_acquisizione più antica (cliente storico prioritario).
-- In caso di parità, si tiene la riga con rating migliore
-- (ordine alfabetico AAA > AA > A > BBB > ..., qui usiamo ASC).
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_staging.stg_clienti` AS
SELECT
  id_cliente,
  UPPER(TRIM(codice_fiscale))        AS codice_fiscale,
  TRIM(nome)                         AS nome,
  UPPER(TRIM(cognome))               AS cognome,
  data_nascita,
  TRIM(comune_nascita)               AS comune_nascita,
  TRIM(indirizzo)                    AS indirizzo,
  TRIM(cap)                          AS cap,
  TRIM(citta_residenza)              AS citta_residenza,
  UPPER(TRIM(provincia))             AS provincia,
  UPPER(TRIM(segmento))              AS segmento,
  UPPER(TRIM(rating_interno))        AS rating_interno,
  filiale_gestrice,
  data_acquisizione,
  UPPER(TRIM(stato_cliente))         AS stato_cliente,
  _loaded_at
FROM (
  SELECT
    *,
    CURRENT_TIMESTAMP()              AS _loaded_at,
    ROW_NUMBER() OVER (
      PARTITION BY id_cliente
      ORDER BY data_acquisizione ASC, rating_interno ASC
    )                                AS _rn
  FROM `phrasal-method-484415-g7.banca_raw.clienti`
  WHERE id_cliente IS NOT NULL
    AND codice_fiscale IS NOT NULL
)
WHERE _rn = 1;


-- ============================================================
-- TABLE: banca_staging.stg_filiali
-- Pulizia semplice.
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_staging.stg_filiali` AS
SELECT
  id_filiale,
  TRIM(nome_filiale)                 AS nome_filiale,
  TRIM(indirizzo)                    AS indirizzo,
  TRIM(citta)                        AS citta,
  UPPER(TRIM(provincia))             AS provincia,
  TRIM(regione)                      AS regione,
  TRIM(codice_abi)                   AS codice_abi,
  TRIM(responsabile)                 AS responsabile,
  num_dipendenti,
  data_apertura,
  CURRENT_TIMESTAMP()                AS _loaded_at
FROM `phrasal-method-484415-g7.banca_raw.filiali`
WHERE id_filiale IS NOT NULL;


-- ============================================================
-- TABLE: banca_staging.stg_conti
-- Pulizia e normalizzazione.
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_staging.stg_conti` AS
SELECT
  id_conto,
  id_cliente,
  UPPER(TRIM(iban))                  AS iban,
  UPPER(TRIM(tipo_conto))            AS tipo_conto,
  id_filiale,
  data_apertura,
  data_chiusura,
  saldo_iniziale,
  UPPER(TRIM(stato))                 AS stato,
  COALESCE(fido_accordato, 0)        AS fido_accordato,
  CASE
    WHEN data_chiusura IS NULL AND UPPER(TRIM(stato)) = 'ATTIVO' THEN TRUE
    ELSE FALSE
  END                                AS is_attivo,
  CURRENT_TIMESTAMP()                AS _loaded_at
FROM `phrasal-method-484415-g7.banca_raw.conti`
WHERE id_conto IS NOT NULL
  AND id_cliente IS NOT NULL
  AND saldo_iniziale IS NOT NULL;


-- ============================================================
-- TABLE: banca_staging.stg_tassi_interesse
-- Solo tassi attualmente in vigore.
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_staging.stg_tassi_interesse` AS
SELECT
  id_tasso,
  UPPER(TRIM(tipo_prodotto))         AS tipo_prodotto,
  tasso_annuo,
  data_inizio,
  data_fine,
  TRIM(note)                         AS note,
  CURRENT_TIMESTAMP()                AS _loaded_at
FROM `phrasal-method-484415-g7.banca_raw.tassi_interesse`
WHERE id_tasso IS NOT NULL
  AND tasso_annuo > 0
  AND data_inizio <= CURRENT_DATE()
  AND (data_fine IS NULL OR data_fine >= CURRENT_DATE());


-- ============================================================
-- TABLE: banca_staging.stg_movimenti
-- Deduplicazione, normalizzazione e calcolo importo con segno.
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_staging.stg_movimenti` AS
SELECT
  id_movimento,
  id_conto,
  data_contabile,
  data_valuta,
  importo,
  UPPER(TRIM(tipo_movimento))                         AS tipo_movimento,
  TRIM(causale)                                       AS causale,
  codice_causale,
  UPPER(TRIM(canale))                                 AS canale,
  controparte_iban,
  CASE
    WHEN UPPER(TRIM(tipo_movimento)) = 'ACCREDITO' THEN  importo
    WHEN UPPER(TRIM(tipo_movimento)) = 'ADDEBITO'  THEN -importo
    ELSE NULL
  END                                                 AS importo_con_segno,
  EXTRACT(YEAR  FROM data_contabile)                  AS anno_contabile,
  EXTRACT(MONTH FROM data_contabile)                  AS mese_contabile,
  _loaded_at
FROM (
  SELECT
    *,
    CURRENT_TIMESTAMP()   AS _loaded_at,
    ROW_NUMBER() OVER (
      PARTITION BY id_conto, DATE(data_contabile)
      ORDER BY data_contabile DESC
    )                     AS _rn
  FROM `phrasal-method-484415-g7.banca_raw.movimenti`
  WHERE id_movimento IS NOT NULL
    AND id_conto     IS NOT NULL
    AND importo      > 0
    AND tipo_movimento IN ('ACCREDITO', 'ADDEBITO')
)
WHERE _rn = 1;
