-- ============================================================
-- FILE: 04_mart_etl.sql
-- PROGETTO: ETL Bancario - Banca Italiana S.p.A.
-- LAYER: MART - Tabelle analitiche per reporting e ALM
-- ============================================================


-- ============================================================
-- TABLE: banca_mart.saldi_correnti
-- Saldo attuale di ogni conto come dato di sintesi per reporting.
-- Una riga per conto. Dipende da stg_conti e stg_movimenti.
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_mart.saldi_correnti` AS

WITH movimenti_per_conto AS (
  SELECT
    id_conto,
    COUNT(*)                                     AS num_movimenti,
    COUNT(CASE WHEN tipo_movimento = 'ACCREDITO' THEN 1 END) AS num_accrediti,
    COUNT(CASE WHEN tipo_movimento = 'ADDEBITO'  THEN 1 END) AS num_addebiti,
    SUM(CASE WHEN tipo_movimento = 'ACCREDITO' THEN importo ELSE 0 END) AS totale_accrediti,
    SUM(CASE WHEN tipo_movimento = 'ADDEBITO'  THEN importo ELSE 0 END) AS totale_addebiti,
    SUM(importo)                                 AS variazione_netta,
    MAX(data_contabile)                          AS ultimo_movimento_ts,
    MAX(data_valuta)                             AS ultima_data_valuta
  FROM `phrasal-method-484415-g7.banca_staging.stg_movimenti`
  GROUP BY id_conto
)

SELECT
  c.id_conto,
  c.iban,
  c.tipo_conto,
  c.id_filiale,
  f.nome_filiale,
  f.citta                                        AS citta_filiale,
  c.data_apertura,
  c.stato,
  c.fido_accordato,
  c.saldo_iniziale,
  ROUND(c.saldo_iniziale + COALESCE(m.variazione_netta, 0), 2)
                                                 AS saldo_calcolato,
  m.totale_accrediti,
  m.totale_addebiti,
  ROUND(m.totale_accrediti - m.totale_addebiti, 2) AS variazione_netta_corretta,
  ROUND(c.saldo_iniziale + COALESCE(m.totale_accrediti, 0) - COALESCE(m.totale_addebiti, 0), 2)
                                                 AS saldo_corretto_ref,
  m.num_movimenti,
  m.num_accrediti,
  m.num_addebiti,
  m.ultimo_movimento_ts,
  m.ultima_data_valuta,

  -- Dati anagrafici del titolare del conto
  k.nome                                         AS nome_titolare,
  k.cognome                                      AS cognome_titolare,
  k.codice_fiscale                               AS cf_titolare,
  k.segmento                                     AS segmento_cliente,
  k.rating_interno                               AS rating_cliente,
  k.data_acquisizione                            AS cliente_dal,

  CURRENT_TIMESTAMP()                            AS _loaded_at

FROM `phrasal-method-484415-g7.banca_staging.stg_conti` c

LEFT JOIN movimenti_per_conto m
  ON c.id_conto = m.id_conto

LEFT JOIN `phrasal-method-484415-g7.banca_staging.stg_filiali` f
  ON c.id_filiale = f.id_filiale

LEFT JOIN `phrasal-method-484415-g7.banca_staging.stg_clienti` k
  ON c.id_cliente = k.codice_fiscale;


-- ============================================================
-- TABLE: banca_mart.metriche_cliente
-- KPI di portafoglio per cliente: conti, saldo totale, operatività.
-- Dipende da: stg_clienti, stg_conti, saldi_correnti, stg_movimenti
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_mart.metriche_cliente` AS
SELECT
  k.id_cliente,
  k.codice_fiscale,
  k.nome,
  k.cognome,
  k.segmento,
  k.rating_interno,
  k.filiale_gestrice,
  k.data_acquisizione,
  DATE_DIFF(CURRENT_DATE(), k.data_acquisizione, YEAR)  AS anni_come_cliente,

  COUNT(DISTINCT c.id_conto)                     AS num_conti,
  SUM(sc.saldo_calcolato)                        AS saldo_portafoglio_totale,
  SUM(sc.saldo_corretto_ref)                     AS saldo_portafoglio_corr_ref,
  COUNT(m.id_movimento)                          AS num_operazioni_totali,
  SUM(c.fido_accordato)                          AS fido_totale,
  MAX(m.data_contabile)                          AS data_ultima_operazione,
  MIN(m.data_contabile)                          AS data_prima_operazione,
  DATE_DIFF(CURRENT_DATE(), MAX(DATE(m.data_contabile)), DAY) AS giorni_inattivita,

  CURRENT_TIMESTAMP()                            AS _loaded_at

FROM `phrasal-method-484415-g7.banca_staging.stg_clienti` k

LEFT JOIN `phrasal-method-484415-g7.banca_staging.stg_conti` c
  ON k.id_cliente = c.id_cliente

LEFT JOIN `phrasal-method-484415-g7.banca_mart.saldi_correnti` sc
  ON c.id_conto = sc.id_conto

LEFT JOIN `phrasal-method-484415-g7.banca_staging.stg_movimenti` m
  ON c.id_conto = m.id_conto

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;


-- ============================================================
-- TABLE: banca_mart.analisi_rendimento
-- Stima degli interessi maturabili per i conti fruttiferi.
-- Conti inclusi: CONTO_RISPARMIO e CONTO_DEPOSITO
-- Dipende da: stg_conti, saldi_correnti, stg_tassi_interesse
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_mart.analisi_rendimento` AS
SELECT
  c.id_conto,
  c.iban,
  c.tipo_conto,
  c.id_cliente,
  c.data_apertura,
  sc.saldo_calcolato                             AS saldo_attuale,
  sc.saldo_corretto_ref                          AS saldo_corretto_ref,
  t.id_tasso,
  t.tasso_annuo,
  t.data_fine                                    AS scadenza_tasso,
  DATE_DIFF(
    COALESCE(t.data_fine, DATE_ADD(CURRENT_DATE(), INTERVAL 1 YEAR)),
    CURRENT_DATE(),
    DAY
  )                                              AS giorni_residui,

  ROUND(sc.saldo_calcolato * t.tasso_annuo, 2)  AS interessi_annui_lordi,
  ROUND(sc.saldo_calcolato * t.tasso_annuo / 12, 2)
                                                AS interessi_mensili_lordi,
  ROUND(sc.saldo_calcolato * (t.tasso_annuo / 100), 2)
                                                AS interessi_annui_corretti,
  ROUND(sc.saldo_calcolato * (t.tasso_annuo / 100) * 0.74, 2)
                                                AS interessi_annui_netti_ref,

  CURRENT_TIMESTAMP()                           AS _loaded_at

FROM `phrasal-method-484415-g7.banca_staging.stg_conti` c

JOIN `phrasal-method-484415-g7.banca_mart.saldi_correnti` sc
  ON c.id_conto = sc.id_conto

JOIN `phrasal-method-484415-g7.banca_staging.stg_tassi_interesse` t
  ON c.tipo_conto = t.tipo_prodotto

WHERE c.tipo_conto IN ('CONTO_RISPARMIO', 'CONTO_DEPOSITO')
  AND c.stato = 'ATTIVO'
  AND sc.saldo_calcolato > 0;
