-- ================================================================
-- ETL SIMULATO — bank_data
-- Architettura 3-layer:
--   RAW      -> tabelle sorgenti già popolate (clienti, conti,
--               movimenti, reclami, pacchetti) in `bank_data`
--   STAGING  -> aggregazioni pre-calcolate per cliente (stg_*)
--   MART     -> tabella finale cliente_360 (dm_*)
--
-- Esegui TUTTO lo script in BigQuery Console: ricostruisce
-- idempotentemente staging + mart (CREATE OR REPLACE).
-- ================================================================


-- ================================================================
-- LAYER 1 — STAGING
--   Ogni tabella aggrega una sorgente a granularità cliente_id
-- ================================================================


-- ----------------------------------------------------------------
-- stg_cliente_conti
--   KPI sui conti correnti del cliente
-- ----------------------------------------------------------------
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.bank_data.stg_cliente_conti` AS
SELECT
  c.cliente_id,
  COUNT(*)                                    AS num_conti,
  COUNTIF(c.stato = 'ATTIVO')                 AS num_conti_attivi,
  SUM(c.saldo)                                AS saldo_totale,   -- include anche conti CHIUSO/BLOCCATO
  SUM(c.fido_accordato)                       AS fido_totale,
  MIN(c.data_apertura)                        AS data_primo_conto,
  MAX(c.data_apertura)                        AS data_ultimo_conto,
  DATE_DIFF(CURRENT_DATE(), MIN(c.data_apertura), YEAR) AS anni_anzianita
FROM `phrasal-method-484415-g7.bank_data.conti` AS c
GROUP BY c.cliente_id;


-- ----------------------------------------------------------------
-- stg_cliente_movimenti
--   KPI sui movimenti ultimi 12 mesi (per cliente via join conti)
-- ----------------------------------------------------------------
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.bank_data.stg_cliente_movimenti` AS
WITH mov_cli AS (
  SELECT
    co.cliente_id,
    m.*
  FROM `phrasal-method-484415-g7.bank_data.movimenti` AS m
  JOIN `phrasal-method-484415-g7.bank_data.conti`     AS co
    USING (conto_id)
  WHERE m.data_operazione >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
)
SELECT
  cliente_id,
  COUNT(*)                                         AS num_movimenti_12m,
  SUM(IF(importo > 0, importo, 0))                 AS tot_entrate_12m,
  SUM(IF(importo < 0, -importo, 0))                AS tot_uscite_12m, -- MODIFICATO: ora le uscite sono valori positivi
  SUM(importo)                                     AS saldo_netto_12m,
  AVG(importo)                                     AS importo_medio,
  COUNTIF(tipo_operazione = 'ACCREDITO_STIPENDIO') AS num_stipendi,
  SUM(IF(tipo_operazione = 'ACCREDITO_STIPENDIO', importo, 0)) AS tot_stipendi,
  COUNTIF(tipo_operazione = 'PAGAMENTO_CARTA')     AS num_pag_carta,
  COUNTIF(categoria = 'INVESTMENT')                AS num_investimenti,
  SUM(IF(categoria = 'INVESTMENT', -importo, 0)) AS tot_investimenti,
  MAX(data_operazione)                             AS data_ultimo_movimento
FROM mov_cli
GROUP BY cliente_id;


-- ----------------------------------------------------------------
-- stg_cliente_reclami
--   KPI sui reclami del cliente
-- ----------------------------------------------------------------
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.bank_data.stg_cliente_reclami` AS
SELECT
  cliente_id,
  COUNT(*)                                      AS num_reclami_totali,
  COUNTIF(stato = 'APERTO')                     AS num_reclami_aperti,
  COUNTIF(stato = 'IN_LAVORAZIONE')              AS num_reclami_in_lavorazione,
  COUNTIF(stato = 'CHIUSO')                     AS num_reclami_chiusi,
  COUNTIF(priorita IN ('ALTA','URGENTE'))        AS num_reclami_priorita_alta,
  COUNTIF(esito = 'ACCETTATO')                  AS num_accolti,
  COUNTIF(esito = 'RIFIUTATO')                  AS num_rifiutati,
  SUM(IFNULL(rimborso_importo, 0))              AS tot_rimborsi,
  AVG(soddisfazione)                            AS soddisfazione_media,
  MAX(data_apertura)                            AS data_ultimo_reclamo
FROM `phrasal-method-484415-g7.bank_data.reclami`
GROUP BY cliente_id;


-- ----------------------------------------------------------------
-- stg_cliente_categoria_spesa
--   Categoria di spesa top per ogni cliente (ultimi 12 mesi)
-- ----------------------------------------------------------------
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.bank_data.stg_cliente_categoria_spesa` AS
WITH spese_cat AS (
  SELECT
    co.cliente_id,
    m.categoria,
    SUM(-m.importo) AS tot_speso,
    ROW_NUMBER() OVER (
      PARTITION BY co.cliente_id
      ORDER BY SUM(-m.importo) DESC
    ) AS rn
  FROM `phrasal-method-484415-g7.bank_data.movimenti` AS m
  JOIN `phrasal-method-484415-g7.bank_data.conti`     AS co USING (conto_id)
  WHERE m.stato = 'COMPLETATO'
    AND m.importo < 0
    AND m.data_operazione >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
  GROUP BY co.cliente_id, m.categoria
)
SELECT cliente_id, categoria AS categoria_top, tot_speso
FROM spese_cat
WHERE rn = 1;


-- ================================================================
-- LAYER 2 — MART (tabella finale)
--   Customer 360: una riga per cliente con tutti i KPI derivati
-- ================================================================

CREATE OR REPLACE TABLE `phrasal-method-484415-g7.bank_data.dm_cliente_360` AS
SELECT
  -- Anagrafica
  cl.cliente_id,
  cl.nome,
  cl.cognome,
  cl.data_nascita,
  DATE_DIFF(CURRENT_DATE(), cl.data_nascita, YEAR) AS eta,
  cl.genere,
  cl.citta,
  cl.regione,
  cl.email,
  cl.stato                                         AS stato_cliente,

  -- Profilo commerciale
  cl.segmento,
  cl.pacchetto_id,
  p.nome                                           AS pacchetto_nome,
  p.canone_mensile,
  cl.data_apertura,
  cl.aum,
  cl.reddito_annuo,
  cl.score_credito,

  -- KPI conti
  co.num_conti,
  co.num_conti_attivi,
  co.saldo_totale,
  co.fido_totale,
  co.anni_anzianita,

  -- KPI movimenti 12m
  mv.num_movimenti_12m,
  mv.tot_entrate_12m,
  mv.tot_uscite_12m,
  mv.saldo_netto_12m,
  mv.num_stipendi,
  mv.tot_stipendi,
  mv.num_pag_carta,
  mv.num_investimenti,
  mv.tot_investimenti,
  mv.data_ultimo_movimento,
  cs.categoria_top                                 AS categoria_spesa_top,

  -- KPI reclami
  IFNULL(rc.num_reclami_totali, 0)                 AS num_reclami_totali,
  IFNULL(rc.num_reclami_aperti, 0)                 AS num_reclami_aperti,
  IFNULL(rc.num_reclami_priorita_alta, 0)          AS num_reclami_priorita_alta,
  IFNULL(rc.tot_rimborsi, 0)                       AS tot_rimborsi,
  rc.soddisfazione_media,

  -- Flag / segmentazioni derivate
  CASE
    WHEN cl.aum >= 500000                    THEN 'HIGH_VALUE'
    WHEN cl.aum >= 100000                    THEN 'MID_VALUE'
    WHEN cl.aum >= 10000                     THEN 'MASS_AFFLUENT'
    ELSE                                          'MASS'
  END                                              AS value_tier,

  CASE
    WHEN IFNULL(rc.num_reclami_priorita_alta,0) >= 2 THEN 'HIGH'
    WHEN IFNULL(rc.num_reclami_totali,0)        >= 3 THEN 'MEDIUM'
    WHEN IFNULL(rc.num_reclami_totali,0)        >= 1 THEN 'LOW'
    ELSE                                                 'NONE'
  END                                              AS churn_risk_reclami,

  CASE
    WHEN mv.data_ultimo_movimento IS NULL                                       THEN 'DORMANT'
    WHEN mv.data_ultimo_movimento < DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)  THEN 'INATTIVO'
    WHEN mv.num_movimenti_12m     >= 50                                         THEN 'MOLTO_ATTIVO'
    WHEN mv.num_movimenti_12m     >= 10                                         THEN 'ATTIVO'
    ELSE                                                                             'POCO_ATTIVO'
  END                                              AS engagement_status,

  CURRENT_TIMESTAMP()                              AS etl_loaded_at

FROM `phrasal-method-484415-g7.bank_data.clienti`              AS cl
LEFT JOIN `phrasal-method-484415-g7.bank_data.pacchetti`       AS p  ON cl.pacchetto_id = p.pacchetto_id
LEFT JOIN `phrasal-method-484415-g7.bank_data.stg_cliente_conti`          AS co USING (cliente_id)
LEFT JOIN `phrasal-method-484415-g7.bank_data.stg_cliente_movimenti`      AS mv USING (cliente_id)
LEFT JOIN `phrasal-method-484415-g7.bank_data.stg_cliente_reclami`        AS rc USING (cliente_id)
LEFT JOIN `phrasal-method-484415-g7.bank_data.stg_cliente_categoria_spesa` AS cs USING (cliente_id);


-- ================================================================
-- QUERY DI VERIFICA (opzionali)
-- ================================================================

-- Numero righe per layer
-- SELECT 'stg_cliente_conti'              AS tbl, COUNT(*) FROM `phrasal-method-484415-g7.bank_data.stg_cliente_conti`
-- UNION ALL SELECT 'stg_cliente_movimenti',       COUNT(*) FROM `phrasal-method-484415-g7.bank_data.stg_cliente_movimenti`
-- UNION ALL SELECT 'stg_cliente_reclami',         COUNT(*) FROM `phrasal-method-444415-g7.bank_data.stg_cliente_reclami`
-- UNION ALL SELECT 'dm_cliente_360',              COUNT(*) FROM `phrasal-method-484415-g7.bank_data.dm_cliente_360`;

-- Top 10 clienti per AUM con engagement e churn risk
-- SELECT cliente_id, nome, cognome, segmento, value_tier, aum,
--        engagement_status, churn_risk_reclami, num_reclami_totali
-- FROM `phrasal-method-484415-g7.bank_data.dm_cliente_360`
-- ORDER BY aum DESC
-- LIMIT 10;
