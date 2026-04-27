-- ================================================================
-- ETL 3 — MOVIMENTI MENSILI PER SEGMENTO
-- Obiettivo: serie temporale dei movimenti aggregati per mese e
--            segmento commerciale del cliente (utile per trend/grafici).
--
-- Architettura:
--   RAW      -> movimenti, conti, clienti
--   STAGING  -> stg_movimenti_arricchiti (denormalizzazione)
--   MART     -> dm_movimenti_mensili_segmento (time series)
-- ================================================================


-- ----------------------------------------------------------------
-- stg_movimenti_arricchiti
--   Denormalizza: movimento + cliente + segmento in un'unica riga
-- ----------------------------------------------------------------
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.bank_data.stg_movimenti_arricchiti` AS
SELECT
  m.movimento_id,
  m.data_operazione,
  m.tipo_operazione,
  m.categoria,
  m.canale,
  m.importo,
  m.stato,
  co.conto_id,
  cl.cliente_id,
  cl.segmento,
  cl.regione,
  cl.pacchetto_id
FROM `phrasal-method-484415-g7.bank_data.movimenti` AS m
JOIN `phrasal-method-484415-g7.bank_data.conti`     AS co USING (conto_id)
JOIN `phrasal-method-484415-g7.bank_data.clienti`   AS cl USING (cliente_id)
WHERE m.stato = 'COMPLETATO';


-- ----------------------------------------------------------------
-- dm_movimenti_mensili_segmento (MART)
--   Serie temporale: una riga per (anno, mese, segmento)
-- ----------------------------------------------------------------
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.bank_data.dm_movimenti_mensili_segmento` AS
SELECT
  EXTRACT(YEAR FROM data_operazione)                AS anno, -- FIX: Aggiunto anno
  EXTRACT(MONTH FROM data_operazione)               AS mese,
  segmento,

  COUNT(*)                                          AS num_movimenti,
  COUNT(DISTINCT cliente_id)                        AS num_clienti_attivi,

  -- Flussi
  SUM(IF(importo > 0, importo, 0))                  AS tot_entrate,
  SUM(IF(importo < 0, importo, 0))                  AS tot_uscite,
  SUM(importo)                                      AS flusso_netto,

  -- Medie
  AVG(importo)                                      AS importo_medio,
  AVG(ABS(importo))                                 AS valore_assoluto_medio,

  -- Breakdown per tipo
  COUNTIF(tipo_operazione = 'ACCREDITO_STIPENDIO')  AS num_stipendi,
  COUNTIF(tipo_operazione = 'PAGAMENTO_CARTA')      AS num_pag_carta,
  COUNTIF(tipo_operazione = 'BONIFICO_IN')          AS num_bonifici_in,
  COUNTIF(tipo_operazione = 'BONIFICO_OUT')         AS num_bonifici_out,
  COUNTIF(tipo_operazione = 'PRELIEVO')             AS num_prelievi,

  -- Breakdown per canale
  COUNTIF(canale = 'APP')                           AS num_ops_app,
  COUNTIF(canale = 'WEB')                           AS num_ops_web,
  COUNTIF(canale = 'POS')                           AS num_ops_pos,
  COUNTIF(canale = 'ATM')                           AS num_ops_atm,
  COUNTIF(canale = 'BANCA')                         AS num_ops_banca, -- FIX: Sostituito 'FILIALE' con 'BANCA'

  -- Mix canale digitale
  SAFE_DIVIDE(COUNTIF(canale IN ('APP','WEB')),
              COUNT(*))                             AS pct_digitale,

  CURRENT_TIMESTAMP()                               AS etl_loaded_at
FROM `phrasal-method-484415-g7.bank_data.stg_movimenti_arricchiti`
GROUP BY anno, mese, segmento -- FIX: Aggiunto anno al group by
ORDER BY anno, mese, segmento; -- FIX: Aggiunto anno all'order by
