-- ================================================================
-- ETL 2 — PACCHETTO PERFORMANCE
-- Obiettivo: misurare la performance commerciale di ogni pacchetto
--            (clienti, AUM, revenue ricorrente, reclami).
--
-- Architettura:
--   RAW      -> clienti, conti, reclami, pacchetti
--   STAGING  -> stg_pacchetto_clienti, stg_pacchetto_reclami
--   MART     -> dm_pacchetto_performance (una riga per pacchetto)
-- ================================================================


-- ----------------------------------------------------------------
-- stg_pacchetto_clienti
--   KPI sulla base clienti per ogni pacchetto
-- ----------------------------------------------------------------
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.bank_data.stg_pacchetto_clienti` AS
SELECT
  pacchetto_id,
  COUNT(*)                                    AS num_clienti_totali,
  COUNTIF(stato <> 'CHIUSO')                  AS num_clienti_attivi,
  COUNTIF(stato = 'CHIUSO')                   AS num_clienti_churned,
  SUM(aum)                                    AS aum_totale,
  AVG(aum)                                    AS aum_medio,
  SUM(reddito_annuo)                          AS reddito_totale,
  AVG(score_credito)                          AS score_medio,
  MIN(data_apertura)                          AS data_prima_sottoscrizione,
  MAX(data_apertura)                          AS data_ultima_sottoscrizione
FROM `phrasal-method-484415-g7.bank_data.clienti`
WHERE pacchetto_id IS NOT NULL
GROUP BY pacchetto_id;


-- ----------------------------------------------------------------
-- stg_pacchetto_reclami
--   KPI sui reclami generati dai clienti di ogni pacchetto
-- ----------------------------------------------------------------
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.bank_data.stg_pacchetto_reclami` AS
SELECT
  cl.pacchetto_id,
  COUNT(r.reclamo_id)                            AS num_reclami,
  COUNTIF(r.priorita IN ('ALTA','URGENTE'))      AS num_reclami_critici,
  SUM(IFNULL(r.rimborso_importo, 0))             AS tot_rimborsi_erogati,
  AVG(r.soddisfazione)                           AS soddisfazione_media,
  SAFE_DIVIDE(COUNTIF(r.esito = 'ACCETTATO'),
              COUNTIF(r.stato = 'CHIUSO'))       AS tasso_accoglimento
FROM `phrasal-method-484415-g7.bank_data.clienti` AS cl
LEFT JOIN `phrasal-method-484415-g7.bank_data.reclami` AS r USING (cliente_id)
WHERE cl.pacchetto_id IS NOT NULL
GROUP BY cl.pacchetto_id;


-- ----------------------------------------------------------------
-- dm_pacchetto_performance (MART)
-- ----------------------------------------------------------------
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.bank_data.dm_pacchetto_performance` AS
SELECT
  p.pacchetto_id,
  p.nome                                             AS pacchetto_nome,
  p.segmento,
  p.canone_mensile,
  p.cashback_pct,
  p.data_lancio,
  DATE_DIFF(CURRENT_DATE(), p.data_lancio, MONTH)    AS mesi_sul_mercato,

  -- KPI base clienti
  IFNULL(c.num_clienti_totali, 0)                    AS num_clienti_totali,
  IFNULL(c.num_clienti_attivi, 0)                    AS num_clienti_attivi,
  IFNULL(c.num_clienti_churned, 0)                   AS num_clienti_churned,
  SAFE_DIVIDE(IFNULL(c.num_clienti_churned, 0),
              c.num_clienti_totali)                  AS churn_rate,
  IFNULL(c.aum_totale, 0)                            AS aum_totale,
  IFNULL(c.aum_medio, 0)                             AS aum_medio,
  IFNULL(c.score_medio, 0)                           AS score_credito_medio,

  -- Revenue stimata da canone ricorrente
  IFNULL(c.num_clienti_totali, 0) * p.canone_mensile            AS revenue_mensile_stimata,
  IFNULL(c.num_clienti_totali, 0) * p.canone_mensile * 12       AS revenue_annua_stimata,

  -- KPI reclami
  IFNULL(r.num_reclami, 0)                           AS num_reclami,
  IFNULL(r.num_reclami_critici, 0)                   AS num_reclami_critici,
  SAFE_DIVIDE(IFNULL(r.num_reclami, 0), c.num_clienti_totali)   AS reclami_per_cliente,
  IFNULL(r.tot_rimborsi_erogati, 0)                  AS tot_rimborsi_erogati,
  r.soddisfazione_media,
  IFNULL(r.tasso_accoglimento, 0)                    AS tasso_accoglimento,

  -- Classificazione performance
  CASE
    WHEN IFNULL(c.num_clienti_attivi, 0) >= 30 AND IFNULL(r.soddisfazione_media, 0) >= 3.5 THEN 'STAR'
    WHEN IFNULL(c.num_clienti_attivi, 0) >= 15                                  THEN 'GROWING'
    WHEN IFNULL(c.num_clienti_attivi, 0) >= 5                                   THEN 'NICHE'
    ELSE                                                                  'UNDERPERFORMING'
  END                                                AS performance_tier,

  CURRENT_TIMESTAMP()                                AS etl_loaded_at
FROM `phrasal-method-484415-g7.bank_data.pacchetti`              AS p
LEFT JOIN `phrasal-method-484415-g7.bank_data.stg_pacchetto_clienti` AS c USING (pacchetto_id)
LEFT JOIN `phrasal-method-484415-g7.bank_data.stg_pacchetto_reclami` AS r USING (pacchetto_id);
