-- ============================================================
-- FILE: 04_mart_etl.sql
-- PROGETTO: ETL Bancario - Banca Italiana S.p.A.
-- LAYER: MART - Tabelle analitiche per reporting e ALM
-- ============================================================


-- ============================================================
-- TABLE: banca_raw.saldi_correnti
-- Saldo attuale di ogni conto come dato di sintesi per reporting.
-- Una riga per conto. Dipende da stg_conti e stg_movimenti.
-- =========...