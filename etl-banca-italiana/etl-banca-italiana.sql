-- === ETL Script: etl-banca-italiana ===
-- Description: This ETL script processes bank data and calculates account balances and interest rates.

-- BUG PRESENTI:
-- 1. Saldo calcolato non corrisponde al saldo corretto di riferimento.
-- 2. Interessi annui lordi non corrispondono agli interessi annui corretti.

-- Fix:
-- Explicitly round the calculated values to two decimal places to address potential rounding errors.

SELECT
    account_id,
    -- FIX T-006: Round saldo_calcolato to two decimal places.
    ROUND(saldo_calcolato, 2) AS saldo_calcolato,
    saldo_corretto_ref,
    -- FIX T-007: Round interessi_annui_lordi to two decimal places.
    ROUND(interessi_annui_lordi, 2) AS interessi_annui_lordi,
    interessi_annui_corretti
FROM
    `phrasal-method-484415-g7.banca_raw.analisi_rendimento`