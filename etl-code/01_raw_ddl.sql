-- ============================================================
-- FILE: 01_raw_ddl.sql
-- PROGETTO: ETL Bancario - Banca Italiana S.p.A.
-- LAYER: RAW (dati grezzi dai sistemi sorgente della banca)
-- ============================================================
--
-- ARCHITETTURA SORGENTI:
--   - Sistema Core Banking (Temenos T24): clienti, conti, movimenti
--   - Sistema CRM: anagrafica clienti estesa
--   - Sistema ALM (Asset Liability Management): tassi e prodotti
--
-- SETUP PREREQUISITO:
--   Sostituire 'phrasal-method-484415-g7' con il proprio GCP Project ID.
--   Eseguire da BigQuery CLI o Console:
--
--   bq mk --dataset --location=EU phrasal-method-484415-g7:banca_raw
--   bq mk --dataset --location=EU phrasal-method-484415-g7:banca_staging
--   bq mk --dataset --location=EU phrasal-method-484415-g7:banca_mart
--
-- ============================================================


-- ============================================================
-- TABLE: banca_raw.clienti
-- Anagrafica clienti esportata dal Core Banking.
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_raw.clienti` (
  id_cliente        STRING   NOT NULL OPTIONS(description='Codice univoco cliente (es. CLI001)'),
  codice_fiscale    STRING            OPTIONS(description='Codice Fiscale - 16 caratteri alfanumerici'),
  nome              STRING,
  cognome           STRING,
  data_nascita      DATE              OPTIONS(description='Data di nascita'),
  comune_nascita    STRING,
  indirizzo         STRING,
  cap               STRING,
  citta_residenza   STRING,
  provincia         STRING,
  segmento          STRING            OPTIONS(description='Segmento: RETAIL | PRIVATE | CORPORATE'),
  rating_interno    STRING            OPTIONS(description='Rating creditizio interno: AAA | AA | A | BBB | BB | B | CCC'),
  filiale_gestrice  STRING            OPTIONS(description='FK verso filiali.id_filiale'),
  data_acquisizione DATE              OPTIONS(description='Data di ingresso come cliente'),
  stato_cliente     STRING            OPTIONS(description='ATTIVO | INATTIVO | CHIUSO')
);


-- ============================================================
-- TABLE: banca_raw.filiali
-- Anagrafica delle filiali bancarie.
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_raw.filiali` (
  id_filiale      STRING   NOT NULL OPTIONS(description='Codice filiale (es. FIL001)'),
  nome_filiale    STRING,
  indirizzo       STRING,
  citta           STRING,
  provincia       STRING,
  regione         STRING,
  codice_abi      STRING            OPTIONS(description='Codice ABI della filiale'),
  responsabile    STRING            OPTIONS(description='Nome e cognome del direttore'),
  num_dipendenti  INT64,
  data_apertura   DATE
);


-- ============================================================
-- TABLE: banca_raw.conti
-- Conti bancari intestati ai clienti.
-- Tipi: CONTO_CORRENTE | CONTO_RISPARMIO | CONTO_DEPOSITO | CONTO_CORRENTE_BUSINESS
-- Un cliente può avere più conti.
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_raw.conti` (
  id_conto          STRING   NOT NULL OPTIONS(description='ID interno conto (es. CC001)'),
  id_cliente        STRING            OPTIONS(description='FK verso clienti.id_cliente'),
  iban              STRING            OPTIONS(description='IBAN completo (27 caratteri IT)'),
  tipo_conto        STRING            OPTIONS(description='CONTO_CORRENTE | CONTO_RISPARMIO | CONTO_DEPOSITO | CONTO_CORRENTE_BUSINESS'),
  id_filiale        STRING            OPTIONS(description='Filiale di apertura'),
  data_apertura     DATE,
  data_chiusura     DATE              OPTIONS(description='NULL se il conto è ancora aperto'),
  saldo_iniziale    NUMERIC           OPTIONS(description='Saldo al momento dell apertura o al 01/01/2025 (EUR)'),
  stato             STRING            OPTIONS(description='ATTIVO | BLOCCATO | CHIUSO'),
  fido_accordato    NUMERIC           OPTIONS(description='Fido di cassa accordato (EUR), NULL se non applicabile'),
  note              STRING
);


-- ============================================================
-- TABLE: banca_raw.movimenti
-- Movimenti contabili sui conti (dare/avere).
--
-- CONVENZIONE IMPORTI:
--   tipo_movimento indica la direzione del movimento:
--     ACCREDITO = entrata (saldo aumenta)
--     ADDEBITO  = uscita (saldo diminuisce)
--   Il campo 'importo' è SEMPRE POSITIVO.
--   Il segno deve essere applicato in base a tipo_movimento.
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_raw.movimenti` (
  id_movimento      STRING    NOT NULL OPTIONS(description='ID univoco movimento'),
  id_conto          STRING    NOT NULL OPTIONS(description='FK verso conti.id_conto'),
  data_contabile    TIMESTAMP          OPTIONS(description='Data e ora di contabilizzazione (UTC)'),
  data_valuta       DATE               OPTIONS(description='Data valuta (può differire dalla data contabile)'),
  importo           NUMERIC            OPTIONS(description='Importo in valore assoluto (sempre positivo, EUR)'),
  tipo_movimento    STRING             OPTIONS(description='ACCREDITO | ADDEBITO'),
  causale           STRING             OPTIONS(description='Descrizione del movimento'),
  codice_causale    STRING             OPTIONS(description='Codice ABI causale (es. 48=Stipendio, 50=Bonifico)'),
  canale            STRING             OPTIONS(description='SPORTELLO | INTERNET_BANKING | APP | ATM | SEPA | SWIFT'),
  controparte_iban  STRING             OPTIONS(description='IBAN della controparte (opzionale)'),
  note_operatore    STRING
);


-- ============================================================
-- TABLE: banca_raw.tassi_interesse
-- Tassi di interesse applicati per tipologia di prodotto.
-- Gestito dal sistema ALM della banca.
-- Per ogni tipo_prodotto esiste un solo tasso attivo alla volta
-- (le date di validità non si sovrappongono).
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_raw.tassi_interesse` (
  id_tasso        STRING   NOT NULL OPTIONS(description='ID tasso (es. TASSO001)'),
  tipo_prodotto   STRING            OPTIONS(description='Tipo conto a cui si applica il tasso'),
  tasso_annuo     NUMERIC           OPTIONS(description='Tasso annuo in percentuale: 1.5 significa 1.5% NON 150%'),
  data_inizio     DATE              OPTIONS(description='Inizio validità tasso'),
  data_fine       DATE              OPTIONS(description='Fine validità tasso (NULL = ancora in vigore)'),
  note            STRING
);
