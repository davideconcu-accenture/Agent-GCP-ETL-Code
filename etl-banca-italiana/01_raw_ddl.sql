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
--   (tutti i layer raw, staging e mart sono nello stesso dataset banca_raw)
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


-- ============================================================
-- TABLE: banca_raw.carte
-- Carte di debito e credito associate ai conti.
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_raw.carte` (
  id_carta                STRING   NOT NULL OPTIONS(description='ID univoco carta (es. CAR001)'),
  id_conto                STRING            OPTIONS(description='FK verso conti.id_conto - conto di addebito'),
  id_cliente              STRING            OPTIONS(description='FK verso clienti.id_cliente - intestatario'),
  tipo_carta              STRING            OPTIONS(description='DEBITO | CREDITO | PREPAGATA'),
  circuito                STRING            OPTIONS(description='VISA | MASTERCARD | AMEX | MAESTRO'),
  numero_carte_cifrato    STRING            OPTIONS(description='Ultimi 4 cifre della carta (es. **** **** **** 1234)'),
  data_emissione          DATE              OPTIONS(description='Data emissione carta'),
  data_scadenza           DATE              OPTIONS(description='Data scadenza carta (MM/AAAA)'),
  stato                   STRING            OPTIONS(description='ATTIVA | BLOCCATA | SCADUTA | CANCELLATA'),
  plafond_mensile         NUMERIC           OPTIONS(description='Plafond mensile carta di credito (EUR), NULL per debito'),
  utilizzo_mese_corrente  NUMERIC           OPTIONS(description='Utilizzo corrente del plafond (EUR)'),
  canale_blocco           STRING            OPTIONS(description='APP | FILIALE | TELEFONO | NULL se non bloccata'),
  data_blocco             DATE              OPTIONS(description='Data blocco carta, NULL se non bloccata'),
  motivo_blocco           STRING            OPTIONS(description='SMARRIMENTO | FURTO | FRODE | RICHIESTA_CLIENTE | NULL'),
  contactless             BOOL              OPTIONS(description='Abilita pagamenti contactless'),
  pin_tentativi_falliti   INT64             OPTIONS(description='Numero tentativi PIN falliti (max 3 prima del blocco)')
);


-- ============================================================
-- TABLE: banca_raw.pacchetti
-- Pacchetti di prodotti/servizi offerti dalla banca.
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_raw.pacchetti` (
  id_pacchetto            STRING   NOT NULL OPTIONS(description='Codice univoco pacchetto (es. PKG001)'),
  nome_pacchetto          STRING            OPTIONS(description='Nome commerciale del pacchetto'),
  segmento_target         STRING            OPTIONS(description='Segmento target: RETAIL | PRIVATE | CORPORATE | ALL'),
  canone_mensile          NUMERIC           OPTIONS(description='Canone mensile in EUR (0 = gratuito)'),
  commissione_prelievo    NUMERIC           OPTIONS(description='Commissione per prelievo ATM (EUR)'),
  num_bonifici_gratuiti   INT64             OPTIONS(description='Numero di bonifici gratuiti al mese'),
  limite_pagamenti_pos    NUMERIC           OPTIONS(description='Limite giornaliero pagamenti POS (EUR)'),
  internet_banking        BOOL              OPTIONS(description='Servizio di internet banking incluso'),
  mobile_app              BOOL              OPTIONS(description='App mobile inclusa'),
  carta_inclusa           STRING            OPTIONS(description='Tipo carta inclusa: NESSUNA | DEBITO | CREDITO'),
  cassetta_sicurezza      BOOL              OPTIONS(description='Cassetta di sicurezza inclusa'),
  data_lancio             DATE              OPTIONS(description='Data inizio commercializzazione'),
  data_fine_vendita       DATE              OPTIONS(description='Data fine vendita (NULL = ancora disponibile)'),
  note                    STRING
);


-- ============================================================
-- TABLE: banca_raw.contratti_pacchetto
-- Associazione cliente-pacchetto (contratti attivi e storici).
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_raw.contratti_pacchetto` (
  id_contratto            STRING   NOT NULL OPTIONS(description='ID univoco contratto (es. CNT001)'),
  id_cliente              STRING            OPTIONS(description='FK verso clienti.id_cliente'),
  id_pacchetto            STRING            OPTIONS(description='FK verso pacchetti.id_pacchetto'),
  id_conto_principale     STRING            OPTIONS(description='FK verso conti.id_conto - conto associato'),
  data_inizio             DATE              OPTIONS(description='Data inizio contratto'),
  data_fine               DATE              OPTIONS(description='Data fine contratto (NULL = attivo)'),
  stato                   STRING            OPTIONS(description='ATTIVO | SOSPESO | CHIUSO'),
  canale_sottoscrizione   STRING            OPTIONS(description='FILIALE | INTERNET_BANKING | APP | PROMOTORE'),
  sconto_percentuale      NUMERIC           OPTIONS(description='Sconto applicato sul canone (%)'),
  note_operatore          STRING
);


-- ============================================================
-- TABLE: banca_raw.reclami
-- Reclami e segnalazioni dei clienti.
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_raw.reclami` (
  id_reclamo          STRING    NOT NULL OPTIONS(description='ID univoco reclamo (es. REC001)'),
  id_cliente          STRING             OPTIONS(description='FK verso clienti.id_cliente'),
  id_conto            STRING             OPTIONS(description='FK verso conti.id_conto (opzionale)'),
  categoria           STRING             OPTIONS(description='OPERATIVA | CONTRATTUALE | INFORMATIVA | FRODE | SERVIZIO_DIGITALE | ALTRO'),
  sottocategoria      STRING             OPTIONS(description='Dettaglio categoria reclamo'),
  descrizione         STRING             OPTIONS(description='Testo libero del reclamo'),
  canale_reclamo      STRING             OPTIONS(description='FILIALE | EMAIL | TELEFONO | APP | RACCOMANDATA'),
  priorita            STRING             OPTIONS(description='BASSA | MEDIA | ALTA | CRITICA'),
  stato               STRING             OPTIONS(description='APERTO | IN_LAVORAZIONE | CHIUSO_POSITIVO | CHIUSO_NEGATIVO | ESCALATO'),
  data_apertura       TIMESTAMP          OPTIONS(description='Data e ora apertura reclamo'),
  data_chiusura       TIMESTAMP          OPTIONS(description='Data e ora chiusura (NULL se ancora aperto)'),
  esito               STRING             OPTIONS(description='Descrizione esito finale'),
  indennizzo_eur      NUMERIC            OPTIONS(description='Importo indennizzo riconosciuto al cliente (EUR), NULL se nessuno'),
  id_operatore        STRING             OPTIONS(description='ID operatore assegnato'),
  sla_giorni          INT64              OPTIONS(description='SLA in giorni lavorativi per la categoria'),
  rispettato_sla      BOOL               OPTIONS(description='TRUE se chiuso entro SLA, FALSE altrimenti, NULL se ancora aperto')
);


-- ============================================================
-- TABLE: banca_raw.segmenti_storia
-- Storico SCD Type 2 dei segmenti cliente (una riga per periodo).
-- ============================================================
CREATE OR REPLACE TABLE `phrasal-method-484415-g7.banca_raw.segmenti_storia` (
  id_storia       STRING   NOT NULL OPTIONS(description='ID univoco riga (es. SEG001)'),
  id_cliente      STRING            OPTIONS(description='FK verso clienti.id_cliente'),
  segmento        STRING            OPTIONS(description='RETAIL | PRIVATE | CORPORATE'),
  rating_interno  STRING            OPTIONS(description='Rating al momento del cambio segmento'),
  data_inizio     DATE              OPTIONS(description='Data inizio validità del segmento'),
  data_fine       DATE              OPTIONS(description='Data fine validità (NULL = corrente)'),
  motivo_cambio   STRING            OPTIONS(description='Motivo classificazione/reclassificazione'),
  operatore       STRING            OPTIONS(description='Utente/sistema che ha effettuato il cambio')
);
