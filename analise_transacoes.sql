-- ============================================================
-- PROJETO: Análise de Transações Financeiras
-- Autora:  Ana Carolina Bacelar França Lira
-- Curso:   Formação em Análise de Dados — EBA Renata Biaggi
-- Ferram.: BigQuery (Google Cloud)
-- ============================================================


-- ============================================================
-- ETAPA 1 — Tabela consolidada de todas as transações (2020)
-- Une movimentações PIX (in/out), transfer_ins e transfer_outs
-- em uma única base com identificação do cliente
-- ============================================================

CREATE OR REPLACE TABLE `cursosql-eba.projeto1.total_transfers` AS (
  SELECT
    transaction_id,
    customer_id,
    full_name,
    account_id,
    amount,
    status,
    type_transaction,
    date_completed,
    month
  FROM (

    -- PIX (entradas e saídas)
    SELECT DISTINCT
      p.id                                          AS transaction_id,
      a.customer_id,
      CONCAT(c.first_name, " ", c.last_name)        AS full_name,
      p.account_id,
      p.pix_amount                                  AS amount,
      p.status,
      p.in_or_out                                   AS type_transaction,
      DATE(action_timestamp)                        AS date_completed,
      EXTRACT(MONTH FROM DATE(action_timestamp))    AS month
    FROM `cursosql-eba.projeto1.pix_movements`  p
    LEFT JOIN `cursosql-eba.projeto1.accounts`   a ON p.account_id         = a.account_id
    LEFT JOIN `cursosql-eba.projeto1.customers`  c ON a.customer_id        = c.customer_id
    LEFT JOIN `cursosql-eba.projeto1.time`       t ON p.pix_completed_at   = t.time_id
    WHERE p.status = 'completed'
      AND DATE(action_timestamp) BETWEEN '2020-01-01' AND '2020-12-31'

    UNION ALL

    -- Transferências recebidas (transfer_ins)
    SELECT DISTINCT
      i.id                                          AS transaction_id,
      a.customer_id,
      CONCAT(c.first_name, " ", c.last_name)        AS full_name,
      i.account_id,
      i.amount,
      i.status,
      'transfer_ins'                                AS type_transaction,
      DATE(action_timestamp)                        AS date_completed,
      EXTRACT(MONTH FROM DATE(action_timestamp))    AS month
    FROM `cursosql-eba.projeto1.transfer_ins`    i
    LEFT JOIN `cursosql-eba.projeto1.accounts`   a ON i.account_id              = a.account_id
    LEFT JOIN `cursosql-eba.projeto1.customers`  c ON a.customer_id             = c.customer_id
    LEFT JOIN `cursosql-eba.projeto1.time`       t ON i.transaction_completed_at = t.time_id
    WHERE i.status = 'completed'
      AND DATE(action_timestamp) BETWEEN '2020-01-01' AND '2020-12-31'

    UNION ALL

    -- Transferências enviadas (transfer_outs)
    SELECT DISTINCT
      o.id                                          AS transaction_id,
      a.customer_id,
      CONCAT(c.first_name, " ", c.last_name)        AS full_name,
      o.account_id,
      o.amount,
      o.status,
      'transfer_outs'                               AS type_transaction,
      DATE(action_timestamp)                        AS date_completed,
      EXTRACT(MONTH FROM DATE(action_timestamp))    AS month
    FROM `cursosql-eba.projeto1.transfer_outs`   o
    LEFT JOIN `cursosql-eba.projeto1.accounts`   a ON o.account_id              = a.account_id
    LEFT JOIN `cursosql-eba.projeto1.customers`  c ON a.customer_id             = c.customer_id
    LEFT JOIN `cursosql-eba.projeto1.time`       t ON o.transaction_completed_at = t.time_id
    WHERE o.status = 'completed'
      AND DATE(action_timestamp) BETWEEN '2020-01-01' AND '2020-12-31'
  )
);


-- ============================================================
-- ETAPA 2 — Saldo mensal acumulado por cliente
-- Calcula entradas e saídas separadas por mês e acumula
-- o saldo progressivo usando Window Function
-- ============================================================

WITH

-- Todos os meses existentes na base
all_months AS (
  SELECT DISTINCT month
  FROM `cursosql-eba.projeto1.total_transfers`
),

-- Total de entradas por cliente/mês
total_transfer_in AS (
  SELECT
    month,
    customer_id,
    full_name,
    COALESCE(SUM(amount), 0) AS total_transfer_in
  FROM `cursosql-eba.projeto1.total_transfers`
  WHERE type_transaction IN ('pix_in', 'transfer_ins')
  GROUP BY 1, 2, 3
),

-- Total de saídas por cliente/mês
total_transfer_out AS (
  SELECT
    month,
    customer_id,
    full_name,
    COALESCE(SUM(amount), 0) AS total_transfer_out
  FROM `cursosql-eba.projeto1.total_transfers`
  WHERE type_transaction IN ('pix_out', 'transfer_outs')
  GROUP BY 1, 2, 3
),

-- Combinação de todos os meses × todos os clientes (evita lacunas)
transfers_all AS (
  SELECT
    a.month,
    c.customer_id,
    c.full_name,
    COALESCE(tti.total_transfer_in,  0) AS total_transfer_in,
    COALESCE(tto.total_transfer_out, 0) AS total_transfer_out
  FROM all_months a
  CROSS JOIN (
    SELECT DISTINCT customer_id, full_name
    FROM total_transfer_in
  ) c
  LEFT JOIN total_transfer_in  tti ON a.month = tti.month AND c.customer_id = tti.customer_id
  LEFT JOIN total_transfer_out tto ON a.month = tto.month AND c.customer_id = tto.customer_id
)

-- Saldo acumulado mensal com Window Function
SELECT
  month,
  customer_id,
  full_name,
  ROUND(total_transfer_in,  2) AS total_transfer_in,
  ROUND(total_transfer_out, 2) AS total_transfer_out,
  ROUND(
    SUM(total_transfer_in - total_transfer_out)
      OVER (PARTITION BY customer_id ORDER BY month),
    2
  ) AS saldo_mensal
FROM transfers_all
ORDER BY full_name, month;


-- ============================================================
-- ETAPA 3 — Versão otimizada do saldo mensal
-- Consolida entradas e saídas em uma única CTE com CASE WHEN,
-- reduzindo o número de leituras na tabela base
-- ============================================================

WITH

all_months AS (
  SELECT DISTINCT month
  FROM `cursosql-eba.projeto1.total_transfers`
),

-- Entradas e saídas calculadas em uma única passagem
transfers_combined AS (
  SELECT
    month,
    customer_id,
    full_name,
    SUM(CASE
          WHEN type_transaction IN ('pix_in', 'transfer_ins')   THEN amount
          ELSE 0
        END) AS total_transfer_in,
    SUM(CASE
          WHEN type_transaction IN ('pix_out', 'transfer_outs') THEN amount
          ELSE 0
        END) AS total_transfer_out
  FROM `cursosql-eba.projeto1.total_transfers`
  GROUP BY month, customer_id, full_name
),

transfers_all AS (
  SELECT
    m.month,
    c.customer_id,
    c.full_name,
    COALESCE(tc.total_transfer_in,  0) AS total_transfer_in,
    COALESCE(tc.total_transfer_out, 0) AS total_transfer_out
  FROM all_months m
  CROSS JOIN (
    SELECT DISTINCT customer_id, full_name
    FROM transfers_combined
  ) c
  LEFT JOIN transfers_combined tc
    ON m.month = tc.month AND c.customer_id = tc.customer_id
)

SELECT
  month,
  customer_id,
  full_name,
  ROUND(total_transfer_in,  2) AS total_transfer_in,
  ROUND(total_transfer_out, 2) AS total_transfer_out,
  ROUND(
    SUM(total_transfer_in - total_transfer_out)
      OVER (PARTITION BY customer_id ORDER BY month),
    2
  ) AS saldo_mensal
FROM transfers_all
ORDER BY full_name, month;
