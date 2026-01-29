{{ config(
    materialized='incremental',
    unique_key='address_number',
    on_schema_change='fail',
    contract={'enforced': true}
) }}

WITH raw_data AS (
    SELECT
        CAST("COD_JDE" AS INTEGER) AS address_number,
        CAST(TRIM("NOMBRE") AS VARCHAR(255)) AS name,
        CAST(TRIM("RUC") AS VARCHAR(13)) AS ruc,
        CAST("CUPO" AS DECIMAL(12,2)) AS credit_limit,
        CAST(TRIM("FORMAPAGO") AS VARCHAR(50)) AS payment_method,
        -- Variable intermedia para el estado
        CASE
            WHEN _sdc_deleted_at IS NULL THEN TRUE
            ELSE FALSE
        END AS is_active,
        CAST(_sdc_extracted_at AS TIMESTAMP) AS extraction_date
    FROM {{ source('oracle_raw_clientes', 'CLIENTES_B2B') }}
),

processed_data AS (
    SELECT
        *,
        -- INCLUIMOS 'is_active' en el HASH
        MD5(CONCAT(
            name,
            ruc,
            CAST(credit_limit AS TEXT),
            payment_method,
            CAST(is_active AS TEXT) -- Si esto cambia de TRUE a FALSE, el MD5 cambia
        )) AS row_hash
    FROM raw_data
)

SELECT
    pd.address_number,
    pd.name,
    pd.ruc,
    pd.credit_limit,
    pd.payment_method,
    CAST(pd.is_active AS BOOLEAN) AS active,
    CAST(
        {% if is_incremental() %}
            COALESCE(dest.created_at, pd.extraction_date)
        {% else %}
            pd.extraction_date
        {% endif %}
        AS TIMESTAMP
    ) AS created_at,
    CAST(
        {% if is_incremental() %}
            CASE
                WHEN pd.row_hash != dest.row_hash THEN CURRENT_TIMESTAMP
                ELSE dest.updated_at
            END
        {% else %}
            CURRENT_TIMESTAMP
        {% endif %}
        AS TIMESTAMP
    ) AS updated_at,
    CAST(pd.row_hash AS TEXT) AS row_hash
FROM processed_data pd
    {% if is_incremental() %}
    LEFT JOIN {{ this }} dest ON pd.address_number = dest.address_number
WHERE dest.address_number IS NULL OR pd.row_hash != dest.row_hash
    {% endif %}