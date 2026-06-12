-- ============================================================
-- 1. metadata_silver_config
--    Controls load behaviour for every [Schema_Name] → silver table
-- ============================================================
CREATE OR REPLACE TABLE [Catalog_Name].[Schema_Name].metadata_silver_config (

    id                      BIGINT  GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
                                    COMMENT 'Surrogate key — auto generated',

    -- ── Source & Target ──────────────────────────────────────
    source_system           STRING  COMMENT 'Source system name e.g. ESK, CRM',
    source_schema           STRING  COMMENT '[Schema_Name] schema name',
    source_table            STRING  COMMENT '[Schema_Name] table name e.g. esk_vctransporter',
    target_schema           STRING  COMMENT 'Silver schema name',
    target_table            STRING  COMMENT 'Silver table name e.g. transporter',

    -- ── Load Behaviour ───────────────────────────────────────
    load_type               STRING  COMMENT 'FULL | INCREMENTAL. Default FULL for safety',
    write_mode              STRING  COMMENT 'UPSERT | OVERWRITE. UPSERT merges on PK, OVERWRITE truncates+reloads',
    scd_type                STRING  COMMENT 'SCD1 | SCD2 | NA. Only applies when write_mode=UPSERT',

    -- ── PK Strategy ──────────────────────────────────────────
    primary_key             STRING  COMMENT 'Comma-separated target PK columns e.g. id',
    pk_strategy             STRING  COMMENT 'NATURAL | HASH. NATURAL uses primary_key as-is. HASH generates surrogate from hash_columns',
    hash_columns            STRING  COMMENT 'Comma-separated SOURCE columns to hash when pk_strategy=HASH e.g. Id',
    hash_column_name        STRING  COMMENT 'Name of generated hash column in silver e.g. surrogate_key',

    -- ── Watermark ────────────────────────────────────────────
    watermark_column        STRING  COMMENT 'SOURCE column name used for incremental filter e.g. ModifiedDateTime',
    last_watermark_value    TIMESTAMP COMMENT 'MAX(watermark_column) from last successful run. NULL on first run',
    last_successful_run     TIMESTAMP COMMENT 'Pipeline end_time of last successful run',

    -- ── Filters ──────────────────────────────────────────────
    filter_condition        STRING  COMMENT 'Optional extra WHERE clause applied on [Schema_Name] read e.g. site_id=102',
    active_filter_column    STRING  COMMENT 'Column to filter soft-deleted records e.g. IsDelete, is_active',
    active_filter_value     STRING  COMMENT 'Value meaning active/not-deleted e.g. false, 0, N, true, 1, Y',

    -- ── Control ──────────────────────────────────────────────
    is_active               BOOLEAN COMMENT 'Only true rows are processed by pipeline',
    load_sequence           INT     COMMENT 'Processing order within a batch — lower runs first',
    created_date            TIMESTAMP,
    modified_date           TIMESTAMP
);


-- ============================================================
-- 2. metadata_column_config
--    Column-level mapping, casting and transformation rules
-- ============================================================
CREATE OR REPLACE TABLE [Catalog_Name].[Schema_Name].metadata_column_config (

    id                  BIGINT  GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),

    -- ── Scope ────────────────────────────────────────────────
    Catalog_Name             STRING  COMMENT 'Unity Catalog_Name name e.g. [Catalog_Name]',
    source_schema       STRING  COMMENT '[Schema_Name] schema',
    source_table        STRING  COMMENT '[Schema_Name] table name',
    target_schema       STRING  COMMENT 'Silver schema',
    target_table        STRING  COMMENT 'Silver table name',

    -- ── Column Mapping ───────────────────────────────────────
    source_column       STRING  COMMENT 'Exact column name in [Schema_Name] table',
    target_column       STRING  COMMENT 'snake_case column name in silver table',
    source_data_type    STRING  COMMENT 'Data type in [Schema_Name] e.g. BIGINT, VARCHAR',
    target_data_type    STRING  COMMENT 'Data type in silver e.g. STRING, INT, BOOLEAN, DECIMAL(18,2)',

    -- ── Transformation ───────────────────────────────────────
    transformation      STRING  COMMENT 'TRIM | UPPER | LOWER | NULL_IF_EMPTY | Y_N_TO_BOOL | INT_TO_BOOL | STANDARDIZE. NULL = cast only',

    -- ── Metadata ─────────────────────────────────────────────
    nullable_flag       BOOLEAN COMMENT 'Whether column allows NULL in silver',
    is_pii              BOOLEAN COMMENT 'Flag PII columns e.g. mobile_no, pan_number, aadharno',

    -- ── Control ──────────────────────────────────────────────
    column_sequence     INT     COMMENT 'Column order in SELECT — controls output column order',
    is_active           BOOLEAN COMMENT 'Only true columns are included in mapping',
    created_date        TIMESTAMP,
    modified_date       TIMESTAMP
);


-- ============================================================
-- 3. meta_data_quality
--    DQ rules applied per column before silver write
-- ============================================================
CREATE OR REPLACE TABLE [Catalog_Name].[Schema_Name].meta_data_quality (

    dq_id               BIGINT  GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),

    -- ── Scope ────────────────────────────────────────────────
    source_table_name   STRING  COMMENT '[Schema_Name] table name e.g. esk_vcdi_no',
    target_table_name   STRING  COMMENT 'Silver table name e.g. di_no — used to look up rules',

    -- ── Rule Definition ──────────────────────────────────────
    source_column       STRING  COMMENT 'Source column name for reference',
    target_column       STRING  COMMENT 'Target column the rule applies to',
    rule_type           STRING  COMMENT 'NOT_NULL | RANGE | DATE | STRING | DOMAIN | DUPLICATE',
    rule_expression     STRING  COMMENT 'Valid Spark SQL expression. Must use TO_TIMESTAMP() for dates not string literals',
    severity            STRING  COMMENT 'HIGH = reject record | MEDIUM = warn and load | LOW = log only',

    -- ── Control ──────────────────────────────────────────────
    is_active           BOOLEAN,
    created_date        TIMESTAMP,
    modified_date       TIMESTAMP
);

-- ============================================================
-- 4. pipeline_audit_log
--    One row per table per pipeline run
-- ============================================================
CREATE OR REPLACE TABLE [Catalog_Name].[Schema_Name].pipeline_audit_log (

    audit_id            BIGINT  GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),

    -- ── Run Identity ─────────────────────────────────────────
    batch_id            STRING  COMMENT 'UUID for the full notebook run — shared across all tables in one run',
    pipeline_id         INT     COMMENT 'Pipeline identifier e.g. 1 = [Schema_Name]→Silver',

    -- ── Table ────────────────────────────────────────────────
    source_table        STRING,
    target_table        STRING,
    source_layer        STRING  COMMENT '[Schema_Name]',
    target_layer        STRING  COMMENT 'silver',

    -- ── Timing ───────────────────────────────────────────────
    start_time          TIMESTAMP,
    end_time            TIMESTAMP,

    -- ── Counts ───────────────────────────────────────────────
    records_read        BIGINT,
    records_inserted    BIGINT,
    records_updated     BIGINT,
    records_rejected    BIGINT,
    records_deleted     BIGINT;
    dq_pass_count       BIGINT,
    dq_fail_count       BIGINT,

    -- ── Config Snapshot ──────────────────────────────────────
    load_type           STRING,
    write_mode          STRING,
    scd_type            STRING,
    pk_strategy         STRING  COMMENT 'NATURAL | HASH — which PK strategy was used',
    was_forced_full     BOOLEAN COMMENT 'true if INCREMENTAL was overridden to FULL due to first run',

    -- ── Result ───────────────────────────────────────────────
    status              STRING  COMMENT 'SUCCESS | FAILED | SKIPPED',
    error_message       STRING,
    created_at          TIMESTAMP
);

-- ============================================================
-- 5. error_log_table
--    One row per DQ failure per record
-- ============================================================
CREATE OR REPLACE TABLE [Catalog_Name].[Schema_Name].error_log_table (

    error_id            BIGINT  GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),

    -- ── Run Identity ─────────────────────────────────────────
    batch_id            STRING,
    table_name          STRING,

    -- ── Record ───────────────────────────────────────────────
    record_id           STRING  COMMENT 'PK value of the failing record as string',
    check_type          STRING  COMMENT 'NOT_NULL | RANGE | DATE | STRING | DOMAIN',
    check_description   STRING  COMMENT 'Rule expression that failed',
    severity            STRING  COMMENT 'HIGH | MEDIUM | LOW',
    raw_record          STRING  COMMENT 'Full record serialised as JSON for debugging',

    created_at          TIMESTAMP
);
