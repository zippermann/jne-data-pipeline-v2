-- ============================================================
-- JNE Tables Unification SQL — Oracle 19c Edition
-- ============================================================
-- Converted from unify_jne_tables_v4.sql (PostgreSQL) to Oracle 19c.
--
-- Key syntax changes from the PostgreSQL version:
--   • DISTINCT ON (key) … ORDER BY … → ROW_NUMBER() inner-subquery pattern
--   • SPLIT_PART(col, '/', n)         → REGEXP_SUBSTR(col, '[^/]+', 1, n)
--   • Table names                     → prefixed with JNE. schema
--   • CAST(x AS VARCHAR(n))           → CAST(x AS VARCHAR2(n))
--   • manifest_deduped marked with /*+ MATERIALIZE */ (used 13×)
--
-- This file contains a pure WITH…SELECT block.
-- The calling Python script (unify_oracle.py) wraps it in a CTAS:
--   CREATE TABLE HOA.UNIFIED_SHIPMENTS NOLOGGING AS
--   <this SQL>
--   WHERE cn.CNOTE_CRDATE >= :start_date
--
-- For incremental runs the script appends:
--   WHERE cn.CNOTE_CRDATE >= :cutoff
--
-- CHANGELOG v5:
--   FIX 1 — Removed stray `mfbag.ESB_ID AS MFBAG_ESB_ID` line after CTEs
--   FIX 2 — Moved NEW TRANSFORMATIONS block into main SELECT body
--   FIX 3 — Added missing space in `AS APICUST_*` aliases (section 4)
--   FIX 4 — Corrected section numbering (31→32→33, no skip)
-- ============================================================

WITH
-- ============================================================
-- DEDUPLICATION CTEs
-- ============================================================

-- CMS_CNOTE_AMO: keep latest by CDATE per CNOTE_NO
cnote_amo_deduped AS (
    SELECT * FROM (
        SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a.CNOTE_NO ORDER BY a.CDATE DESC NULLS LAST) rn
        FROM JNE.CMS_CNOTE_AMO a
    ) WHERE rn = 1
),

-- CMS_MRCNOTE: keep latest by MRCNOTE_DATE per MRCNOTE_NO
mrcnote_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MRCNOTE_NO ORDER BY m.MRCNOTE_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_MRCNOTE m
    ) WHERE rn = 1
),

-- CMS_DRSHEET: keep latest by DRSHEET_DATE per DRSHEET_CNOTE_NO
drsheet_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DRSHEET_CNOTE_NO ORDER BY d.DRSHEET_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_DRSHEET d
    ) WHERE rn = 1
),

-- CMS_MRSHEET: keep latest by MRSHEET_DATE per MRSHEET_NO
mrsheet_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MRSHEET_NO ORDER BY m.MRSHEET_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_MRSHEET m
    ) WHERE rn = 1
),

-- CMS_DRSHEET_PRA: keep latest by CREATION_DATE per DRSHEET_CNOTE_NO
drsheet_pra_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DRSHEET_CNOTE_NO ORDER BY d.CREATION_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_DRSHEET_PRA d
    ) WHERE rn = 1
),

-- CMS_DHICNOTE: keep latest by DHICNOTE_TDATE per DHICNOTE_CNOTE_NO
dhicnote_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DHICNOTE_CNOTE_NO ORDER BY d.DHICNOTE_TDATE DESC NULLS LAST) rn
        FROM JNE.CMS_DHICNOTE d
    ) WHERE rn = 1
),

-- CMS_MHICNOTE: keep latest by MHICNOTE_DATE per MHICNOTE_NO
mhicnote_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MHICNOTE_NO ORDER BY m.MHICNOTE_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_MHICNOTE m
    ) WHERE rn = 1
),

-- CMS_DHOCNOTE: keep latest by DHOCNOTE_TDATE per DHOCNOTE_CNOTE_NO
dhocnote_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DHOCNOTE_CNOTE_NO ORDER BY d.DHOCNOTE_TDATE DESC NULLS LAST) rn
        FROM JNE.CMS_DHOCNOTE d
    ) WHERE rn = 1
),

-- CMS_MHOCNOTE: keep latest by MHOCNOTE_DATE per MHOCNOTE_NO
mhocnote_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MHOCNOTE_NO ORDER BY m.MHOCNOTE_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_MHOCNOTE m
    ) WHERE rn = 1
),

-- CMS_DHOUNDEL_POD: keep latest by CREATE_DATE per DHOUNDEL_CNOTE_NO
dhoundel_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DHOUNDEL_CNOTE_NO ORDER BY d.CREATE_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_DHOUNDEL_POD d
    ) WHERE rn = 1
),

-- CMS_MHOUNDEL_POD: keep latest by MHOUNDEL_DATE per MHOUNDEL_NO
mhoundel_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MHOUNDEL_NO ORDER BY m.MHOUNDEL_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_MHOUNDEL_POD m
    ) WHERE rn = 1
),

-- CMS_MFCNOTE: classify each row by manifest type and sequence within type
mfcnote_typed AS (
    SELECT
        m.MFCNOTE_NO,
        m.MFCNOTE_MAN_NO,
        m.MFCNOTE_BAG_NO,
        m.MFCNOTE_WEIGHT,
        m.MFCNOTE_CRDATE,
        UPPER(REGEXP_SUBSTR(m.MFCNOTE_MAN_NO, '[^/]+', 1, 2)) AS MAN_TYPE,
        ROW_NUMBER() OVER (
            PARTITION BY m.MFCNOTE_NO, UPPER(REGEXP_SUBSTR(m.MFCNOTE_MAN_NO, '[^/]+', 1, 2))
            ORDER BY m.MFCNOTE_CRDATE ASC NULLS LAST
        ) AS type_seq
    FROM JNE.CMS_MFCNOTE m
    WHERE m.MFCNOTE_MAN_NO IS NOT NULL
),

-- CMS_MFCNOTE: pivot to one row per CNOTE with columns per manifest type
mfcnote_pivoted AS (
    SELECT
        MFCNOTE_NO,
        -- OM (Outbound Manifest)
        MAX(CASE WHEN MAN_TYPE = 'OM' AND type_seq = 1 THEN MFCNOTE_MAN_NO END) AS OM_MAN_NO,
        MAX(CASE WHEN MAN_TYPE = 'OM' AND type_seq = 1 THEN MFCNOTE_BAG_NO END) AS OM_BAG_NO,
        MAX(CASE WHEN MAN_TYPE = 'OM' AND type_seq = 1 THEN MFCNOTE_WEIGHT END) AS OM_MFC_WEIGHT,
        MAX(CASE WHEN MAN_TYPE = 'OM' AND type_seq = 1 THEN MFCNOTE_CRDATE END) AS OM_MFC_CRDATE,
        -- TM1 (Transit Manifest #1 — earliest)
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 1 THEN MFCNOTE_MAN_NO END) AS TM1_MAN_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 1 THEN MFCNOTE_BAG_NO END) AS TM1_BAG_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 1 THEN MFCNOTE_WEIGHT END) AS TM1_MFC_WEIGHT,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 1 THEN MFCNOTE_CRDATE END) AS TM1_MFC_CRDATE,
        -- TM2 (Transit Manifest #2)
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 2 THEN MFCNOTE_MAN_NO END) AS TM2_MAN_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 2 THEN MFCNOTE_BAG_NO END) AS TM2_BAG_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 2 THEN MFCNOTE_WEIGHT END) AS TM2_MFC_WEIGHT,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 2 THEN MFCNOTE_CRDATE END) AS TM2_MFC_CRDATE,
        -- TM3 (Transit Manifest #3)
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 3 THEN MFCNOTE_MAN_NO END) AS TM3_MAN_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 3 THEN MFCNOTE_BAG_NO END) AS TM3_BAG_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 3 THEN MFCNOTE_WEIGHT END) AS TM3_MFC_WEIGHT,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 3 THEN MFCNOTE_CRDATE END) AS TM3_MFC_CRDATE,
        -- TM4 (Transit Manifest #4)
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 4 THEN MFCNOTE_MAN_NO END) AS TM4_MAN_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 4 THEN MFCNOTE_BAG_NO END) AS TM4_BAG_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 4 THEN MFCNOTE_WEIGHT END) AS TM4_MFC_WEIGHT,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 4 THEN MFCNOTE_CRDATE END) AS TM4_MFC_CRDATE,
        -- TM5 (Transit Manifest #5)
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 5 THEN MFCNOTE_MAN_NO END) AS TM5_MAN_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 5 THEN MFCNOTE_BAG_NO END) AS TM5_BAG_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 5 THEN MFCNOTE_WEIGHT END) AS TM5_MFC_WEIGHT,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 5 THEN MFCNOTE_CRDATE END) AS TM5_MFC_CRDATE,
        -- TM6 (Transit Manifest #6)
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 6 THEN MFCNOTE_MAN_NO END) AS TM6_MAN_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 6 THEN MFCNOTE_BAG_NO END) AS TM6_BAG_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 6 THEN MFCNOTE_WEIGHT END) AS TM6_MFC_WEIGHT,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 6 THEN MFCNOTE_CRDATE END) AS TM6_MFC_CRDATE,
        -- TM7 (Transit Manifest #7)
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 7 THEN MFCNOTE_MAN_NO END) AS TM7_MAN_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 7 THEN MFCNOTE_BAG_NO END) AS TM7_BAG_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 7 THEN MFCNOTE_WEIGHT END) AS TM7_MFC_WEIGHT,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 7 THEN MFCNOTE_CRDATE END) AS TM7_MFC_CRDATE,
        -- TM8 (Transit Manifest #8)
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 8 THEN MFCNOTE_MAN_NO END) AS TM8_MAN_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 8 THEN MFCNOTE_BAG_NO END) AS TM8_BAG_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 8 THEN MFCNOTE_WEIGHT END) AS TM8_MFC_WEIGHT,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 8 THEN MFCNOTE_CRDATE END) AS TM8_MFC_CRDATE,
        -- TM9 (Transit Manifest #9)
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 9 THEN MFCNOTE_MAN_NO END) AS TM9_MAN_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 9 THEN MFCNOTE_BAG_NO END) AS TM9_BAG_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 9 THEN MFCNOTE_WEIGHT END) AS TM9_MFC_WEIGHT,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 9 THEN MFCNOTE_CRDATE END) AS TM9_MFC_CRDATE,
        -- TM10 (Transit Manifest #10)
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 10 THEN MFCNOTE_MAN_NO END) AS TM10_MAN_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 10 THEN MFCNOTE_BAG_NO END) AS TM10_BAG_NO,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 10 THEN MFCNOTE_WEIGHT END) AS TM10_MFC_WEIGHT,
        MAX(CASE WHEN MAN_TYPE = 'TM' AND type_seq = 10 THEN MFCNOTE_CRDATE END) AS TM10_MFC_CRDATE,
        -- IM (Inbound Manifest)
        MAX(CASE WHEN MAN_TYPE = 'IM' AND type_seq = 1 THEN MFCNOTE_MAN_NO END) AS IM_MAN_NO,
        MAX(CASE WHEN MAN_TYPE = 'IM' AND type_seq = 1 THEN MFCNOTE_BAG_NO END) AS IM_BAG_NO,
        MAX(CASE WHEN MAN_TYPE = 'IM' AND type_seq = 1 THEN MFCNOTE_WEIGHT END) AS IM_MFC_WEIGHT,
        MAX(CASE WHEN MAN_TYPE = 'IM' AND type_seq = 1 THEN MFCNOTE_CRDATE END) AS IM_MFC_CRDATE,
        -- HM (Hub Manifest)
        MAX(CASE WHEN MAN_TYPE = 'HM' AND type_seq = 1 THEN MFCNOTE_MAN_NO END) AS HM_MAN_NO,
        MAX(CASE WHEN MAN_TYPE = 'HM' AND type_seq = 1 THEN MFCNOTE_BAG_NO END) AS HM_BAG_NO,
        MAX(CASE WHEN MAN_TYPE = 'HM' AND type_seq = 1 THEN MFCNOTE_WEIGHT END) AS HM_MFC_WEIGHT,
        MAX(CASE WHEN MAN_TYPE = 'HM' AND type_seq = 1 THEN MFCNOTE_CRDATE END) AS HM_MFC_CRDATE
    FROM mfcnote_typed
    GROUP BY MFCNOTE_NO
),

-- CMS_MANIFEST: dedupe by MANIFEST_NO — MATERIALIZE because joined 13× below
manifest_deduped AS (
    SELECT /*+ MATERIALIZE */ * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MANIFEST_NO ORDER BY m.MANIFEST_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_MANIFEST m
    ) WHERE rn = 1
),

-- CMS_DBAG_HO: keep latest by CDATE per DBAG_CNOTE_NO
dbag_ho_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DBAG_CNOTE_NO ORDER BY d.CDATE DESC NULLS LAST) rn
        FROM JNE.CMS_DBAG_HO d
    ) WHERE rn = 1
),

-- CMS_DMBAG: keep latest by ESB_TIME per source bag used to join from CMS_MFBAG
dmbag_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DMBAG_BAG_NO ORDER BY d.ESB_TIME DESC NULLS LAST) rn
        FROM JNE.CMS_DMBAG d
    ) WHERE rn = 1
),

-- CMS_DHOV_RSHEET: keep latest by CREATE_DATE per DHOV_RSHEET_CNOTE
dhov_rsheet_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DHOV_RSHEET_CNOTE ORDER BY d.CREATE_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_DHOV_RSHEET d
    ) WHERE rn = 1
),

-- CMS_DSTATUS: keep latest by CREATE_DATE per DSTATUS_CNOTE_NO
dstatus_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DSTATUS_CNOTE_NO ORDER BY d.CREATE_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_DSTATUS d
    ) WHERE rn = 1
),

-- CMS_COST_DTRANSIT_AGEN: keep latest by ESB_TIME per CNOTE_NO
cost_dtransit_deduped AS (
    SELECT * FROM (
        SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.CNOTE_NO ORDER BY c.ESB_TIME DESC NULLS LAST) rn
        FROM JNE.CMS_COST_DTRANSIT_AGEN c
    ) WHERE rn = 1
),

-- CMS_COST_MTRANSIT_AGEN: keep latest by MANIFEST_DATE per MANIFEST_NO
cost_mtransit_deduped AS (
    SELECT * FROM (
        SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.MANIFEST_NO ORDER BY c.MANIFEST_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_COST_MTRANSIT_AGEN c
    ) WHERE rn = 1
),

-- CMS_RDSJ: keep latest per inbound handover key used to join from CMS_DHICNOTE
rdsj_deduped AS (
    SELECT * FROM (
        SELECT r.*, ROW_NUMBER() OVER (PARTITION BY r.RDSJ_HVI_NO ORDER BY r.RDSJ_CDATE DESC NULLS LAST) rn
        FROM JNE.CMS_RDSJ r
    ) WHERE rn = 1
),

-- CMS_DSJ: keep latest by DSJ_CDATE per DSJ_HVO_NO
dsj_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DSJ_HVO_NO ORDER BY d.DSJ_CDATE DESC NULLS LAST) rn
        FROM JNE.CMS_DSJ d
    ) WHERE rn = 1
),

-- CMS_MSJ: keep latest by MSJ_DATE per MSJ_NO
msj_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MSJ_NO ORDER BY m.MSJ_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_MSJ m
    ) WHERE rn = 1
),

-- CMS_DSMU: keep latest by ESB_TIME per (DSMU_NO, DSMU_BAG_NO) so all bag-level rows survive
dsmu_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DSMU_NO, d.DSMU_BAG_NO ORDER BY d.ESB_TIME DESC NULLS LAST) rn
        FROM JNE.CMS_DSMU d
    ) WHERE rn = 1
),

-- CMS_MSMU: keep latest by MSMU_DATE per MSMU_NO
msmu_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MSMU_NO ORDER BY m.MSMU_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_MSMU m
    ) WHERE rn = 1
),

-- CMS_DROURATE: keep latest by DROURATE_UDATE per (DROURATE_CODE, DROURATE_SERVICE)
drourate_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DROURATE_CODE, d.DROURATE_SERVICE ORDER BY d.DROURATE_UDATE DESC NULLS LAST) rn
        FROM JNE.CMS_DROURATE d
    ) WHERE rn = 1
),

-- T_MDT_CITY_ORIGIN: keep latest by CREATE_DATE per CITY_CODE
mdt_city_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.CITY_CODE ORDER BY m.CREATE_DATE DESC NULLS LAST) rn
        FROM JNE.T_MDT_CITY_ORIGIN m
    ) WHERE rn = 1
),

-- LASTMILE_COURIER: keep latest by COURIER_UPDATED_AT per COURIER_ID
courier_deduped AS (
    SELECT * FROM (
        SELECT c.*, ROW_NUMBER() OVER (PARTITION BY c.COURIER_ID ORDER BY c.COURIER_UPDATED_AT DESC NULLS LAST) rn
        FROM JNE.LASTMILE_COURIER c
    ) WHERE rn = 1
),

-- ORA_ZONE: keep latest by LASTUPDDTM per ZONE_CODE
ora_zone_deduped AS (
    SELECT * FROM (
        SELECT o.*, ROW_NUMBER() OVER (PARTITION BY o.ZONE_CODE ORDER BY o.LASTUPDDTM DESC NULLS LAST) rn
        FROM JNE.ORA_ZONE o
    ) WHERE rn = 1
),

-- CMS_DHI_HOC: keep latest by CDATE per DHI_CNOTE_NO
dhi_hoc_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DHI_CNOTE_NO ORDER BY d.CDATE DESC NULLS LAST) rn
        FROM JNE.CMS_DHI_HOC d
    ) WHERE rn = 1
),

-- CMS_MHI_HOC: keep latest by MHI_DATE per MHI_NO
mhi_hoc_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MHI_NO ORDER BY m.MHI_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_MHI_HOC m
    ) WHERE rn = 1
),

-- ORA_USER: keep latest by USER_DATE per USER_ID
ora_user_deduped AS (
    SELECT * FROM (
        SELECT u.*, ROW_NUMBER() OVER (PARTITION BY u.USER_ID ORDER BY u.USER_DATE DESC NULLS LAST) rn
        FROM JNE.ORA_USER u
    ) WHERE rn = 1
),

-- CMS_MMBAG: keep latest by MMBAG_DATE per MMBAG_NO
mmbag_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MMBAG_NO ORDER BY m.MMBAG_DATE DESC NULLS LAST) rn
        FROM JNE.CMS_MMBAG m
    ) WHERE rn = 1
),

-- CMS_MFBAG: keep latest by MFBAG_CRDATE per MFBAG_NO
mfbag_deduped AS (
    SELECT * FROM (
        SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.MFBAG_NO ORDER BY m.MFBAG_CRDATE DESC NULLS LAST) rn
        FROM JNE.CMS_MFBAG m
    ) WHERE rn = 1
),

-- CMS_DCORRECT_DESTINATION: keep latest by DCORRECT_CDATE per DCORRECT_CNOTE_NO
dcorrect_deduped AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.DCORRECT_CNOTE_NO ORDER BY d.DCORRECT_CDATE DESC NULLS LAST) rn
        FROM JNE.CMS_DCORRECT_DEST d
    ) WHERE rn = 1
)

-- ============================================================
-- MAIN SELECT — 39 tables, kept columns only
-- ============================================================
SELECT
    -- ========================================
    -- 1. CMS_CNOTE (59 of 117 kept)
    -- ========================================
    cn.CNOTE_NO,
    cn.CNOTE_DATE,
    cn.CNOTE_BRANCH_ID,
    cn.CNOTE_SERVICES_CODE,
    cn.CNOTE_CUST_NO,
    cn.CNOTE_ROUTE_CODE,
    cn.CNOTE_ORIGIN,
    cn.CNOTE_DESTINATION,
    cn.CNOTE_QTY,
    cn.CNOTE_WEIGHT,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_SHIPPER_NAME,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_SHIPPER_ADDR1,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_SHIPPER_ADDR2,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_SHIPPER_ADDR3,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_SHIPPER_CITY,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_SHIPPER_ZIP,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_SHIPPER_REGION,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_SHIPPER_COUNTRY,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_SHIPPER_CONTACT,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_SHIPPER_PHONE,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_RECEIVER_NAME,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_RECEIVER_ADDR1,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_RECEIVER_ADDR2,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_RECEIVER_ADDR3,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_RECEIVER_CITY,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_RECEIVER_ZIP,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_RECEIVER_REGION,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_RECEIVER_COUNTRY,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_RECEIVER_CONTACT,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_RECEIVER_PHONE,
    cn.CNOTE_GOODS_TYPE,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_GOODS_DESCR,
    cn.CNOTE_GOODS_VALUE,
    cn.CNOTE_SPECIAL_INS,
    cn.CNOTE_INSURANCE_ID,
    cn.CNOTE_INSURANCE_VALUE,
    cn.CNOTE_PAYMENT_TYPE,
    cn.CNOTE_CURRENCY,
    cn.CNOTE_AMOUNT,
    cn.CNOTE_ADDITIONAL_FEE,
    cn.CNOTE_NOTICE,
    cn.CNOTE_PRINTED,
    cn.CNOTE_CANCEL,
    cn.CNOTE_HOLD,
    cn.CNOTE_HOLD_REASON,
    cn.CNOTE_USER,
    cn.CNOTE_REFNO,
    cn.CNOTE_INSURANCE_NO,
    cn.CNOTE_OTHER_FEE,
    cn.CNOTE_CURC_PAYMENT,
    cn.CNOTE_CURC_RATE,
    cn.CNOTE_PAYMENT_BY,
    cn.CNOTE_AMOUNT_PAYMENT,
    cn.CNOTE_BILNOTE,
    cn.CNOTE_CRDATE,
    cn.CNOTE_PACKING,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_CARD_NO,
    cn.CNOTE_ECNOTE,
    cn.CNOTE_SES_FRM,

    -- ========================================
    -- 2. CMS_CNOTE_POD (7 of 7 — ALL KEPT)
    -- ========================================
    pod.CNOTE_POD_NO,
    pod.CNOTE_POD_DATE,
    CAST(NULL AS VARCHAR2(500)) AS CNOTE_POD_RECEIVER,
    pod.CNOTE_POD_STATUS,
    pod.CNOTE_POD_DELIVERED,
    pod.CNOTE_POD_DOC_NO,
    pod.CNOTE_POD_CREATION_DATE,

    -- ========================================
    -- 3. CMS_CNOTE_AMO (13 of 13 — ALL KEPT)
    -- ========================================
    amo.CNOTE_BRANCH_ID  AS AMO_BRANCH_ID,
    amo.FIELDCHAR1       AS AMO_FIELDCHAR1,
    amo.FIELDCHAR2       AS AMO_FIELDCHAR2,
    amo.FIELDCHAR3       AS AMO_FIELDCHAR3,
    amo.FIELDCHAR4       AS AMO_FIELDCHAR4,
    amo.FIELDCHAR5       AS AMO_FIELDCHAR5,
    amo.FIELDNUM1        AS AMO_FIELDNUM1,
    amo.FIELDNUM2        AS AMO_FIELDNUM2,
    amo.FIELDNUM3        AS AMO_FIELDNUM3,
    amo.FIELDNUM4        AS AMO_FIELDNUM4,
    amo.FIELDNUM5        AS AMO_FIELDNUM5,
    amo.CDATE            AS AMO_CDATE,

    -- ========================================
    -- 4. CMS_APICUST (42 of 44 — 2 excluded)
    -- FIX 3: Added missing space in all AS APICUST_* aliases
    -- ========================================
    api.APICUST_ORDER_ID,
    api.APICUST_CNOTE_NO  AS APICUST_CNOTE_NO,
    api.APICUST_ORIGIN,
    api.APICUST_BRANCH,
    api.APICUST_CUST_NO,
    api.APICUST_SERVICES_CODE AS APICUST_SERVICES_CODE,
    api.APICUST_DESTINATION,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_SHIPPER_NAME,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_SHIPPER_ADDR1,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_SHIPPER_ADDR2,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_SHIPPER_ADDR3,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_SHIPPER_CITY,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_SHIPPER_ZIP,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_SHIPPER_REGION,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_SHIPPER_COUNTRY,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_SHIPPER_CONTACT,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_SHIPPER_PHONE,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_RECEIVER_NAME,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_RECEIVER_ADDR1,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_RECEIVER_ADDR2,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_RECEIVER_ADDR3,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_RECEIVER_CITY,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_RECEIVER_ZIP,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_RECEIVER_REGION,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_RECEIVER_COUNTRY,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_RECEIVER_CONTACT,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_RECEIVER_PHONE,
    api.APICUST_QTY,
    api.APICUST_WEIGHT,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_GOODS_DESCR,
    api.APICUST_GOODS_VALUE,
    api.APICUST_SPECIAL_INS,
    api.APICUST_INS_FLAG,
    api.APICUST_COD_FLAG,
    api.APICUST_COD_AMOUNT,
    api.CREATE_DATE       AS APICUST_CREATE_DATE,
    api.APICUST_LATITUDE,
    api.APICUST_LONGITUDE,
    api.APICUST_SHIPMENT_TYPE,
    api.APICUST_MERCHAN_ID,
    CAST(NULL AS VARCHAR2(500)) AS APICUST_NAME,
    api.SHIPPER_PROVIDER,

    -- ========================================
    -- 5. CMS_DROURATE (8 of 17 — 9 excluded)
    -- ========================================
    drou.DROURATE_CODE,
    drou.DROURATE_SERVICE AS DROURATE_SERVICE,
    drou.DROURATE_ACTIVE,
    drou.DROURATE_UDATE,
    drou.DROURATE_ETD_FROM,
    drou.DROURATE_ETD_THRU,
    drou.DROURATE_TIME,
    drou.DROURATE_YES_LATE,

    -- ========================================
    -- 6. CMS_DRCNOTE (7 of 9 — 2 excluded)
    -- ========================================
    drc.DRCNOTE_NO,
    drc.DRCNOTE_CNOTE_NO  AS DRCNOTE_CNOTE_NO,
    drc.DRCNOTE_QTY,
    drc.DRCNOTE_REMARKS,
    drc.DRCNOTE_TDATE,
    drc.DRCNOTE_TUSER,
    drc.DRCNOTE_PAYMENT,

    -- ========================================
    -- 7. CMS_MRCNOTE (9 of 12 — 3 excluded)
    -- ========================================
    mrc.MRCNOTE_NO        AS MRCNOTE_NO,
    mrc.MRCNOTE_DATE,
    mrc.MRCNOTE_BRANCH_ID,
    mrc.MRCNOTE_USER_ID,
    mrc.MRCNOTE_COURIER_ID,
    mrc.MRCNOTE_USER1,
    mrc.MRCNOTE_USER2,
    mrc.MRCNOTE_SIGNDATE,
    mrc.MRCNOTE_PAYMENT,

    -- ========================================
    -- 8. CMS_DRSHEET (9 of 9 — ALL KEPT)
    -- ========================================
    drs.DRSHEET_NO,
    drs.DRSHEET_CNOTE_NO  AS DRSHEET_CNOTE_NO,
    drs.DRSHEET_DATE,
    drs.DRSHEET_STATUS,
    drs.DRSHEET_RECEIVER,
    drs.DRSHEET_FLAG,
    drs.DRSHEET_UID,
    drs.DRSHEET_UDATE,
    drs.CREATION_DATE     AS DRSHEET_CREATION_DATE,

    -- ========================================
    -- 9. CMS_MRSHEET (8 of 13 — 5 excluded)
    -- ========================================
    mrsht.MRSHEET_BRANCH,
    mrsht.MRSHEET_NO      AS MRSHEET_NO,
    mrsht.MRSHEET_DATE,
    mrsht.MRSHEET_COURIER_ID,
    mrsht.MRSHEET_UID,
    mrsht.MRSHEET_UDATE,
    mrsht.MRSHEET_APPROVED_DR,
    mrsht.MRSHEET_UID_DR,

    -- ========================================
    -- 10. CMS_DRSHEET_PRA (3 of 10 — 7 excluded)
    -- ========================================
    pra.DRSHEET_NO        AS DRSHEET_PRA_NO,
    pra.DRSHEET_CNOTE_NO  AS DRSHEET_PRA_CNOTE_NO,
    pra.CREATION_DATE     AS DRSHEET_PRA_CREATION_DATE,

    -- ========================================
    -- 11. CMS_DHICNOTE (5 of 6 — 1 excluded)
    -- ========================================
    dhicn.DHICNOTE_NO,
    dhicn.DHICNOTE_CNOTE_NO AS DHICNOTE_CNOTE_NO,
    dhicn.DHICNOTE_QTY,
    dhicn.DHICNOTE_REMARKS,
    dhicn.DHICNOTE_TDATE,

    -- ========================================
    -- 12. CMS_MHICNOTE (11 of 11 — ALL KEPT)
    -- ========================================
    mhicn.MHICNOTE_BRANCH_ID,
    mhicn.MHICNOTE_ZONE,
    mhicn.MHICNOTE_NO     AS MHICNOTE_NO,
    mhicn.MHICNOTE_REF_NO,
    mhicn.MHICNOTE_DATE,
    mhicn.MHICNOTE_ZONE_ORIG,
    mhicn.MHICNOTE_USER_ID,
    mhicn.MHICNOTE_USER1,
    mhicn.MHICNOTE_USER2,
    mhicn.MHICNOTE_SIGNDATE,
    mhicn.MHICNOTE_APPROVE,

    -- ========================================
    -- 13. CMS_DHOCNOTE (5 of 12 — 7 excluded)
    -- ========================================
    dhoc.DHOCNOTE_NO,
    dhoc.DHOCNOTE_CNOTE_NO AS DHOCNOTE_CNOTE_NO,
    dhoc.DHOCNOTE_QTY,
    dhoc.DHOCNOTE_REMARKS,
    dhoc.DHOCNOTE_TDATE,

    -- ========================================
    -- 14. CMS_MHOCNOTE (13 of 20 — 7 excluded)
    -- ========================================
    mhoc.MHOCNOTE_BRANCH_ID,
    mhoc.MHOCNOTE_ZONE,
    mhoc.MHOCNOTE_NO      AS MHOCNOTE_NO,
    mhoc.MHOCNOTE_DATE,
    mhoc.MHOCNOTE_ZONE_DEST,
    mhoc.MHOCNOTE_USER_ID AS MHOCNOTE_USER_ID,
    mhoc.MHOCNOTE_USER1,
    mhoc.MHOCNOTE_USER2,
    mhoc.MHOCNOTE_SIGNDATE AS MHOCNOTE_SIGNDATE,
    mhoc.MHOCNOTE_APPROVE,
    mhoc.MHOCNOTE_REMARKS,
    mhoc.MHOCNOTE_COURIER_ID,
    mhoc.MHOCNOTE_APP_DATE,

    -- ========================================
    -- 15. CMS_DHOUNDEL_POD (6 of 11 — 5 excluded)
    -- ========================================
    dhund.DHOUNDEL_NO,
    dhund.DHOUNDEL_CNOTE_NO AS DHOUNDEL_CNOTE_NO,
    dhund.DHOUNDEL_QTY,
    dhund.DHOUNDEL_REMARKS,
    dhund.DHOUNDEL_HRS,
    dhund.CREATE_DATE      AS DHOUNDEL_CREATE_DATE,

    -- ========================================
    -- 16. CMS_MHOUNDEL_POD (10 of 11 — 1 excluded)
    -- ========================================
    mhund.MHOUNDEL_BRANCH_ID,
    mhund.MHOUNDEL_NO     AS MHOUNDEL_NO,
    mhund.MHOUNDEL_REMARKS AS MHOUNDEL_REMARKS,
    mhund.MHOUNDEL_DATE,
    mhund.MHOUNDEL_USER_ID,
    mhund.MHOUNDEL_ZONE,
    mhund.MHOUNDEL_APPROVE,
    mhund.MHOUNDEL_USER1,
    mhund.MHOUNDEL_USER2,
    mhund.MHOUNDEL_SIGNDATE,

    -- ========================================
    -- 17-18. CMS_MFCNOTE × CMS_MANIFEST (pivoted by type)
    -- ========================================

    -- OM (Outbound Manifest) — MFCNOTE
    mfcp.OM_MAN_NO,
    mfcp.OM_BAG_NO,
    mfcp.OM_MFC_WEIGHT,
    mfcp.OM_MFC_CRDATE,
    -- OM — MANIFEST
    man_om.MANIFEST_DATE         AS OM_MANIFEST_DATE,
    man_om.MANIFEST_ROUTE        AS OM_MANIFEST_ROUTE,
    man_om.MANIFEST_FROM         AS OM_MANIFEST_FROM,
    man_om.MANIFEST_THRU         AS OM_MANIFEST_THRU,
    man_om.MANIFEST_NOTICE       AS OM_MANIFEST_NOTICE,
    man_om.MANIFEST_APPROVED     AS OM_MANIFEST_APPROVED,
    man_om.MANIFEST_ORIGIN       AS OM_MANIFEST_ORIGIN,
    man_om.MANIFEST_CODE         AS OM_MANIFEST_CODE,
    man_om.MANIFEST_UID          AS OM_MANIFEST_UID,
    man_om.MANIFEST_CRDATE       AS OM_MANIFEST_CRDATE,
    man_om.MANIFEST_CANCELED     AS OM_MANIFEST_CANCELED,
    man_om.MANIFEST_CANCELED_UID AS OM_MANIFEST_CANCELED_UID,

    -- TM1 (Transit Manifest #1) — MFCNOTE
    mfcp.TM1_MAN_NO,
    mfcp.TM1_BAG_NO,
    mfcp.TM1_MFC_WEIGHT,
    mfcp.TM1_MFC_CRDATE,
    -- TM1 — MANIFEST
    man_tm1.MANIFEST_DATE         AS TM1_MANIFEST_DATE,
    man_tm1.MANIFEST_ROUTE        AS TM1_MANIFEST_ROUTE,
    man_tm1.MANIFEST_FROM         AS TM1_MANIFEST_FROM,
    man_tm1.MANIFEST_THRU         AS TM1_MANIFEST_THRU,
    man_tm1.MANIFEST_NOTICE       AS TM1_MANIFEST_NOTICE,
    man_tm1.MANIFEST_APPROVED     AS TM1_MANIFEST_APPROVED,
    man_tm1.MANIFEST_ORIGIN       AS TM1_MANIFEST_ORIGIN,
    man_tm1.MANIFEST_CODE         AS TM1_MANIFEST_CODE,
    man_tm1.MANIFEST_UID          AS TM1_MANIFEST_UID,
    man_tm1.MANIFEST_CRDATE       AS TM1_MANIFEST_CRDATE,
    man_tm1.MANIFEST_CANCELED     AS TM1_MANIFEST_CANCELED,
    man_tm1.MANIFEST_CANCELED_UID AS TM1_MANIFEST_CANCELED_UID,

    -- TM2 (Transit Manifest #2) — MFCNOTE
    mfcp.TM2_MAN_NO,
    mfcp.TM2_BAG_NO,
    mfcp.TM2_MFC_WEIGHT,
    mfcp.TM2_MFC_CRDATE,
    -- TM2 — MANIFEST
    man_tm2.MANIFEST_DATE         AS TM2_MANIFEST_DATE,
    man_tm2.MANIFEST_ROUTE        AS TM2_MANIFEST_ROUTE,
    man_tm2.MANIFEST_FROM         AS TM2_MANIFEST_FROM,
    man_tm2.MANIFEST_THRU         AS TM2_MANIFEST_THRU,
    man_tm2.MANIFEST_NOTICE       AS TM2_MANIFEST_NOTICE,
    man_tm2.MANIFEST_APPROVED     AS TM2_MANIFEST_APPROVED,
    man_tm2.MANIFEST_ORIGIN       AS TM2_MANIFEST_ORIGIN,
    man_tm2.MANIFEST_CODE         AS TM2_MANIFEST_CODE,
    man_tm2.MANIFEST_UID          AS TM2_MANIFEST_UID,
    man_tm2.MANIFEST_CRDATE       AS TM2_MANIFEST_CRDATE,
    man_tm2.MANIFEST_CANCELED     AS TM2_MANIFEST_CANCELED,
    man_tm2.MANIFEST_CANCELED_UID AS TM2_MANIFEST_CANCELED_UID,

    -- TM3 (Transit Manifest #3) — MFCNOTE
    mfcp.TM3_MAN_NO,
    mfcp.TM3_BAG_NO,
    mfcp.TM3_MFC_WEIGHT,
    mfcp.TM3_MFC_CRDATE,
    -- TM3 — MANIFEST
    man_tm3.MANIFEST_DATE         AS TM3_MANIFEST_DATE,
    man_tm3.MANIFEST_ROUTE        AS TM3_MANIFEST_ROUTE,
    man_tm3.MANIFEST_FROM         AS TM3_MANIFEST_FROM,
    man_tm3.MANIFEST_THRU         AS TM3_MANIFEST_THRU,
    man_tm3.MANIFEST_NOTICE       AS TM3_MANIFEST_NOTICE,
    man_tm3.MANIFEST_APPROVED     AS TM3_MANIFEST_APPROVED,
    man_tm3.MANIFEST_ORIGIN       AS TM3_MANIFEST_ORIGIN,
    man_tm3.MANIFEST_CODE         AS TM3_MANIFEST_CODE,
    man_tm3.MANIFEST_UID          AS TM3_MANIFEST_UID,
    man_tm3.MANIFEST_CRDATE       AS TM3_MANIFEST_CRDATE,
    man_tm3.MANIFEST_CANCELED     AS TM3_MANIFEST_CANCELED,
    man_tm3.MANIFEST_CANCELED_UID AS TM3_MANIFEST_CANCELED_UID,

    -- TM4 (Transit Manifest #4) — MFCNOTE
    mfcp.TM4_MAN_NO,
    mfcp.TM4_BAG_NO,
    mfcp.TM4_MFC_WEIGHT,
    mfcp.TM4_MFC_CRDATE,
    -- TM4 — MANIFEST
    man_tm4.MANIFEST_DATE         AS TM4_MANIFEST_DATE,
    man_tm4.MANIFEST_ROUTE        AS TM4_MANIFEST_ROUTE,
    man_tm4.MANIFEST_FROM         AS TM4_MANIFEST_FROM,
    man_tm4.MANIFEST_THRU         AS TM4_MANIFEST_THRU,
    man_tm4.MANIFEST_NOTICE       AS TM4_MANIFEST_NOTICE,
    man_tm4.MANIFEST_APPROVED     AS TM4_MANIFEST_APPROVED,
    man_tm4.MANIFEST_ORIGIN       AS TM4_MANIFEST_ORIGIN,
    man_tm4.MANIFEST_CODE         AS TM4_MANIFEST_CODE,
    man_tm4.MANIFEST_UID          AS TM4_MANIFEST_UID,
    man_tm4.MANIFEST_CRDATE       AS TM4_MANIFEST_CRDATE,
    man_tm4.MANIFEST_CANCELED     AS TM4_MANIFEST_CANCELED,
    man_tm4.MANIFEST_CANCELED_UID AS TM4_MANIFEST_CANCELED_UID,

    -- TM5 (Transit Manifest #5) — MFCNOTE
    mfcp.TM5_MAN_NO,
    mfcp.TM5_BAG_NO,
    mfcp.TM5_MFC_WEIGHT,
    mfcp.TM5_MFC_CRDATE,
    -- TM5 — MANIFEST
    man_tm5.MANIFEST_DATE         AS TM5_MANIFEST_DATE,
    man_tm5.MANIFEST_ROUTE        AS TM5_MANIFEST_ROUTE,
    man_tm5.MANIFEST_FROM         AS TM5_MANIFEST_FROM,
    man_tm5.MANIFEST_THRU         AS TM5_MANIFEST_THRU,
    man_tm5.MANIFEST_NOTICE       AS TM5_MANIFEST_NOTICE,
    man_tm5.MANIFEST_APPROVED     AS TM5_MANIFEST_APPROVED,
    man_tm5.MANIFEST_ORIGIN       AS TM5_MANIFEST_ORIGIN,
    man_tm5.MANIFEST_CODE         AS TM5_MANIFEST_CODE,
    man_tm5.MANIFEST_UID          AS TM5_MANIFEST_UID,
    man_tm5.MANIFEST_CRDATE       AS TM5_MANIFEST_CRDATE,
    man_tm5.MANIFEST_CANCELED     AS TM5_MANIFEST_CANCELED,
    man_tm5.MANIFEST_CANCELED_UID AS TM5_MANIFEST_CANCELED_UID,

    -- TM6 (Transit Manifest #6) — MFCNOTE
    mfcp.TM6_MAN_NO,
    mfcp.TM6_BAG_NO,
    mfcp.TM6_MFC_WEIGHT,
    mfcp.TM6_MFC_CRDATE,
    -- TM6 — MANIFEST
    man_tm6.MANIFEST_DATE         AS TM6_MANIFEST_DATE,
    man_tm6.MANIFEST_ROUTE        AS TM6_MANIFEST_ROUTE,
    man_tm6.MANIFEST_FROM         AS TM6_MANIFEST_FROM,
    man_tm6.MANIFEST_THRU         AS TM6_MANIFEST_THRU,
    man_tm6.MANIFEST_NOTICE       AS TM6_MANIFEST_NOTICE,
    man_tm6.MANIFEST_APPROVED     AS TM6_MANIFEST_APPROVED,
    man_tm6.MANIFEST_ORIGIN       AS TM6_MANIFEST_ORIGIN,
    man_tm6.MANIFEST_CODE         AS TM6_MANIFEST_CODE,
    man_tm6.MANIFEST_UID          AS TM6_MANIFEST_UID,
    man_tm6.MANIFEST_CRDATE       AS TM6_MANIFEST_CRDATE,
    man_tm6.MANIFEST_CANCELED     AS TM6_MANIFEST_CANCELED,
    man_tm6.MANIFEST_CANCELED_UID AS TM6_MANIFEST_CANCELED_UID,

    -- TM7 (Transit Manifest #7) — MFCNOTE
    mfcp.TM7_MAN_NO,
    mfcp.TM7_BAG_NO,
    mfcp.TM7_MFC_WEIGHT,
    mfcp.TM7_MFC_CRDATE,
    -- TM7 — MANIFEST
    man_tm7.MANIFEST_DATE         AS TM7_MANIFEST_DATE,
    man_tm7.MANIFEST_ROUTE        AS TM7_MANIFEST_ROUTE,
    man_tm7.MANIFEST_FROM         AS TM7_MANIFEST_FROM,
    man_tm7.MANIFEST_THRU         AS TM7_MANIFEST_THRU,
    man_tm7.MANIFEST_NOTICE       AS TM7_MANIFEST_NOTICE,
    man_tm7.MANIFEST_APPROVED     AS TM7_MANIFEST_APPROVED,
    man_tm7.MANIFEST_ORIGIN       AS TM7_MANIFEST_ORIGIN,
    man_tm7.MANIFEST_CODE         AS TM7_MANIFEST_CODE,
    man_tm7.MANIFEST_UID          AS TM7_MANIFEST_UID,
    man_tm7.MANIFEST_CRDATE       AS TM7_MANIFEST_CRDATE,
    man_tm7.MANIFEST_CANCELED     AS TM7_MANIFEST_CANCELED,
    man_tm7.MANIFEST_CANCELED_UID AS TM7_MANIFEST_CANCELED_UID,

    -- TM8 (Transit Manifest #8) — MFCNOTE
    mfcp.TM8_MAN_NO,
    mfcp.TM8_BAG_NO,
    mfcp.TM8_MFC_WEIGHT,
    mfcp.TM8_MFC_CRDATE,
    -- TM8 — MANIFEST
    man_tm8.MANIFEST_DATE         AS TM8_MANIFEST_DATE,
    man_tm8.MANIFEST_ROUTE        AS TM8_MANIFEST_ROUTE,
    man_tm8.MANIFEST_FROM         AS TM8_MANIFEST_FROM,
    man_tm8.MANIFEST_THRU         AS TM8_MANIFEST_THRU,
    man_tm8.MANIFEST_NOTICE       AS TM8_MANIFEST_NOTICE,
    man_tm8.MANIFEST_APPROVED     AS TM8_MANIFEST_APPROVED,
    man_tm8.MANIFEST_ORIGIN       AS TM8_MANIFEST_ORIGIN,
    man_tm8.MANIFEST_CODE         AS TM8_MANIFEST_CODE,
    man_tm8.MANIFEST_UID          AS TM8_MANIFEST_UID,
    man_tm8.MANIFEST_CRDATE       AS TM8_MANIFEST_CRDATE,
    man_tm8.MANIFEST_CANCELED     AS TM8_MANIFEST_CANCELED,
    man_tm8.MANIFEST_CANCELED_UID AS TM8_MANIFEST_CANCELED_UID,

    -- TM9 (Transit Manifest #9) — MFCNOTE
    mfcp.TM9_MAN_NO,
    mfcp.TM9_BAG_NO,
    mfcp.TM9_MFC_WEIGHT,
    mfcp.TM9_MFC_CRDATE,
    -- TM9 — MANIFEST
    man_tm9.MANIFEST_DATE         AS TM9_MANIFEST_DATE,
    man_tm9.MANIFEST_ROUTE        AS TM9_MANIFEST_ROUTE,
    man_tm9.MANIFEST_FROM         AS TM9_MANIFEST_FROM,
    man_tm9.MANIFEST_THRU         AS TM9_MANIFEST_THRU,
    man_tm9.MANIFEST_NOTICE       AS TM9_MANIFEST_NOTICE,
    man_tm9.MANIFEST_APPROVED     AS TM9_MANIFEST_APPROVED,
    man_tm9.MANIFEST_ORIGIN       AS TM9_MANIFEST_ORIGIN,
    man_tm9.MANIFEST_CODE         AS TM9_MANIFEST_CODE,
    man_tm9.MANIFEST_UID          AS TM9_MANIFEST_UID,
    man_tm9.MANIFEST_CRDATE       AS TM9_MANIFEST_CRDATE,
    man_tm9.MANIFEST_CANCELED     AS TM9_MANIFEST_CANCELED,
    man_tm9.MANIFEST_CANCELED_UID AS TM9_MANIFEST_CANCELED_UID,

    -- TM10 (Transit Manifest #10) — MFCNOTE
    mfcp.TM10_MAN_NO,
    mfcp.TM10_BAG_NO,
    mfcp.TM10_MFC_WEIGHT,
    mfcp.TM10_MFC_CRDATE,
    -- TM10 — MANIFEST
    man_tm10.MANIFEST_DATE         AS TM10_MANIFEST_DATE,
    man_tm10.MANIFEST_ROUTE        AS TM10_MANIFEST_ROUTE,
    man_tm10.MANIFEST_FROM         AS TM10_MANIFEST_FROM,
    man_tm10.MANIFEST_THRU         AS TM10_MANIFEST_THRU,
    man_tm10.MANIFEST_NOTICE       AS TM10_MANIFEST_NOTICE,
    man_tm10.MANIFEST_APPROVED     AS TM10_MANIFEST_APPROVED,
    man_tm10.MANIFEST_ORIGIN       AS TM10_MANIFEST_ORIGIN,
    man_tm10.MANIFEST_CODE         AS TM10_MANIFEST_CODE,
    man_tm10.MANIFEST_UID          AS TM10_MANIFEST_UID,
    man_tm10.MANIFEST_CRDATE       AS TM10_MANIFEST_CRDATE,
    man_tm10.MANIFEST_CANCELED     AS TM10_MANIFEST_CANCELED,
    man_tm10.MANIFEST_CANCELED_UID AS TM10_MANIFEST_CANCELED_UID,

    -- IM (Inbound Manifest) — MFCNOTE
    mfcp.IM_MAN_NO,
    mfcp.IM_BAG_NO,
    mfcp.IM_MFC_WEIGHT,
    mfcp.IM_MFC_CRDATE,
    -- IM — MANIFEST
    man_im.MANIFEST_DATE         AS IM_MANIFEST_DATE,
    man_im.MANIFEST_ROUTE        AS IM_MANIFEST_ROUTE,
    man_im.MANIFEST_FROM         AS IM_MANIFEST_FROM,
    man_im.MANIFEST_THRU         AS IM_MANIFEST_THRU,
    man_im.MANIFEST_NOTICE       AS IM_MANIFEST_NOTICE,
    man_im.MANIFEST_APPROVED     AS IM_MANIFEST_APPROVED,
    man_im.MANIFEST_ORIGIN       AS IM_MANIFEST_ORIGIN,
    man_im.MANIFEST_CODE         AS IM_MANIFEST_CODE,
    man_im.MANIFEST_UID          AS IM_MANIFEST_UID,
    man_im.MANIFEST_CRDATE       AS IM_MANIFEST_CRDATE,
    man_im.MANIFEST_CANCELED     AS IM_MANIFEST_CANCELED,
    man_im.MANIFEST_CANCELED_UID AS IM_MANIFEST_CANCELED_UID,

    -- HM (Hub Manifest) — MFCNOTE
    mfcp.HM_MAN_NO,
    mfcp.HM_BAG_NO,
    mfcp.HM_MFC_WEIGHT,
    mfcp.HM_MFC_CRDATE,
    -- HM — MANIFEST
    man_hm.MANIFEST_DATE         AS HM_MANIFEST_DATE,
    man_hm.MANIFEST_ROUTE        AS HM_MANIFEST_ROUTE,
    man_hm.MANIFEST_FROM         AS HM_MANIFEST_FROM,
    man_hm.MANIFEST_THRU         AS HM_MANIFEST_THRU,
    man_hm.MANIFEST_NOTICE       AS HM_MANIFEST_NOTICE,
    man_hm.MANIFEST_APPROVED     AS HM_MANIFEST_APPROVED,
    man_hm.MANIFEST_ORIGIN       AS HM_MANIFEST_ORIGIN,
    man_hm.MANIFEST_CODE         AS HM_MANIFEST_CODE,
    man_hm.MANIFEST_UID          AS HM_MANIFEST_UID,
    man_hm.MANIFEST_CRDATE       AS HM_MANIFEST_CRDATE,
    man_hm.MANIFEST_CANCELED     AS HM_MANIFEST_CANCELED,
    man_hm.MANIFEST_CANCELED_UID AS HM_MANIFEST_CANCELED_UID,

    -- ========================================
    -- 19. CMS_DBAG_HO (10 of 10 — ALL KEPT)
    -- ========================================
    dbag.DBAG_HO_NO,
    dbag.DBAG_NO,
    dbag.DBAG_CNOTE_NO     AS DBAG_CNOTE_NO,
    dbag.DBAG_CNOTE_QTY,
    dbag.DBAG_CNOTE_WEIGHT,
    dbag.DBAG_CNOTE_DESTINATION,
    dbag.CDATE             AS DBAG_HO_CDATE,
    dbag.DBAG_CNOTE_SERVICE,
    dbag.DBAG_CNOTE_DATE,
    dbag.DBAG_ZONE_DEST,

    -- ========================================
    -- 20. CMS_DMBAG (7 of 11 — 4 excluded)
    -- ========================================
    dmbag.DMBAG_NO         AS DMBAG_NO,
    dmbag.DMBAG_BAG_NO,
    dmbag.DMBAG_ORIGIN,
    dmbag.DMBAG_DESTINATION,
    dmbag.DMBAG_WEIGHT,
    dmbag.ESB_TIME         AS DMBAG_ESB_TIME,
    dmbag.ESB_ID           AS DMBAG_ESB_ID,

    -- ========================================
    -- 21. CMS_DHOV_RSHEET (15 of 23 — 8 excluded)
    -- ========================================
    dhov.DHOV_RSHEET_NO,
    dhov.DHOV_RSHEET_CNOTE AS DHOV_RSHEET_CNOTE,
    dhov.DHOV_RSHEET_DO,
    dhov.DHOV_RSHEET_COD,
    dhov.DHOV_RSHEET_UNDEL,
    dhov.DHOV_RSHEET_QTY,
    dhov.CREATE_DATE       AS DHOV_CREATE_DATE,
    dhov.DHOV_RSHEET_UZONE,
    dhov.DHOV_RSHEET_CYCLE,
    dhov.DHOV_RSHEET_RSHEETNO,
    dhov.DHOV_DRSHEET_EPAY_VEND,
    dhov.DHOV_DRSHEET_EPAY_TRXID,
    dhov.DHOV_DRSHEET_EPAY_AMOUNT,
    dhov.DHOV_DRSHEET_EPAY_DEVICE,
    dhov.DHOV_DRSHEET_STATUS,

    -- ========================================
    -- 22. CMS_DSJ (5 of 5 — ALL KEPT)
    -- ========================================
    dsj.DSJ_NO,
    dsj.DSJ_BAG_NO,
    dsj.DSJ_HVO_NO,
    dsj.DSJ_UID,
    dsj.DSJ_CDATE,

    -- ========================================
    -- 23. CMS_MSJ (13 of 17 — 4 excluded)
    -- ========================================
    msj.MSJ_NO             AS MSJ_NO,
    msj.MSJ_BRANCH_ID,
    msj.MSJ_DATE,
    msj.MSJ_USER1,
    msj.MSJ_USER2,
    msj.MSJ_APPROVE,
    msj.MSJ_CDATE,
    msj.MSJ_UID,
    msj.MSJ_REMARKS,
    msj.MSJ_SIGNDATE,
    msj.MSJ_DEST,
    msj.MSJ_ORIG,
    msj.MSJ_COURIER_ID,

    -- ========================================
    -- 24. CMS_RDSJ (6 of 6 — ALL KEPT)
    -- ========================================
    rdsj.RDSJ_NO,
    rdsj.RDSJ_BAG_NO,
    rdsj.RDSJ_HVO_NO,
    rdsj.RDSJ_UID,
    rdsj.RDSJ_CDATE,
    rdsj.RDSJ_HVI_NO,

    -- ========================================
    -- 25. CMS_DSMU (10 of 14 — 4 excluded)
    -- ========================================
    dsmu.DSMU_NO,
    dsmu.DSMU_FLIGHT_NO,
    dsmu.DSMU_FLIGHT_DATE,
    dsmu.DSMU_BAG_NO       AS DSMU_BAG_NO,
    dsmu.DSMU_WEIGHT,
    dsmu.DSMU_BAG_ORIGIN,
    dsmu.DSMU_BAG_DESTINATION,
    dsmu.ESB_TIME          AS DSMU_ESB_TIME,
    dsmu.ESB_ID            AS DSMU_ESB_ID,
    dsmu.DSMU_POLICE_LICENSE_PLATE,

    -- ========================================
    -- 26. CMS_MSMU (25 of 26 — 1 excluded)
    -- ========================================
    msmu.MSMU_NO           AS MSMU_NO,
    msmu.MSMU_DATE,
    msmu.MSMU_ORIGIN,
    msmu.MSMU_DESTINATION,
    msmu.MSMU_FLIGHT_NO    AS MSMU_FLIGHT_NO,
    msmu.MSMU_FLIGHT_DATE  AS MSMU_FLIGHT_DATE,
    msmu.MSMU_ETD,
    msmu.MSMU_ETA,
    msmu.MSMU_QTY,
    msmu.MSMU_WEIGHT,
    msmu.MSMU_USER,
    msmu.MSMU_FLAG,
    msmu.MSMU_REMARKS,
    msmu.MSMU_STATUS,
    msmu.MSMU_WRH_DATE,
    msmu.MSMU_WRH_TIME,
    msmu.MSMU_OFF_DATE,
    msmu.MSMU_OFF_TIME,
    msmu.MSMU_CONFIRM,
    msmu.MSMU_CANCEL,
    msmu.MSMU_USER_CANCEL,
    msmu.MSMU_REPLACE,
    msmu.MSMU_MODA,
    msmu.MSMU_POLICE_LICENSE_PLATE,
    msmu.MSMU_HOURS,

    -- ========================================
    -- 27. CMS_DSTATUS (12 of 13 — 1 excluded)
    -- ========================================
    dst.DSTATUS_NO,
    dst.DSTATUS_CNOTE_NO   AS DSTATUS_CNOTE_NO,
    dst.DSTATUS_STATUS,
    dst.DSTATUS_REMARKS,
    dst.CREATE_DATE        AS DSTATUS_CREATE_DATE,
    dst.DSTATUS_STATUS_DATE,
    dst.DSTATUS_MANIFEST_NO_OLD,
    dst.DSTATUS_BAG_NO_OLD,
    dst.DSTATUS_MANIFEST_NO_NEW,
    dst.DSTATUS_MANIFEST_DEST,
    dst.DSTATUS_MANIFEST_THRU,
    dst.DSTATUS_ZONE_CODE,

    -- ========================================
    -- 28. CMS_COST_DTRANSIT_AGEN (10 of 11 — 1 excluded)
    -- ========================================
    costd.DMANIFEST_NO     AS COST_D_MANIFEST_NO,
    costd.CNOTE_NO         AS COST_D_CNOTE_NO,
    costd.CNOTE_ORIGIN     AS COST_D_ORIGIN,
    costd.CNOTE_DESTINATION AS COST_D_DESTINATION,
    costd.CNOTE_QTY        AS COST_D_QTY,
    costd.CNOTE_WEIGHT     AS COST_D_WEIGHT,
    costd.CNOTE_SERVICES_CODE AS COST_D_SERVICES_CODE,
    costd.ESB_TIME         AS COST_D_ESB_TIME,
    costd.ESB_ID           AS COST_D_ESB_ID,
    costd.DMANIFEST_DOC_REF AS COST_D_DOC_REF,

    -- ========================================
    -- 29. CMS_COST_MTRANSIT_AGEN (12 of 15 — 3 excluded)
    -- ========================================
    costm.MANIFEST_NO      AS COST_M_MANIFEST_NO,
    costm.MANIFEST_DATE    AS COST_M_MANIFEST_DATE,
    costm.BRANCH_ID        AS COST_M_BRANCH_ID,
    costm.DESTINATION      AS COST_M_DESTINATION,
    costm.CTC_WEIGHT       AS COST_M_CTC_WEIGHT,
    costm.ACT_WEIGHT       AS COST_M_ACT_WEIGHT,
    costm.MANIFEST_APPROVED AS COST_M_APPROVED,
    costm.REMARK           AS COST_M_REMARK,
    costm.MANIFEST_UID     AS COST_M_UID,
    costm.ESB_TIME         AS COST_M_ESB_TIME,
    costm.ESB_ID           AS COST_M_ESB_ID,
    costm.MANIFEST_DOC_REF AS COST_M_DOC_REF,

    -- ========================================
    -- 30. T_MDT_CITY_ORIGIN (6 of 6 — ALL KEPT)
    -- ========================================
    mdt.CITY_BRANCH        AS MDT_CITY_BRANCH,
    mdt.CITY_CODE          AS MDT_CITY_CODE,
    mdt.CITY_ORIGIN        AS MDT_CITY_ORIGIN,
    mdt.CITY_MTS           AS MDT_CITY_MTS,
    mdt.CITY_ACTIVE        AS MDT_CITY_ACTIVE,
    mdt.CREATE_DATE        AS MDT_CREATE_DATE,

    -- ========================================
    -- 31. LASTMILE_COURIER (17 of 29 — 12 excluded: PII + vacant)
    -- ========================================
    cour.COURIER_ID,
    cour.COURIER_NAME,
    cour.COURIER_REGIONAL,
    cour.COURIER_BRANCH    AS COURIER_BRANCH,
    cour.COURIER_ZONE      AS COURIER_ZONE,
    cour.COURIER_ORIGIN    AS COURIER_ORIGIN,
    cour.COURIER_ACTIVE,
    cour.COURIER_SP_VALUE,
    cour.COURIER_INCENTIVE_GROUP,
    cour.COURIER_ARMADA,
    cour.COURIER_EMPLOYEE_STATUS,
    cour.COURIER_CREATED_AT,
    cour.COURIER_UPDATED_AT,
    cour.COURIER_ROLE_ID,
    cour.COURIER_LEVEL,
    cour.COURIER_COMPANY_ID,
    cour.COURIER_TYPE,

    -- ========================================
    -- 32. ORA_ZONE (25 of 25 — ALL KEPT)
    -- FIX 4: Renumbered from 33 → 32 (section 32 was previously skipped)
    -- ========================================
    ora.ZONE_BRANCH        AS ORA_ZONE_BRANCH,
    ora.ZONE_CODE          AS ORA_ZONE_CODE,
    ora.ZONE_DESC          AS ORA_ZONE_DESC,
    ora.ZONE_UID           AS ORA_ZONE_UID,
    ora.ZONE_ACTIVE        AS ORA_ZONE_ACTIVE,
    ora.ZONE_SEQ           AS ORA_ZONE_SEQ,
    ora.ZONE_DATE          AS ORA_ZONE_DATE,
    ora.ZONE_TYPE          AS ORA_ZONE_TYPE,
    ora.ZONE_ECNOTE_PARM   AS ORA_ZONE_ECNOTE_PARM,
    ora.ZONE_ORIGIN        AS ORA_ZONE_ORIGIN,
    ora.ZONE_NAME          AS ORA_ZONE_NAME,
    ora.ZONE_ADDR1         AS ORA_ZONE_ADDR1,
    ora.ZONE_ADDR2         AS ORA_ZONE_ADDR2,
    ora.ZONE_ADDR3         AS ORA_ZONE_ADDR3,
    ora.ZONE_DP_FLAG       AS ORA_ZONE_DP_FLAG,
    ora.ZONE_LATITUDE      AS ORA_ZONE_LATITUDE,
    ora.ZONE_LONGTITIDE    AS ORA_ZONE_LONGTITIDE,
    ora.ZONE_KOTA          AS ORA_ZONE_KOTA,
    ora.ZONE_KECAMATAN     AS ORA_ZONE_KECAMATAN,
    ora.ZONE_PROVINSI      AS ORA_ZONE_PROVINSI,
    ora.ZONE_CATEGORY      AS ORA_ZONE_CATEGORY,
    ora.LASTUPDDTM         AS ORA_ZONE_LASTUPDDTM,
    ora.LASTUPDBY          AS ORA_ZONE_LASTUPDBY,
    ora.LASTUPDPROCESS     AS ORA_ZONE_LASTUPDPROCESS,
    ora.ZONE_KELURAHAN     AS ORA_ZONE_KELURAHAN,

    -- ========================================
    -- 33. ORA_USER (10 of 10 — ALL KEPT)
    -- ========================================
    usr.USER_ID            AS ORA_USER_ID,
    usr.USER_NAME          AS ORA_USER_NAME,
    usr.USER_CUST_ID       AS ORA_USER_CUST_ID,
    usr.USER_CUST_NAME     AS ORA_USER_CUST_NAME,
    usr.USER_ZONE_CODE     AS ORA_USER_ZONE_CODE,
    usr.USER_ACTIVE        AS ORA_USER_ACTIVE,
    usr.USER_DATE          AS ORA_USER_DATE,
    usr.USER_ORIGIN        AS ORA_USER_ORIGIN,
    CAST(NULL AS VARCHAR2(500)) AS ORA_USER_NIK,
    usr.USER_VACANT10      AS ORA_USER_VACANT10,

    -- ========================================
    -- 34. CMS_DHI_HOC (6 of 9 — 3 excluded)
    -- ========================================
    dhi.DHI_NO,
    dhi.DHI_ONO,
    dhi.DHI_CNOTE_NO       AS DHI_HOC_CNOTE_NO,
    dhi.DHI_CNOTE_QTY,
    dhi.CDATE              AS DHI_HOC_CDATE,
    dhi.DHI_REMARKS,

    -- ========================================
    -- 35. CMS_MHI_HOC (11 of 12 — 1 excluded)
    -- ========================================
    mhi.MHI_NO             AS MHI_HOC_NO,
    mhi.MHI_REF_NO,
    mhi.MHI_DATE,
    mhi.MHI_UID,
    mhi.MHI_APPROVE,
    mhi.MHI_BRANCH         AS MHI_HOC_BRANCH,
    mhi.MHI_APPROVE_DATE,
    mhi.MHI_REMARKS        AS MHI_HOC_REMARKS,
    mhi.MHI_USER1,
    mhi.MHI_USER2,
    mhi.MHI_ZONE,

    -- ========================================
    -- 36. CMS_MMBAG (13 of 13 — ALL KEPT)
    -- ========================================
    COALESCE(mmbag_by_no.MMBAG_BRANCH,       mmbag_by_bag.MMBAG_BRANCH)       AS MMBAG_BRANCH,
    COALESCE(mmbag_by_no.MMBAG_NO,           mmbag_by_bag.MMBAG_NO)           AS MMBAG_NO,
    COALESCE(mmbag_by_no.MMBAG_ORIGIN,       mmbag_by_bag.MMBAG_ORIGIN)       AS MMBAG_ORIGIN,
    COALESCE(mmbag_by_no.MMBAG_DESTINATION,  mmbag_by_bag.MMBAG_DESTINATION)  AS MMBAG_DESTINATION,
    COALESCE(mmbag_by_no.MMBAG_DATE,         mmbag_by_bag.MMBAG_DATE)         AS MMBAG_DATE,
    COALESCE(mmbag_by_no.MMBAG_QTY,          mmbag_by_bag.MMBAG_QTY)          AS MMBAG_QTY,
    COALESCE(mmbag_by_no.MMBAG_WEIGHT,       mmbag_by_bag.MMBAG_WEIGHT)       AS MMBAG_WEIGHT,
    COALESCE(mmbag_by_no.MMBAG_FLAG,         mmbag_by_bag.MMBAG_FLAG)         AS MMBAG_FLAG,
    COALESCE(mmbag_by_no.MMBAG_USER,         mmbag_by_bag.MMBAG_USER)         AS MMBAG_USER,
    COALESCE(mmbag_by_no.MMBAG_USER_APPROVE, mmbag_by_bag.MMBAG_USER_APPROVE) AS MMBAG_USER_APPROVE,
    COALESCE(mmbag_by_no.MMBAG_DATE_APPROVE, mmbag_by_bag.MMBAG_DATE_APPROVE) AS MMBAG_DATE_APPROVE,
    COALESCE(mmbag_by_no.MMBAG_APPROVED,     mmbag_by_bag.MMBAG_APPROVED)     AS MMBAG_APPROVED,
    COALESCE(mmbag_by_no.MMBAG_REMARKS,      mmbag_by_bag.MMBAG_REMARKS)      AS MMBAG_REMARKS,

    -- ========================================
    -- 37. CMS_MFBAG (21 of 21 — ALL KEPT)
    -- ========================================
    mfbag.MFBAG_MAN_NO,
    mfbag.MFBAG_NO             AS MFBAG_NO,
    mfbag.MFBAG_ACT_WEIGHT,
    mfbag.MFBAG_CTC_WEIGHT,
    mfbag.MFBAG_COST,
    mfbag.MFBAG_STATUS,
    mfbag.MFBAG_AWB,
    mfbag.MFBAG_FLAG,
    mfbag.MFBAG_MAN_REF,
    mfbag.MFBAG_ROUTE,
    mfbag.MFBAG_SMU,
    mfbag.MFBAG_DATE,
    mfbag.MFBAG_ORIGIN,
    mfbag.MFBAG_USER_AUDIT,
    mfbag.MFBAG_TIME_AUDIT,
    mfbag.MFBAG_FORM_AUDIT,
    mfbag.MFBAG_TRS,
    mfbag.MFBAG_CRDATE,
    mfbag.MFBAG_MAN_CODE,
    mfbag.ESB_TIME             AS MFBAG_ESB_TIME,
    mfbag.ESB_ID               AS MFBAG_ESB_ID,

    -- ========================================

    -- HANDOVER DROP-OFF COUNTER
    -- Menghitung berapa kali paket pindah tangan berdasarkan dokumen operasional yang terisi
    ( (CASE WHEN mrc.MRCNOTE_NO   IS NOT NULL THEN 1 ELSE 0 END) +    -- Pickup oleh kurir
      (CASE WHEN dhi.DHI_NO        IS NOT NULL THEN 1 ELSE 0 END) +    -- Handover Inbound
      (CASE WHEN dhoc.DHOCNOTE_NO  IS NOT NULL THEN 1 ELSE 0 END) +    -- Handover Outbound
      (CASE WHEN drs.DRSHEET_NO    IS NOT NULL THEN 1 ELSE 0 END) +    -- Masuk ke Runsheet kurir
      (CASE WHEN mfcp.TM1_MAN_NO   IS NOT NULL THEN 1 ELSE 0 END) +    -- Transit Manifest 1
      (CASE WHEN mfcp.TM2_MAN_NO   IS NOT NULL THEN 1 ELSE 0 END) +    -- Transit Manifest 2
      (CASE WHEN mfcp.TM3_MAN_NO   IS NOT NULL THEN 1 ELSE 0 END) +    -- Transit Manifest 3
      (CASE WHEN mfcp.TM4_MAN_NO   IS NOT NULL THEN 1 ELSE 0 END) +    -- Transit Manifest 4
      (CASE WHEN mfcp.TM5_MAN_NO   IS NOT NULL THEN 1 ELSE 0 END) +    -- Transit Manifest 5
      (CASE WHEN mfcp.TM6_MAN_NO   IS NOT NULL THEN 1 ELSE 0 END) +    -- Transit Manifest 6
      (CASE WHEN mfcp.TM7_MAN_NO   IS NOT NULL THEN 1 ELSE 0 END) +    -- Transit Manifest 7
      (CASE WHEN mfcp.TM8_MAN_NO   IS NOT NULL THEN 1 ELSE 0 END) +    -- Transit Manifest 8
      (CASE WHEN mfcp.TM9_MAN_NO   IS NOT NULL THEN 1 ELSE 0 END) +    -- Transit Manifest 9
      (CASE WHEN mfcp.TM10_MAN_NO  IS NOT NULL THEN 1 ELSE 0 END)      -- Transit Manifest 10
    ) AS HANDOVER_COUNT,

    -- DELIVERY TYPE (Logic Kemarin)
    CASE
        WHEN COALESCE(
            mfcp.TM1_MAN_NO, mfcp.TM2_MAN_NO, mfcp.TM3_MAN_NO, mfcp.TM4_MAN_NO, mfcp.TM5_MAN_NO,
            mfcp.TM6_MAN_NO, mfcp.TM7_MAN_NO, mfcp.TM8_MAN_NO, mfcp.TM9_MAN_NO, mfcp.TM10_MAN_NO
        ) IS NULL THEN 'Direct'
        ELSE 'Transit'
    END AS DELIVERY_TYPE,

    -- SHIPMENT SCOPE
    CASE
        WHEN SUBSTR(cn.CNOTE_ORIGIN, 1, 3) = SUBSTR(cn.CNOTE_DESTINATION, 1, 3) THEN 'Intracity'
        ELSE 'Intercity'
    END AS SHIPMENT_SCOPE,

    -- TRANSIT MANIFEST COUNTER (Logic Kemarin)
    ( (CASE WHEN mfcp.TM1_MAN_NO IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN mfcp.TM2_MAN_NO IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN mfcp.TM3_MAN_NO IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN mfcp.TM4_MAN_NO IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN mfcp.TM5_MAN_NO IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN mfcp.TM6_MAN_NO IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN mfcp.TM7_MAN_NO IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN mfcp.TM8_MAN_NO IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN mfcp.TM9_MAN_NO IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN mfcp.TM10_MAN_NO IS NOT NULL THEN 1 ELSE 0 END)
    ) AS TRANSIT_MANIFEST_COUNT,

    -- 39. CMS_DCORRECT_DESTINATION
    -- ========================================
    dcorr.DCORRECT_DEST_AFT  AS DCORRECT_NEW_DEST
FROM JNE.CMS_CNOTE cn

-- ============================================================
-- JOIN CHAIN (38 tables)
-- ============================================================

-- 2. CMS_CNOTE_POD (1:1 on CNOTE_NO)
LEFT JOIN JNE.CMS_CNOTE_POD pod
    ON cn.CNOTE_NO = pod.CNOTE_POD_NO

-- 3. CMS_CNOTE_AMO (deduped, 1:1 on CNOTE_NO)
LEFT JOIN cnote_amo_deduped amo
    ON cn.CNOTE_NO = amo.CNOTE_NO

-- 4. CMS_APICUST (1:1 on CNOTE_NO)
LEFT JOIN JNE.CMS_APICUST api
    ON cn.CNOTE_NO = api.APICUST_CNOTE_NO

-- 5. CMS_DROURATE (reference on route+service)
LEFT JOIN drourate_deduped drou
    ON cn.CNOTE_ROUTE_CODE = drou.DROURATE_CODE
    AND cn.CNOTE_SERVICES_CODE = drou.DROURATE_SERVICE

-- 6. CMS_DRCNOTE (1:1 on CNOTE_NO)
LEFT JOIN JNE.CMS_DRCNOTE drc
    ON cn.CNOTE_NO = drc.DRCNOTE_CNOTE_NO

-- 7. CMS_MRCNOTE (via DRCNOTE_NO)
LEFT JOIN mrcnote_deduped mrc
    ON drc.DRCNOTE_NO = mrc.MRCNOTE_NO

-- 8. CMS_DRSHEET (on CNOTE_NO)
LEFT JOIN drsheet_deduped drs
    ON cn.CNOTE_NO = drs.DRSHEET_CNOTE_NO

-- 9. CMS_MRSHEET (via DRSHEET_NO)
LEFT JOIN mrsheet_deduped mrsht
    ON drs.DRSHEET_NO = mrsht.MRSHEET_NO

-- 10. CMS_DRSHEET_PRA (on CNOTE_NO)
LEFT JOIN drsheet_pra_deduped pra
    ON cn.CNOTE_NO = pra.DRSHEET_CNOTE_NO

-- 11. CMS_DHICNOTE (on CNOTE_NO)
LEFT JOIN dhicnote_deduped dhicn
    ON cn.CNOTE_NO = dhicn.DHICNOTE_CNOTE_NO

-- 12. CMS_MHICNOTE (via DHICNOTE_NO)
LEFT JOIN mhicnote_deduped mhicn
    ON dhicn.DHICNOTE_NO = mhicn.MHICNOTE_NO

-- 13. CMS_DHOCNOTE (on CNOTE_NO)
LEFT JOIN dhocnote_deduped dhoc
    ON cn.CNOTE_NO = dhoc.DHOCNOTE_CNOTE_NO

-- 14. CMS_MHOCNOTE (via DHOCNOTE_NO)
LEFT JOIN mhocnote_deduped mhoc
    ON dhoc.DHOCNOTE_NO = mhoc.MHOCNOTE_NO

-- 15. CMS_DHOUNDEL_POD (on CNOTE_NO)
LEFT JOIN dhoundel_deduped dhund
    ON cn.CNOTE_NO = dhund.DHOUNDEL_CNOTE_NO

-- 16. CMS_MHOUNDEL_POD (via DHOUNDEL_NO)
LEFT JOIN mhoundel_deduped mhund
    ON dhund.DHOUNDEL_NO = mhund.MHOUNDEL_NO

-- 17. CMS_MFCNOTE (pivoted by manifest type: OM/TM1..TM10/IM/HM)
LEFT JOIN mfcnote_pivoted mfcp
    ON cn.CNOTE_NO = mfcp.MFCNOTE_NO

-- 18a. CMS_MANIFEST — Outbound (OM)
LEFT JOIN manifest_deduped man_om
    ON mfcp.OM_MAN_NO = man_om.MANIFEST_NO

-- 18b. CMS_MANIFEST — Transit #1 (TM1)
LEFT JOIN manifest_deduped man_tm1
    ON mfcp.TM1_MAN_NO = man_tm1.MANIFEST_NO

-- 18c. CMS_MANIFEST — Transit #2 (TM2)
LEFT JOIN manifest_deduped man_tm2
    ON mfcp.TM2_MAN_NO = man_tm2.MANIFEST_NO

-- 18d. CMS_MANIFEST — Transit #3 (TM3)
LEFT JOIN manifest_deduped man_tm3
    ON mfcp.TM3_MAN_NO = man_tm3.MANIFEST_NO

-- 18e. CMS_MANIFEST — Transit #4 (TM4)
LEFT JOIN manifest_deduped man_tm4
    ON mfcp.TM4_MAN_NO = man_tm4.MANIFEST_NO

-- 18f. CMS_MANIFEST — Transit #5 (TM5)
LEFT JOIN manifest_deduped man_tm5
    ON mfcp.TM5_MAN_NO = man_tm5.MANIFEST_NO

-- 18g. CMS_MANIFEST — Transit #6 (TM6)
LEFT JOIN manifest_deduped man_tm6
    ON mfcp.TM6_MAN_NO = man_tm6.MANIFEST_NO

-- 18h. CMS_MANIFEST — Transit #7 (TM7)
LEFT JOIN manifest_deduped man_tm7
    ON mfcp.TM7_MAN_NO = man_tm7.MANIFEST_NO

-- 18i. CMS_MANIFEST — Transit #8 (TM8)
LEFT JOIN manifest_deduped man_tm8
    ON mfcp.TM8_MAN_NO = man_tm8.MANIFEST_NO

-- 18j. CMS_MANIFEST — Transit #9 (TM9)
LEFT JOIN manifest_deduped man_tm9
    ON mfcp.TM9_MAN_NO = man_tm9.MANIFEST_NO

-- 18k. CMS_MANIFEST — Transit #10 (TM10)
LEFT JOIN manifest_deduped man_tm10
    ON mfcp.TM10_MAN_NO = man_tm10.MANIFEST_NO

-- 18l. CMS_MANIFEST — Inbound (IM)
LEFT JOIN manifest_deduped man_im
    ON mfcp.IM_MAN_NO = man_im.MANIFEST_NO

-- 18m. CMS_MANIFEST — Hub (HM)
LEFT JOIN manifest_deduped man_hm
    ON mfcp.HM_MAN_NO = man_hm.MANIFEST_NO

-- 19. CMS_DBAG_HO (on CNOTE_NO)
LEFT JOIN dbag_ho_deduped dbag
    ON cn.CNOTE_NO = dbag.DBAG_CNOTE_NO

-- 38. CMS_MFBAG (via first non-null bag number across all manifest types)
LEFT JOIN mfbag_deduped mfbag
    ON COALESCE(
        mfcp.OM_BAG_NO, mfcp.TM1_BAG_NO, mfcp.TM2_BAG_NO, mfcp.TM3_BAG_NO, mfcp.TM4_BAG_NO,
        mfcp.TM5_BAG_NO, mfcp.TM6_BAG_NO, mfcp.TM7_BAG_NO, mfcp.TM8_BAG_NO, mfcp.TM9_BAG_NO,
        mfcp.TM10_BAG_NO, mfcp.IM_BAG_NO, mfcp.HM_BAG_NO
    ) = mfbag.MFBAG_NO

-- 20. CMS_DMBAG (via MFBAG — MFCNOTE → MFBAG → DMBAG → MMBAG chain)
LEFT JOIN dmbag_deduped dmbag
    ON mfbag.MFBAG_NO = dmbag.DMBAG_BAG_NO

-- 21. CMS_DHOV_RSHEET (on CNOTE_NO)
LEFT JOIN dhov_rsheet_deduped dhov
    ON cn.CNOTE_NO = dhov.DHOV_RSHEET_CNOTE

-- 22. CMS_RDSJ (via DHICNOTE_NO → RDSJ_HVI_NO)
LEFT JOIN rdsj_deduped rdsj
    ON dhicn.DHICNOTE_NO = rdsj.RDSJ_HVI_NO

-- 23. CMS_DSJ (via RDSJ_HVO_NO)
LEFT JOIN dsj_deduped dsj
    ON rdsj.RDSJ_HVO_NO = dsj.DSJ_HVO_NO

-- 24. CMS_MSJ (via DSJ_NO)
LEFT JOIN msj_deduped msj
    ON dsj.DSJ_NO = msj.MSJ_NO

-- 25. CMS_DSMU (via DMBAG_NO — chains through dmbag, not dsj)
LEFT JOIN dsmu_deduped dsmu
    ON dmbag.DMBAG_NO = dsmu.DSMU_BAG_NO

-- 26. CMS_MSMU (via DSMU_NO — shares SMU number with DSMU)
LEFT JOIN msmu_deduped msmu
    ON dsmu.DSMU_NO = msmu.MSMU_NO

-- 27. CMS_DSTATUS (on CNOTE_NO — DSTATUS_CNOTE_NO may differ in type)
LEFT JOIN dstatus_deduped dst
    ON cn.CNOTE_NO = CAST(dst.DSTATUS_CNOTE_NO AS VARCHAR2(50))

-- 28. CMS_COST_DTRANSIT_AGEN (on CNOTE_NO)
LEFT JOIN cost_dtransit_deduped costd
    ON cn.CNOTE_NO = costd.CNOTE_NO

-- 29. CMS_COST_MTRANSIT_AGEN (via COST_DTRANSIT_AGEN manifest)
LEFT JOIN cost_mtransit_deduped costm
    ON costd.DMANIFEST_NO = costm.MANIFEST_NO

-- 30. T_MDT_CITY_ORIGIN (via CNOTE_ORIGIN → CITY_CODE)
LEFT JOIN mdt_city_deduped mdt
    ON cn.CNOTE_ORIGIN = mdt.CITY_CODE

-- 31. LASTMILE_COURIER (via MRSHEET_COURIER_ID)
LEFT JOIN courier_deduped cour
    ON mrsht.MRSHEET_COURIER_ID = cour.COURIER_ID

-- 32. ORA_ZONE (via DRSHEET_PRA zone → ZONE_CODE)
LEFT JOIN ora_zone_deduped ora
    ON CAST(pra.DRSHEET_ZONE AS VARCHAR2(50)) = ora.ZONE_CODE

-- 33. ORA_USER (via CNOTE_USER → USER_ID)
LEFT JOIN ora_user_deduped usr
    ON cn.CNOTE_USER = usr.USER_ID

-- 34. CMS_DHI_HOC (on CNOTE_NO)
LEFT JOIN dhi_hoc_deduped dhi
    ON cn.CNOTE_NO = dhi.DHI_CNOTE_NO

-- 35. CMS_MHI_HOC (via DHI_NO)
LEFT JOIN mhi_hoc_deduped mhi
    ON dhi.DHI_NO = mhi.MHI_NO

LEFT JOIN mmbag_deduped mmbag_by_no
    ON dmbag.DMBAG_NO = mmbag_by_no.MMBAG_NO
LEFT JOIN mmbag_deduped mmbag_by_bag
    ON dmbag.DMBAG_BAG_NO = mmbag_by_bag.MMBAG_NO

-- 39. CMS_DCORRECT_DESTINATION (on DCORRECT_CNOTE_NO)
LEFT JOIN dcorrect_deduped dcorr
    ON cn.CNOTE_NO = dcorr.DCORRECT_CNOTE_NO
