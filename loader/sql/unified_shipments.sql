CREATE TABLE {target_table}
ENGINE = MergeTree
ORDER BY tuple()
AS
WITH
api_ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY `APICUST_CNOTE_NO`
            ORDER BY `CREATE_DATE` DESC NULLS LAST
        ) AS `rn`
    FROM {bronze_schema}.`cms_apicust`
    WHERE `APICUST_CNOTE_NO` IS NOT NULL
),
cancel_api_ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY `API_CNOTE_NO`
            ORDER BY `CREATE_DATE` DESC NULLS LAST
        ) AS `rn`
    FROM {bronze_schema}.`t_cancel_cnote_api`
    WHERE `API_CNOTE_NO` IS NOT NULL
),
cnote_api AS (
    SELECT
        c.`CNOTE_NO` AS `CNOTE_NO`,
        coalesce(a.`CREATE_DATE`, c.`CNOTE_CRDATE`) AS `DB_CRDATE`,
        CASE
            WHEN upper(toString(a.`APICUST_SHIPMENT_TYPE`)) = 'INTERNATIONAL' THEN 'Intl'
            WHEN c.`CNOTE_ORIGIN` = c.`CNOTE_DESTINATION` THEN 'Intracity'
            WHEN left(toString(c.`CNOTE_ORIGIN`), 3) = left(toString(c.`CNOTE_DESTINATION`), 3) THEN 'Intercity'
            ELSE 'Domestic'
        END AS `SHIPMENT_TYPE`,
        c.`CNOTE_DATE`,
        c.`CNOTE_CRDATE`,
        c.`CNOTE_CUST_NO`,
        c.`CNOTE_USER`,
        c.`CNOTE_ORIGIN`,
        c.`CNOTE_DESTINATION`,
        c.`CNOTE_SERVICES_CODE`,
        c.`CNOTE_WEIGHT`,
        c.`CNOTE_AMOUNT`,
        c.`CNOTE_CANCEL`,
        CAST(NULL AS Nullable(String)) AS `CNOTE_USER_ZONE`,
        a.`CREATE_DATE` AS `API_DATE`,
        a.`APICUST_CUST_NO` AS `API_CUST_ID`,
        a.`APICUST_ORIGIN` AS `API_ORIGIN`,
        a.`APICUST_DESTINATION` AS `API_DESTINATION`,
        a.`APICUST_SERVICES_CODE` AS `API_SERVICES`,
        a.`APICUST_SHIPMENT_TYPE` AS `API_FM_TYPE`,
        a.`APICUST_WEIGHT` AS `API_WEIGHT`,
        t.`CREATE_DATE` AS `API_CANCELED_DATE`
    FROM {bronze_schema}.`cms_cnote` c
    LEFT JOIN api_ranked a
        ON c.`CNOTE_NO` = a.`APICUST_CNOTE_NO`
       AND a.`rn` = 1
    LEFT JOIN cancel_api_ranked t
        ON a.`APICUST_CNOTE_NO` = t.`API_CNOTE_NO`
       AND t.`rn` = 1
),
recv_events AS (
    SELECT
        d.`DRCNOTE_CNOTE_NO` AS `RECV_CNOTE_NO`,
        d.`DRCNOTE_NO` AS `RECV_NO`,
        d.`DRCNOTE_TDATE` AS `RECV_SCAN_DATE`,
        m.`MRCNOTE_USER_ID` AS `RECV_USER_ID`,
        CAST(NULL AS Nullable(String)) AS `RECV_ZONE`,
        m.`MRCNOTE_DATE` AS `RECV_DATE`,
        m.`MRCNOTE_SIGNDATE` AS `RECV_SIGN_DATE`,
        0 AS `source_priority`
    FROM {bronze_schema}.`cms_drcnote` d
    LEFT JOIN {bronze_schema}.`cms_mrcnote` m
        ON d.`DRCNOTE_NO` = m.`MRCNOTE_NO`
    UNION ALL
    SELECT
        d.`DHI_CNOTE_NO` AS `RECV_CNOTE_NO`,
        d.`DHI_NO` AS `RECV_NO`,
        d.`CDATE` AS `RECV_SCAN_DATE`,
        m.`MHI_UID` AS `RECV_USER_ID`,
        CAST(NULL AS Nullable(String)) AS `RECV_ZONE`,
        m.`MHI_DATE` AS `RECV_DATE`,
        m.`MHI_APPROVE_DATE` AS `RECV_SIGN_DATE`,
        1 AS `source_priority`
    FROM {bronze_schema}.`cms_dhi_hoc` d
    LEFT JOIN {bronze_schema}.`cms_mhi_hoc` m
        ON d.`DHI_NO` = m.`MHI_NO`
),
stg_recv AS (
    SELECT * EXCEPT (`rn`, `source_priority`)
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY `RECV_CNOTE_NO`
                ORDER BY `RECV_SCAN_DATE` ASC NULLS LAST, `source_priority` ASC
            ) AS `rn`
        FROM recv_events
        WHERE `RECV_CNOTE_NO` IS NOT NULL
    )
    WHERE `rn` = 1
),
manifest_asset AS (
    SELECT
        f.`MFCNOTE_MAN_NO` AS `MF_NO`,
        f.`MFCNOTE_NO` AS `MF_CNOTE_NO`,
        f.`MFCNOTE_BAG_NO` AS `MF_BAG_NO`,
        f.`MFCNOTE_CRDATE` AS `MF_SCAN_DATE`,
        m.`MANIFEST_DATE` AS `MF_DATE`,
        m.`MANIFEST_CRDATE` AS `MF_CRDATE`,
        m.`MANIFEST_ROUTE` AS `MF_ROUTE`,
        m.`MANIFEST_FROM` AS `MF_FROM`,
        m.`MANIFEST_THRU` AS `MF_THRU`,
        toString(m.`MANIFEST_CODE`) AS `MF_CODE`,
        m.`MANIFEST_UID` AS `MF_USER`,
        m.`MANIFEST_APPROVED` AS `MF_APPROVED`
    FROM {bronze_schema}.`cms_mfcnote` f
    LEFT JOIN {bronze_schema}.`cms_manifest` m
        ON f.`MFCNOTE_MAN_NO` = m.`MANIFEST_NO`
),
pra_runsheet_events AS (
    SELECT
        d.`DRSHEET_CNOTE_NO` AS `DRI_PRA_CNOTE_NO`,
        d.`DRSHEET_NO` AS `DRI_PRA_EVENT_NO`,
        m.`MRSHEET_DATE` AS `DRI_PRA_EVENT_DATE`,
        m.`MRSHEET_UID` AS `DRI_PRA_EVENT_USER`,
        CAST(NULL AS Nullable(String)) AS `DRI_PRA_EVENT_ZONE`
    FROM {bronze_schema}.`cms_drsheet_pra` d
    LEFT JOIN {bronze_schema}.`cms_mrsheet_pra` m
        ON d.`DRSHEET_NO` = m.`MRSHEET_NO`
),
pra_runsheet AS (
    SELECT
        `DRI_PRA_CNOTE_NO`,
        argMin(`DRI_PRA_EVENT_NO`, `DRI_PRA_EVENT_DATE`) AS `DRI_PRA_NO`,
        min(`DRI_PRA_EVENT_DATE`) AS `DRI_PRA_DATE`,
        argMin(`DRI_PRA_EVENT_USER`, `DRI_PRA_EVENT_DATE`) AS `DRI_PRA_USER`,
        argMin(`DRI_PRA_EVENT_ZONE`, `DRI_PRA_EVENT_DATE`) AS `DRI_PRA_ZONE`
    FROM pra_runsheet_events
    WHERE `DRI_PRA_CNOTE_NO` IS NOT NULL
    GROUP BY `DRI_PRA_CNOTE_NO`
),
manifest_grouped AS (
    SELECT
        `MF_CNOTE_NO`,
        argMinIf(`MF_NO`, `MF_SCAN_DATE`, `MF_CODE` = '1') AS `OM_NO`,
        argMinIf(`MF_BAG_NO`, `MF_SCAN_DATE`, `MF_CODE` = '1') AS `OM_BAG_NO`,
        minIf(`MF_SCAN_DATE`, `MF_CODE` = '1') AS `OM_SCAN_DATE`,
        argMinIf(`MF_DATE`, `MF_SCAN_DATE`, `MF_CODE` = '1') AS `OM_DATE`,
        argMinIf(`MF_USER`, `MF_SCAN_DATE`, `MF_CODE` = '1') AS `OM_USER`,
        CAST(NULL AS Nullable(String)) AS `OM_ZONE`,
        countIf(`MF_CODE` = '2') AS `TM_COUNT`,
        argMinIf(`MF_NO`, `MF_SCAN_DATE`, `MF_CODE` = '2') AS `TM_FIRST_NO`,
        minIf(`MF_SCAN_DATE`, `MF_CODE` = '2') AS `TM_FIRST_SCAN_DATE`,
        argMinIf(`MF_DATE`, `MF_SCAN_DATE`, `MF_CODE` = '2') AS `TM_FIRST_DATE`,
        argMinIf(`MF_USER`, `MF_SCAN_DATE`, `MF_CODE` = '2') AS `TM_FIRST_USER`,
        CAST(NULL AS Nullable(String)) AS `TM_FIRST_ZONE`,
        argMaxIf(`MF_NO`, `MF_SCAN_DATE`, `MF_CODE` = '2') AS `TM_LAST_NO`,
        maxIf(`MF_SCAN_DATE`, `MF_CODE` = '2') AS `TM_LAST_SCAN_DATE`,
        argMaxIf(`MF_DATE`, `MF_SCAN_DATE`, `MF_CODE` = '2') AS `TM_LAST_DATE`,
        argMaxIf(`MF_USER`, `MF_SCAN_DATE`, `MF_CODE` = '2') AS `TM_LAST_USER`,
        CAST(NULL AS Nullable(String)) AS `TM_LAST_ZONE`,
        argMinIf(`MF_NO`, `MF_SCAN_DATE`, `MF_CODE` = '3') AS `IM_NO`,
        minIf(`MF_SCAN_DATE`, `MF_CODE` = '3') AS `IM_SCAN_DATE`,
        argMinIf(`MF_DATE`, `MF_SCAN_DATE`, `MF_CODE` = '3') AS `IM_DATE`,
        argMinIf(`MF_USER`, `MF_SCAN_DATE`, `MF_CODE` = '3') AS `IM_USER`,
        CAST(NULL AS Nullable(String)) AS `IM_ZONE`,
        CASE
            WHEN countIf(`MF_CODE` = '1') = 0 THEN 'Null OM'
            WHEN countIf(`MF_CODE` = '3') = 0 THEN 'Null OM-IM'
            ELSE 'Complete'
        END AS `MANIFEST_TAG`
    FROM manifest_asset
    WHERE `MF_CNOTE_NO` IS NOT NULL
    GROUP BY `MF_CNOTE_NO`
),
stg_manifest AS (
    SELECT
        m.*,
        p.`DRI_PRA_NO`,
        p.`DRI_PRA_DATE`,
        p.`DRI_PRA_USER`,
        p.`DRI_PRA_ZONE`
    FROM manifest_grouped m
    FULL OUTER JOIN pra_runsheet p
        ON m.`MF_CNOTE_NO` = p.`DRI_PRA_CNOTE_NO`
),
smu_asset AS (
    SELECT
        d.`DSMU_BAG_NO` AS `SMU_BAG`,
        d.`ESB_TIME` AS `SMU_ESB_TIME`,
        m.`MSMU_NO` AS `SMU_NO`,
        m.`MSMU_DATE` AS `SMU_DATE`,
        m.`MSMU_FLIGHT_DATE` AS `SMU_FLIGHT_DATE`,
        m.`MSMU_WRH_DATE` AS `SMU_WH_DATE`,
        m.`MSMU_OFF_DATE` AS `SMU_OFF_DATE`,
        m.`MSMU_MODA` AS `SMU_MODA`
    FROM {bronze_schema}.`cms_dsmu` d
    LEFT JOIN {bronze_schema}.`cms_msmu` m
        ON d.`DSMU_NO` = m.`MSMU_NO`
    WHERE d.`DSMU_BAG_NO` IS NOT NULL
      AND d.`DSMU_NO` IS NOT NULL
),
smu_ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY `SMU_BAG`
            ORDER BY `SMU_ESB_TIME` ASC NULLS LAST, `SMU_NO` ASC
        ) AS `rn`
    FROM smu_asset
),
stg_sm AS (
    SELECT
        `SMU_BAG` AS `SM_BAG`,
        count() AS `SM_COUNT`,
        argMin(`SMU_NO`, `rn`) AS `SM_FIRST_NO`,
        argMin(`SMU_ESB_TIME`, `rn`) AS `SM_FIRST_ESB_TIME`,
        argMin(`SMU_DATE`, `rn`) AS `SM_FIRST_DATE`,
        argMin(`SMU_FLIGHT_DATE`, `rn`) AS `SM_FIRST_FLIGHT_DATE`,
        argMin(`SMU_WH_DATE`, `rn`) AS `SM_FIRST_WH_DATE`,
        argMin(`SMU_OFF_DATE`, `rn`) AS `SM_FIRST_OFF_DATE`,
        argMin(`SMU_MODA`, `rn`) AS `SM_FIRST_MODA`,
        maxIf(`SMU_NO`, `rn` = 2) AS `SM_SECOND_NO`,
        maxIf(`SMU_ESB_TIME`, `rn` = 2) AS `SM_SECOND_ESB_TIME`,
        maxIf(`SMU_DATE`, `rn` = 2) AS `SM_SECOND_DATE`,
        maxIf(`SMU_FLIGHT_DATE`, `rn` = 2) AS `SM_SECOND_FLIGHT_DATE`,
        maxIf(`SMU_WH_DATE`, `rn` = 2) AS `SM_SECOND_WH_DATE`,
        maxIf(`SMU_OFF_DATE`, `rn` = 2) AS `SM_SECOND_OFF_DATE`,
        maxIf(`SMU_MODA`, `rn` = 2) AS `SM_SECOND_MODA`,
        argMax(`SMU_NO`, `rn`) AS `SM_LAST_NO`,
        argMax(`SMU_ESB_TIME`, `rn`) AS `SM_LAST_ESB_TIME`,
        argMax(`SMU_DATE`, `rn`) AS `SM_LAST_DATE`,
        argMax(`SMU_FLIGHT_DATE`, `rn`) AS `SM_LAST_FLIGHT_DATE`,
        argMax(`SMU_WH_DATE`, `rn`) AS `SM_LAST_WH_DATE`,
        argMax(`SMU_OFF_DATE`, `rn`) AS `SM_LAST_OFF_DATE`,
        argMax(`SMU_MODA`, `rn`) AS `SM_LAST_MODA`
    FROM smu_ranked
    GROUP BY `SMU_BAG`
),
hvo_hvi_events AS (
    SELECT
        d.`DHOCNOTE_CNOTE_NO` AS `CNOTE_NO`,
        m.`MHOCNOTE_NO` AS `HVO_NO`,
        m.`MHOCNOTE_DATE` AS `HVO_DATE`,
        m.`MHOCNOTE_USER_ID` AS `HVO_USER`,
        m.`MHOCNOTE_ZONE` AS `HVO_ZONE`,
        CAST(NULL AS Nullable(String)) AS `HVI_NO`,
        CAST(NULL AS Nullable(DateTime)) AS `HVI_DATE`,
        CAST(NULL AS Nullable(String)) AS `HVI_USER`,
        CAST(NULL AS Nullable(String)) AS `HVI_ZONE`,
        'HVO_ONLY' AS `event_tag`
    FROM {bronze_schema}.`cms_dhocnote` d
    LEFT JOIN {bronze_schema}.`cms_mhocnote` m
        ON d.`DHOCNOTE_NO` = m.`MHOCNOTE_NO`
    UNION ALL
    SELECT
        d.`DHICNOTE_CNOTE_NO` AS `CNOTE_NO`,
        CAST(NULL AS Nullable(String)) AS `HVO_NO`,
        CAST(NULL AS Nullable(DateTime)) AS `HVO_DATE`,
        CAST(NULL AS Nullable(String)) AS `HVO_USER`,
        CAST(NULL AS Nullable(String)) AS `HVO_ZONE`,
        m.`MHICNOTE_NO` AS `HVI_NO`,
        m.`MHICNOTE_DATE` AS `HVI_DATE`,
        m.`MHICNOTE_USER_ID` AS `HVI_USER`,
        m.`MHICNOTE_ZONE` AS `HVI_ZONE`,
        'HVI_ONLY' AS `event_tag`
    FROM {bronze_schema}.`cms_dhicnote` d
    LEFT JOIN {bronze_schema}.`cms_mhicnote` m
        ON d.`DHICNOTE_NO` = m.`MHICNOTE_NO`
),
hvo_hvi_ranked AS (
    SELECT
        *,
        coalesce(`HVO_DATE`, `HVI_DATE`) AS `event_date`,
        row_number() OVER (
            PARTITION BY `CNOTE_NO`
            ORDER BY coalesce(`HVO_DATE`, `HVI_DATE`) ASC NULLS LAST
        ) AS `rn`
    FROM hvo_hvi_events
    WHERE `CNOTE_NO` IS NOT NULL
),
stg_hvo_hvi AS (
    SELECT
        `CNOTE_NO` AS `HVO_CNOTE_NO`,
        count() AS `HVO_COUNT`,
        if(countIf(`event_tag` = 'HVO_ONLY') > 0 AND countIf(`event_tag` = 'HVI_ONLY') > 0, 'Mixed', 'Incomplete') AS `HVO_HVI_TAG`,
        argMin(`HVO_NO`, `rn`) AS `HVO_FIRST_NO`,
        argMin(`HVO_DATE`, `rn`) AS `HVO_FIRST_DATE`,
        argMin(`HVO_USER`, `rn`) AS `HVO_FIRST_USER`,
        argMin(`HVO_ZONE`, `rn`) AS `HVO_FIRST_ZONE`,
        argMin(`HVI_NO`, `rn`) AS `HVI_FIRST_NO`,
        argMin(`HVI_DATE`, `rn`) AS `HVI_FIRST_DATE`,
        argMin(`HVI_USER`, `rn`) AS `HVI_FIRST_USER`,
        argMin(`HVI_ZONE`, `rn`) AS `HVI_FIRST_ZONE`,
        argMax(`HVO_NO`, `rn`) AS `HVO_LAST_NO`,
        argMax(`HVO_DATE`, `rn`) AS `HVO_LAST_DATE`,
        argMax(`HVO_USER`, `rn`) AS `HVO_LAST_USER`,
        argMax(`HVO_ZONE`, `rn`) AS `HVO_LAST_ZONE`,
        argMax(`HVI_NO`, `rn`) AS `HVI_LAST_NO`,
        argMax(`HVI_DATE`, `rn`) AS `HVI_LAST_DATE`,
        argMax(`HVI_USER`, `rn`) AS `HVI_LAST_USER`,
        argMax(`HVI_ZONE`, `rn`) AS `HVI_LAST_ZONE`
    FROM hvo_hvi_ranked
    GROUP BY `CNOTE_NO`
),
mts_mti_events AS (
    SELECT
        d.`CNOTE_NO` AS `CNOTE_NO`,
        d.`DMANIFEST_NO` AS `MTS_NO`,
        d.`ESB_TIME` AS `MTS_DATE`,
        m.`MANIFEST_UID` AS `MTS_USER`,
        CAST(NULL AS Nullable(String)) AS `MTS_ZONE`,
        CAST(NULL AS Nullable(String)) AS `MTI_NO`,
        CAST(NULL AS Nullable(DateTime)) AS `MTI_DATE`,
        CAST(NULL AS Nullable(String)) AS `MTI_USER`,
        CAST(NULL AS Nullable(String)) AS `MTI_ZONE`,
        'MTS_ONLY' AS `event_tag`
    FROM {bronze_schema}.`cms_cost_dtransit_agen` d
    LEFT JOIN {bronze_schema}.`cms_cost_mtransit_agen` m
        ON d.`DMANIFEST_NO` = m.`MANIFEST_NO`
    WHERE d.`DMANIFEST_DOC_REF` IS NULL
    UNION ALL
    SELECT
        d.`CNOTE_NO` AS `CNOTE_NO`,
        CAST(NULL AS Nullable(String)) AS `MTS_NO`,
        CAST(NULL AS Nullable(DateTime)) AS `MTS_DATE`,
        CAST(NULL AS Nullable(String)) AS `MTS_USER`,
        CAST(NULL AS Nullable(String)) AS `MTS_ZONE`,
        d.`DMANIFEST_NO` AS `MTI_NO`,
        d.`ESB_TIME` AS `MTI_DATE`,
        m.`MANIFEST_UID` AS `MTI_USER`,
        CAST(NULL AS Nullable(String)) AS `MTI_ZONE`,
        'MTI_ONLY' AS `event_tag`
    FROM {bronze_schema}.`cms_cost_dtransit_agen` d
    LEFT JOIN {bronze_schema}.`cms_cost_mtransit_agen` m
        ON d.`DMANIFEST_NO` = m.`MANIFEST_NO`
    WHERE d.`DMANIFEST_DOC_REF` IS NOT NULL
),
mts_mti_ranked AS (
    SELECT
        *,
        coalesce(`MTS_DATE`, `MTI_DATE`) AS `event_date`,
        row_number() OVER (
            PARTITION BY `CNOTE_NO`
            ORDER BY coalesce(`MTS_DATE`, `MTI_DATE`) ASC NULLS LAST
        ) AS `rn`
    FROM mts_mti_events
    WHERE `CNOTE_NO` IS NOT NULL
),
stg_mts_mti AS (
    SELECT
        `CNOTE_NO` AS `MTS_CNOTE_NO`,
        count() AS `MTS_COUNT`,
        if(countIf(`event_tag` = 'MTS_ONLY') > 0 AND countIf(`event_tag` = 'MTI_ONLY') > 0, 'Mixed', 'Incomplete') AS `MTS_MTI_TAG`,
        argMin(`MTS_NO`, `rn`) AS `MTS_FIRST_NO`,
        argMin(`MTS_DATE`, `rn`) AS `MTS_FIRST_DATE`,
        argMin(`MTS_USER`, `rn`) AS `MTS_FIRST_USER`,
        argMin(`MTS_ZONE`, `rn`) AS `MTS_FIRST_ZONE`,
        argMin(`MTI_NO`, `rn`) AS `MTI_FIRST_NO`,
        argMin(`MTI_DATE`, `rn`) AS `MTI_FIRST_DATE`,
        argMin(`MTI_USER`, `rn`) AS `MTI_FIRST_USER`,
        argMin(`MTI_ZONE`, `rn`) AS `MTI_FIRST_ZONE`,
        argMax(`MTS_NO`, `rn`) AS `MTS_LAST_NO`,
        argMax(`MTS_DATE`, `rn`) AS `MTS_LAST_DATE`,
        argMax(`MTS_USER`, `rn`) AS `MTS_LAST_USER`,
        argMax(`MTS_ZONE`, `rn`) AS `MTS_LAST_ZONE`,
        argMax(`MTI_NO`, `rn`) AS `MTI_LAST_NO`,
        argMax(`MTI_DATE`, `rn`) AS `MTI_LAST_DATE`,
        argMax(`MTI_USER`, `rn`) AS `MTI_LAST_USER`,
        argMax(`MTI_ZONE`, `rn`) AS `MTI_LAST_ZONE`
    FROM mts_mti_ranked
    GROUP BY `CNOTE_NO`
),
delivery_events AS (
    SELECT
        d.`DRSHEET_CNOTE_NO` AS `CNOTE_NO`,
        d.`DRSHEET_NO` AS `DRI_NO`,
        m.`MRSHEET_DATE` AS `DRI_DATE`,
        m.`MRSHEET_UID` AS `DRI_USER`,
        CAST(NULL AS Nullable(String)) AS `DRI_ZONE`,
        m.`MRSHEET_COURIER_ID` AS `DRI_COURIER_ID`,
        d.`DRSHEET_DATE` AS `DRI_POD_DATE`,
        d.`DRSHEET_STATUS` AS `DRI_POD_STATUS`
    FROM {bronze_schema}.`cms_drsheet` d
    LEFT JOIN {bronze_schema}.`cms_mrsheet` m
        ON d.`DRSHEET_NO` = m.`MRSHEET_NO`
    WHERE d.`DRSHEET_CNOTE_NO` IS NOT NULL
),
delivery_ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY `CNOTE_NO`
            ORDER BY `DRI_DATE` ASC NULLS LAST, `DRI_NO` ASC
        ) AS `rn`
    FROM delivery_events
),
stg_dri AS (
    SELECT
        `CNOTE_NO`,
        count() AS `DRI_ATTEMPT`,
        argMin(`DRI_NO`, `rn`) AS `DRI_FIRST_NO`,
        argMin(`DRI_DATE`, `rn`) AS `DRI_FIRST_DATE`,
        argMin(`DRI_USER`, `rn`) AS `DRI_FIRST_USER`,
        argMin(`DRI_ZONE`, `rn`) AS `DRI_FIRST_ZONE`,
        argMin(`DRI_COURIER_ID`, `rn`) AS `DRI_FIRST_COURIER_ID`,
        argMin(`DRI_POD_DATE`, `rn`) AS `DRI_FIRST_POD_DATE`,
        argMin(`DRI_POD_STATUS`, `rn`) AS `DRI_FIRST_POD_STATUS`,
        argMax(`DRI_NO`, `rn`) AS `DRI_LAST_NO`,
        argMax(`DRI_DATE`, `rn`) AS `DRI_LAST_DATE`,
        argMax(`DRI_USER`, `rn`) AS `DRI_LAST_USER`,
        argMax(`DRI_ZONE`, `rn`) AS `DRI_LAST_ZONE`,
        argMax(`DRI_COURIER_ID`, `rn`) AS `DRI_LAST_COURIER_ID`,
        argMax(`DRI_POD_DATE`, `rn`) AS `DRI_LAST_POD_DATE`,
        argMax(`DRI_POD_STATUS`, `rn`) AS `DRI_LAST_POD_STATUS`
    FROM delivery_ranked
    GROUP BY `CNOTE_NO`
),
hrs_events AS (
    SELECT
        d.`DHOV_RSHEET_CNOTE` AS `CNOTE_NO`,
        m.`MHOV_RSHEET_NO` AS `HRS_NO`,
        m.`MHOV_RSHEET_DATE` AS `HRS_DATE`,
        m.`MHOV_RSHEET_UID` AS `HRS_USER`,
        CAST(NULL AS Nullable(String)) AS `HRS_ZONE`
    FROM {bronze_schema}.`cms_dhov_rsheet` d
    LEFT JOIN {bronze_schema}.`cms_mhov_rsheet` m
        ON d.`DHOV_RSHEET_NO` = m.`MHOV_RSHEET_NO`
    WHERE d.`DHOV_RSHEET_CNOTE` IS NOT NULL
),
hrs_ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY `CNOTE_NO`
            ORDER BY `HRS_DATE` ASC NULLS LAST, `HRS_NO` ASC
        ) AS `rn`
    FROM hrs_events
),
stg_hrs AS (
    SELECT
        `CNOTE_NO`,
        count() AS `HRS_COUNT`,
        argMin(`HRS_NO`, `rn`) AS `HRS_FIRST_NO`,
        argMin(`HRS_DATE`, `rn`) AS `HRS_FIRST_DATE`,
        argMin(`HRS_USER`, `rn`) AS `HRS_FIRST_USER`,
        argMin(`HRS_ZONE`, `rn`) AS `HRS_FIRST_ZONE`,
        argMax(`HRS_NO`, `rn`) AS `HRS_LAST_NO`,
        argMax(`HRS_DATE`, `rn`) AS `HRS_LAST_DATE`,
        argMax(`HRS_USER`, `rn`) AS `HRS_LAST_USER`,
        argMax(`HRS_ZONE`, `rn`) AS `HRS_LAST_ZONE`
    FROM hrs_ranked
    GROUP BY `CNOTE_NO`
),
stg_dri_hrs AS (
    SELECT
        coalesce(d.`CNOTE_NO`, h.`CNOTE_NO`) AS `DRI_CNOTE_NO`,
        d.`DRI_ATTEMPT`,
        d.`DRI_FIRST_NO`,
        d.`DRI_FIRST_DATE`,
        d.`DRI_FIRST_USER`,
        d.`DRI_FIRST_ZONE`,
        d.`DRI_FIRST_COURIER_ID`,
        d.`DRI_FIRST_POD_DATE`,
        d.`DRI_FIRST_POD_STATUS`,
        d.`DRI_LAST_NO`,
        d.`DRI_LAST_DATE`,
        d.`DRI_LAST_USER`,
        d.`DRI_LAST_ZONE`,
        d.`DRI_LAST_COURIER_ID`,
        d.`DRI_LAST_POD_DATE`,
        d.`DRI_LAST_POD_STATUS`,
        h.`HRS_COUNT`,
        h.`HRS_FIRST_NO`,
        h.`HRS_FIRST_DATE`,
        h.`HRS_FIRST_USER`,
        h.`HRS_FIRST_ZONE`,
        h.`HRS_LAST_NO`,
        h.`HRS_LAST_DATE`,
        h.`HRS_LAST_USER`,
        h.`HRS_LAST_ZONE`,
        if(h.`CNOTE_NO` IS NOT NULL AND d.`CNOTE_NO` IS NULL, 'Incomplete', 'Complete') AS `HRS_TAG`
    FROM stg_dri d
    FULL OUTER JOIN stg_hrs h
        ON d.`CNOTE_NO` = h.`CNOTE_NO`
),
irreg_events AS (
    SELECT
        d.`DSTATUS_CNOTE_NO` AS `IRG_CNOTE_NO`,
        m.`MSTATUS_NO` AS `IRG_NO`,
        d.`DSTATUS_STATUS` AS `IRG_STATUS`,
        d.`DSTATUS_STATUS_DATE` AS `IRG_DATE`,
        m.`MSTATUS_UID` AS `IRG_USER`,
        m.`MSTATUS_ZONE_CODE` AS `IRG_ZONE`,
        row_number() OVER (
            PARTITION BY d.`DSTATUS_CNOTE_NO`
            ORDER BY d.`DSTATUS_STATUS_DATE` DESC NULLS LAST, m.`MSTATUS_NO` DESC
        ) AS `rn`,
        count() OVER (PARTITION BY d.`DSTATUS_CNOTE_NO`) AS `IRG_COUNT`
    FROM {bronze_schema}.`cms_dstatus` d
    LEFT JOIN {bronze_schema}.`cms_mstatus` m
        ON d.`DSTATUS_NO` = m.`MSTATUS_NO`
    WHERE d.`DSTATUS_CNOTE_NO` IS NOT NULL
),
stg_irreg AS (
    SELECT
        `IRG_CNOTE_NO`,
        `IRG_COUNT`,
        `IRG_NO`,
        `IRG_STATUS`,
        `IRG_DATE`,
        `IRG_USER`,
        `IRG_ZONE`
    FROM irreg_events
    WHERE `rn` = 1
)
SELECT
    c.*,
    r.`RECV_CNOTE_NO`,
    r.`RECV_NO`,
    r.`RECV_SCAN_DATE`,
    r.`RECV_USER_ID`,
    r.`RECV_ZONE`,
    r.`RECV_DATE`,
    m.`MF_CNOTE_NO`,
    m.`OM_NO`,
    m.`OM_BAG_NO`,
    m.`OM_SCAN_DATE`,
    m.`OM_DATE`,
    m.`OM_USER`,
    m.`OM_ZONE`,
    m.`TM_COUNT`,
    m.`TM_FIRST_NO`,
    m.`TM_FIRST_SCAN_DATE`,
    m.`TM_FIRST_DATE`,
    m.`TM_FIRST_USER`,
    m.`TM_FIRST_ZONE`,
    m.`TM_LAST_NO`,
    m.`TM_LAST_SCAN_DATE`,
    m.`TM_LAST_DATE`,
    m.`TM_LAST_USER`,
    m.`TM_LAST_ZONE`,
    m.`IM_NO`,
    m.`IM_SCAN_DATE`,
    m.`IM_DATE`,
    m.`IM_USER`,
    m.`IM_ZONE`,
    m.`MANIFEST_TAG`,
    m.`DRI_PRA_NO`,
    m.`DRI_PRA_DATE`,
    m.`DRI_PRA_USER`,
    m.`DRI_PRA_ZONE`,
    sm.`SM_BAG`,
    sm.`SM_COUNT`,
    sm.`SM_FIRST_NO`,
    sm.`SM_FIRST_ESB_TIME`,
    sm.`SM_FIRST_DATE`,
    sm.`SM_FIRST_FLIGHT_DATE`,
    sm.`SM_FIRST_WH_DATE`,
    sm.`SM_FIRST_OFF_DATE`,
    sm.`SM_FIRST_MODA`,
    sm.`SM_SECOND_NO`,
    sm.`SM_SECOND_ESB_TIME`,
    sm.`SM_SECOND_DATE`,
    sm.`SM_SECOND_FLIGHT_DATE`,
    sm.`SM_SECOND_WH_DATE`,
    sm.`SM_SECOND_OFF_DATE`,
    sm.`SM_SECOND_MODA`,
    sm.`SM_LAST_NO`,
    sm.`SM_LAST_ESB_TIME`,
    sm.`SM_LAST_DATE`,
    sm.`SM_LAST_FLIGHT_DATE`,
    sm.`SM_LAST_WH_DATE`,
    sm.`SM_LAST_OFF_DATE`,
    sm.`SM_LAST_MODA`,
    h.`HVO_CNOTE_NO`,
    h.`HVO_COUNT`,
    h.`HVO_HVI_TAG`,
    h.`HVO_FIRST_NO`,
    h.`HVO_FIRST_DATE`,
    h.`HVO_FIRST_USER`,
    h.`HVO_FIRST_ZONE`,
    h.`HVI_FIRST_NO`,
    h.`HVI_FIRST_DATE`,
    h.`HVI_FIRST_USER`,
    h.`HVI_FIRST_ZONE`,
    h.`HVO_LAST_NO`,
    h.`HVO_LAST_DATE`,
    h.`HVO_LAST_USER`,
    h.`HVO_LAST_ZONE`,
    h.`HVI_LAST_NO`,
    h.`HVI_LAST_DATE`,
    h.`HVI_LAST_USER`,
    h.`HVI_LAST_ZONE`,
    mt.`MTS_CNOTE_NO`,
    mt.`MTS_COUNT`,
    mt.`MTS_MTI_TAG`,
    mt.`MTS_FIRST_NO`,
    mt.`MTS_FIRST_DATE`,
    mt.`MTS_FIRST_USER`,
    mt.`MTS_FIRST_ZONE`,
    mt.`MTI_FIRST_NO`,
    mt.`MTI_FIRST_DATE`,
    mt.`MTI_FIRST_USER`,
    mt.`MTI_FIRST_ZONE`,
    mt.`MTS_LAST_NO`,
    mt.`MTS_LAST_DATE`,
    mt.`MTS_LAST_USER`,
    mt.`MTS_LAST_ZONE`,
    mt.`MTI_LAST_NO`,
    mt.`MTI_LAST_DATE`,
    mt.`MTI_LAST_USER`,
    mt.`MTI_LAST_ZONE`,
    d.`DRI_CNOTE_NO`,
    d.`DRI_ATTEMPT`,
    d.`DRI_FIRST_NO`,
    d.`DRI_FIRST_DATE`,
    d.`DRI_FIRST_USER`,
    d.`DRI_FIRST_ZONE`,
    d.`DRI_FIRST_COURIER_ID`,
    d.`DRI_FIRST_POD_DATE`,
    d.`DRI_FIRST_POD_STATUS`,
    d.`DRI_LAST_NO`,
    d.`DRI_LAST_DATE`,
    d.`DRI_LAST_USER`,
    d.`DRI_LAST_ZONE`,
    d.`DRI_LAST_COURIER_ID`,
    d.`DRI_LAST_POD_DATE`,
    d.`DRI_LAST_POD_STATUS`,
    d.`HRS_COUNT`,
    d.`HRS_FIRST_NO`,
    d.`HRS_FIRST_DATE`,
    d.`HRS_FIRST_USER`,
    d.`HRS_FIRST_ZONE`,
    d.`HRS_LAST_NO`,
    d.`HRS_LAST_DATE`,
    d.`HRS_LAST_USER`,
    d.`HRS_LAST_ZONE`,
    d.`HRS_TAG`,
    i.`IRG_CNOTE_NO`,
    i.`IRG_COUNT`,
    i.`IRG_NO`,
    i.`IRG_STATUS`,
    i.`IRG_DATE`,
    i.`IRG_USER`,
    i.`IRG_ZONE`
FROM cnote_api c
LEFT JOIN stg_recv r
    ON c.`CNOTE_NO` = r.`RECV_CNOTE_NO`
LEFT JOIN stg_manifest m
    ON c.`CNOTE_NO` = m.`MF_CNOTE_NO`
LEFT JOIN stg_sm sm
    ON m.`OM_BAG_NO` = sm.`SM_BAG`
LEFT JOIN stg_hvo_hvi h
    ON c.`CNOTE_NO` = h.`HVO_CNOTE_NO`
LEFT JOIN stg_mts_mti mt
    ON c.`CNOTE_NO` = mt.`MTS_CNOTE_NO`
LEFT JOIN stg_dri_hrs d
    ON c.`CNOTE_NO` = d.`DRI_CNOTE_NO`
LEFT JOIN stg_irreg i
    ON c.`CNOTE_NO` = i.`IRG_CNOTE_NO`
