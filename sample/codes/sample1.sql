
select * from SAMPLE_SALES;

-- 1. 時系列予測モデル--
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST sample_sales_forecast(
    INPUT_DATA => TABLE(SAMPLE_SALES),
    TIMESTAMP_COLNAME =>'date',
    TARGET_COLNAME => 'SALES'
);

-- 2)推論
CALL sample_sales_forecast!FORECAST(
    FORECASTING_PERIODS => 7
);

-- 3)可視化
WITH
    actual AS(
        SELECT
            DATE as dt,
            SALES as value,
            'actual' as series
        FROM SAMPLE_SALES
    ),
    forecast AS(
        SELECT
            TS,
            FORECAST,
            'forecast' as series
        FROM TABLE(
            sample_sales_forecast!FORECAST(FORECASTING_PERIODS => 7)
            )
    )
SELECT * FROM actual
UNION ALL
SELECT * FROM forecast
;

--2.異常検知--------------------------------

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION sales_ad_model(
    INPUT_DATA => TABLE(
        SELECT date,sales
        FROM sample_sales
        WHERE date <= '2025-02-28'
    ),
    TIMESTAMP_COLNAME => 'DATE',
    TARGET_COLNAME => 'SALES',
    LABEL_COLNAME => ''
);

--　2)推論
SELECT
    ts AS anomaly_date,
    y AS actual_sales,
    forecast AS expected_sales,
    is_anomaly
FROM TABLE(
    sales_ad_model!DETECT_ANOMALIES(
        INPUT_DATA => TABLE(
            SELECT date, sales
            FROM sample_sales
            WHERE date > '2025-02-28'
        ),
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'SALES'
    )
);


-- 3.LLM(Cortex)--------------------------------------------
-- 1)complete

SELECT
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3-8b','snowflake cortexのcomplete関数について教えて'
    ) as result
;


-- テーブル作成（既存の CUSTOMER_FEEDBACK をそのまま使ってもOK）
CREATE OR REPLACE TABLE CUSTOMER_FEEDBACK (
  id      INTEGER,
  content STRING
);

INSERT INTO CUSTOMER_FEEDBACK (id, content) VALUES
  (1, 'I love the new interface—very clean and intuitive. It has improved my workflow.'),
  (2, 'Performance is sluggish when loading large datasets. Sometimes it times out.'),
  (3, 'Great customer support, but the billing dashboard could use more detailed reports.'),
  (4, 'The mobile app crashes frequently on Android 12. Please fix ASAP.'),
  (5, 'Overall excellent product. Would recommend to colleagues, but pricing is a bit high.');

select * from CUSTOMER_FEEDBACK;


SELECT
    id,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3-8b',
        CONCAT(
            'Classify the primary topic of this feedback in ONE WORD (e.g., "UI", "Performance", "Support", "Billing"): ',
            content
        )
    ) AS topic_label
FROM CUSTOMER_FEEDBACK;


-- 2)translate
SELECT
    id,
    content,
    SNOWFLAKE.CORTEX.TRANSLATE(content, 'en', 'ja') AS translated_content
FROM CUSTOMER_FEEDBACK;


-- 3) Summarize
SELECT
    TEXT,
    SNOWFLAKE.CORTEX.SUMMARIZE(TEXT) AS SUMMARY
FROM (
    SELECT
        'Snowflake is a cloud-based data platform that offers flexible and scalable environments to resolve issues with traditional on-premises DWH. Users can have consistent experiences across multiple clouds, including Amazon Web Services, Microsoft Azure, and Google Cloud Platform. Snowflake allows users to create separate virtual warehouses for each workload, enabling performance and cost optimization. It also offers features like zero-copy cloning and time travel for data versioning and secure experimentation. Snowflake prioritizes security with features like row-level access control and masking policies. Data sharing enables seamless collaboration with external parties, and the Snowflake Marketplace provides access to external data. Recently, Snowflake has expanded capabilities with Snowpark for programmatic data manipulation using Python, Java, and Scala, SQL-based machine learning model creation with Snowflake Cortex ML, and LLM functions for natural language processing.'
        AS TEXT
);
        


