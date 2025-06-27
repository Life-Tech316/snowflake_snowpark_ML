# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()

# conn = st.experimental_connection("snowflake", type="snowflake")

# メインアプリ
st.title("Snowflake × Streamlit APP")

# 1. CUSTOMER_DATA_1000 全件表示
st.header("📋 CUSTOMER_DATA_1000 全件表示")
# conn = session.sql("select * from table")
df_customers = session.sql("SELECT * FROM CUSTOMER_DATA_1000")
st.dataframe(df_customers)

# 2. age, gender, churn 別の平均収入
st.header("📊 年齢・性別・解約別の平均収入")
df_income = session.sql("""
    SELECT
      AGE,
      GENDER,
      CHURN,
      AVG(ANNUAL_INCOME) AS AVG_INCOME
    FROM CUSTOMER_DATA_1000
    GROUP BY AGE, GENDER, CHURN
    ORDER BY AGE, GENDER, CHURN
""")
st.dataframe(df_income)

# 3. グラフ表示用の集計データ取得
# 3-1. 年齢別の平均収入
df_age_income = session.sql("""
    SELECT
      AGE,
      AVG(ANNUAL_INCOME) AS AVG_INCOME
    FROM CUSTOMER_DATA_1000
    GROUP BY AGE
    ORDER BY AGE
""").to_pandas()

# 3-2. 性別別の平均収入
df_gender_income = session.sql("""
    SELECT
      GENDER,
      AVG(ANNUAL_INCOME) AS AVG_INCOME
    FROM CUSTOMER_DATA_1000
    GROUP BY GENDER
    ORDER BY GENDER
""").to_pandas()

# 3-3. 解約有無別の平均収入
df_churn_income = session.sql("""
    SELECT
      CHURN,
      AVG(ANNUAL_INCOME) AS AVG_INCOME
    FROM CUSTOMER_DATA_1000
    GROUP BY CHURN
    ORDER BY CHURN
""").to_pandas()


# グラフ表示
st.subheader("📈 年齢別平均収入")
st.bar_chart(df_age_income, x="AGE", y="AVG_INCOME", use_container_width=True)

st.subheader("📈 性別別平均収入")
st.bar_chart(df_gender_income, x="GENDER", y="AVG_INCOME", use_container_width=True)

st.subheader("📈 解約有無別平均収入")
st.bar_chart(df_churn_income, x="CHURN", y="AVG_INCOME", use_container_width=True)



# 4. 設定されているタスク一覧
st.header("⚙️ 設定されているタスク一覧")
df_tasks = session.sql("SHOW TASKS")
st.dataframe(df_tasks)

# 5. タスク実行履歴 (最新 100 件)
st.header("🕒 タスク実行履歴 (最新 100 件)")
df_history = session.sql("""
    SELECT 
        NAME AS TASK_NAME,
        QUERY_TEXT,
        DATABASE_NAME,
        SCHEMA_NAME,
        COMPLETED_TIME,
        ERROR_CODE,
        ERROR_MESSAGE,
        STATE
    FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
    ORDER BY COMPLETED_TIME DESC
    LIMIT 100
    """)
st.dataframe(df_history)

