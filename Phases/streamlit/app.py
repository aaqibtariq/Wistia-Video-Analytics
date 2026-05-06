import streamlit as st
import pandas as pd
from pyathena import connect

st.set_page_config(
    page_title="Wistia Video Analytics",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Wistia Video Analytics Dashboard")

st.write("Dashboard connected to AWS Athena and S3 Delta Gold table.")

AWS_REGION = "us-east-1"
DATABASE = "wistia_analytics"
S3_STAGING_DIR = "s3://wistia-video-analytics-at/athena-results/"

@st.cache_data(ttl=300)
def load_data():
    conn = connect(
        s3_staging_dir=S3_STAGING_DIR,
        region_name=AWS_REGION,
        schema_name=DATABASE
    )

    query = """
        SELECT
            media_id,
            play_count,
            play_rate,
            engagement,
            hours_watched,
            visitors,
            load_date,
            engagement_date,
            surrogate_key,
            load_timestamp
        FROM gold_fact_media_engagement
        ORDER BY load_date DESC, media_id
    """

    df = pd.read_sql(query, conn)
    return df

try:
    df = load_data()

    st.success("Connected to Athena successfully!")

    st.subheader("Raw Gold Table Preview")
    st.dataframe(df)

except Exception as e:
    st.error("Failed to load data from Athena.")
    st.exception(e)
