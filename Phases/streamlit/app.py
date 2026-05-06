import streamlit as st
import pandas as pd
import plotly.express as px
from pyathena import connect

st.set_page_config(
    page_title="Wistia Video Analytics",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Wistia Video Analytics Dashboard")
st.caption("AWS Glue → S3 Delta → Athena → Streamlit")

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

    return pd.read_sql(query, conn)


try:
    df = load_data()

    df["load_date"] = pd.to_datetime(df["load_date"])

    st.success("Connected to Athena successfully!")

    # Filters
    st.sidebar.header("Filters")

    selected_media = st.sidebar.multiselect(
        "Select Media ID",
        options=sorted(df["media_id"].unique()),
        default=sorted(df["media_id"].unique())
    )

    filtered_df = df[df["media_id"].isin(selected_media)]

    # KPI Cards
    total_plays = int(filtered_df["play_count"].sum())
    total_visitors = int(filtered_df["visitors"].sum())
    avg_play_rate = filtered_df["play_rate"].mean()
    avg_engagement = filtered_df["engagement"].mean()
    total_hours = filtered_df["hours_watched"].sum()

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("Total Plays", f"{total_plays:,}")
    col2.metric("Total Visitors", f"{total_visitors:,}")
    col3.metric("Avg Play Rate", f"{avg_play_rate:.2%}")
    col4.metric("Avg Engagement", f"{avg_engagement:.2%}")
    col5.metric("Hours Watched", f"{total_hours:,.2f}")

    st.divider()

    # Charts
    trend_df = (
        filtered_df
        .groupby("load_date", as_index=False)
        .agg({
            "play_count": "sum",
            "visitors": "sum",
            "hours_watched": "sum",
            "play_rate": "mean",
            "engagement": "mean"
        })
    )

    st.subheader("📈 Plays Over Time")
    fig_plays = px.line(
        trend_df,
        x="load_date",
        y="play_count",
        markers=True,
        title="Daily Play Count"
    )
    st.plotly_chart(fig_plays, use_container_width=True)

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("🎥 Play Count by Media")
        media_play_df = (
            filtered_df
            .groupby("media_id", as_index=False)["play_count"]
            .sum()
            .sort_values("play_count", ascending=False)
        )

        fig_media = px.bar(
            media_play_df,
            x="media_id",
            y="play_count",
            title="Total Plays by Media"
        )
        st.plotly_chart(fig_media, use_container_width=True)

    with col_b:
        st.subheader("👥 Visitors by Media")
        media_visitor_df = (
            filtered_df
            .groupby("media_id", as_index=False)["visitors"]
            .sum()
            .sort_values("visitors", ascending=False)
        )

        fig_visitors = px.bar(
            media_visitor_df,
            x="media_id",
            y="visitors",
            title="Total Visitors by Media"
        )
        st.plotly_chart(fig_visitors, use_container_width=True)

    col_c, col_d = st.columns(2)

    with col_c:
        st.subheader("▶️ Play Rate Trend")
        fig_rate = px.line(
            trend_df,
            x="load_date",
            y="play_rate",
            markers=True,
            title="Average Play Rate Over Time"
        )
        st.plotly_chart(fig_rate, use_container_width=True)

    with col_d:
        st.subheader("⏱️ Hours Watched Trend")
        fig_hours = px.line(
            trend_df,
            x="load_date",
            y="hours_watched",
            markers=True,
            title="Hours Watched Over Time"
        )
        st.plotly_chart(fig_hours, use_container_width=True)

    st.divider()

    st.subheader("Gold Table Preview")
    st.dataframe(filtered_df, use_container_width=True)

except Exception as e:
    st.error("Failed to load data from Athena.")
    st.exception(e)
