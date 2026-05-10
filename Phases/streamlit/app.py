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


def get_connection():
    return connect(
        s3_staging_dir=S3_STAGING_DIR,
        region_name=AWS_REGION,
        schema_name=DATABASE
    )


@st.cache_data(ttl=300)
def load_main_data():
    conn = get_connection()

    query = """
        SELECT
            f.media_id,
            m.media_title,
            m.duration,
            m.status,
            m.media_type,
            f.play_count,
            f.play_rate,
            f.engagement,
            f.hours_watched,
            f.visitors,
            f.load_date,
            f.engagement_date,
            f.surrogate_key,
            f.load_timestamp
        FROM gold_fact_media_engagement f
        LEFT JOIN dim_media m
            ON f.media_id = m.media_id
            AND f.load_date = m.load_date
        ORDER BY f.load_date DESC, f.media_id
    """

    return pd.read_sql(query, conn)


@st.cache_data(ttl=300)
def load_engagement_curve():
    conn = get_connection()

    query = """
        SELECT
            c.media_id,
            m.media_title,
            c.load_date,
            c.second_index,
            c.engagement,
            c.engagement_value,
            c.rewatch_value
        FROM gold_fact_engagement_curve c
        LEFT JOIN dim_media m
            ON c.media_id = m.media_id
            AND c.load_date = m.load_date
        ORDER BY c.media_id, c.second_index
    """

    return pd.read_sql(query, conn)


@st.cache_data(ttl=300)
def load_daily_stats():
    conn = get_connection()

    query = """
        SELECT
            d.media_id,
            m.media_title,
            d.load_date,
            d.stat_date,
            d.load_count,
            d.play_count,
            d.hours_watched
        FROM gold_fact_media_daily_stats d
        LEFT JOIN dim_media m
            ON d.media_id = m.media_id
            AND d.load_date = m.load_date
        ORDER BY d.stat_date DESC, d.media_id
    """

    return pd.read_sql(query, conn)


@st.cache_data(ttl=300)
def load_inventory():
    conn = get_connection()

    query = """
        SELECT
            media_id,
            media_title,
            duration,
            status,
            media_type,
            archived,
            folder_name,
            section,
            created_at,
            updated_at,
            load_date
        FROM gold_dim_media_inventory
        ORDER BY updated_at DESC
    """

    return pd.read_sql(query, conn)


try:
    df = load_main_data()
    curve_df = load_engagement_curve()
    daily_df = load_daily_stats()
    inventory_df = load_inventory()

    df["load_date"] = pd.to_datetime(df["load_date"])
    curve_df["load_date"] = pd.to_datetime(curve_df["load_date"])
    daily_df["load_date"] = pd.to_datetime(daily_df["load_date"])
    daily_df["stat_date"] = pd.to_datetime(daily_df["stat_date"])
    inventory_df["load_date"] = pd.to_datetime(inventory_df["load_date"])

    st.success("Connected to Athena successfully!")

    # Sidebar filters
    st.sidebar.header("Filters")

    selected_media = st.sidebar.multiselect(
        "Select Media Title",
        options=sorted(df["media_title"].dropna().unique()),
        default=sorted(df["media_title"].dropna().unique())
    )

    selected_dates = st.sidebar.multiselect(
        "Select Load Date",
        options=sorted(df["load_date"].dt.date.unique()),
        default=sorted(df["load_date"].dt.date.unique())
    )

    filtered_df = df[
        (df["media_title"].isin(selected_media)) &
        (df["load_date"].dt.date.isin(selected_dates))
    ]

    filtered_curve_df = curve_df[
        (curve_df["media_title"].isin(selected_media)) &
        (curve_df["load_date"].dt.date.isin(selected_dates))
    ]

    filtered_daily_df = daily_df[
        (daily_df["media_title"].isin(selected_media)) &
        (daily_df["load_date"].dt.date.isin(selected_dates))
    ]

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

    # Media Metadata
    st.subheader("🎬 Selected Media Metadata")

    st.dataframe(
        filtered_df[
            [
                "media_id",
                "media_title",
                "duration",
                "status",
                "media_type"
            ]
        ].drop_duplicates(),
        use_container_width=True
    )

    st.divider()

    # Main trends
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
            .groupby("media_title", as_index=False)["play_count"]
            .sum()
            .sort_values("play_count", ascending=False)
        )

        fig_media = px.bar(
            media_play_df,
            x="media_title",
            y="play_count",
            title="Total Plays by Media",
            text_auto=True
        )

        st.plotly_chart(fig_media, use_container_width=True)

    with col_b:
        st.subheader("👥 Visitors by Media")

        media_visitor_df = (
            filtered_df
            .groupby("media_title", as_index=False)["visitors"]
            .sum()
            .sort_values("visitors", ascending=False)
        )

        fig_visitors = px.bar(
            media_visitor_df,
            x="media_title",
            y="visitors",
            title="Total Visitors by Media",
            text_auto=True
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

    # Engagement curve
    st.subheader("📉 Video Engagement Curve by Second")

    fig_curve = px.line(
        filtered_curve_df,
        x="second_index",
        y="engagement_value",
        color="media_title",
        title="Engagement Curve"
    )

    st.plotly_chart(fig_curve, use_container_width=True)

    st.subheader("🔁 Rewatch Curve by Second")

    fig_rewatch = px.line(
        filtered_curve_df,
        x="second_index",
        y="rewatch_value",
        color="media_title",
        title="Rewatch Curve"
    )

    st.plotly_chart(fig_rewatch, use_container_width=True)

    st.divider()

    # Daily stats
    st.subheader("📅 Media Daily Stats")

    st.dataframe(
        filtered_daily_df,
        use_container_width=True
    )

    st.divider()

    # Gold preview
    st.subheader("Gold Fact + Media Dimension Preview")

    st.dataframe(
        filtered_df,
        use_container_width=True
    )

    st.divider()

    # Inventory
    st.subheader("📦 Full Media Inventory")

    st.dataframe(
        inventory_df,
        use_container_width=True
    )

except Exception as e:
    st.error("Failed to load data from Athena.")
    st.exception(e)
