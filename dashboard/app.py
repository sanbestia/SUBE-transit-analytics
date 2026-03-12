"""
dashboard/app.py — SUBE Transit Analytics Dashboard

Run with:
    streamlit run dashboard/app.py

Features:
  - KPI cards (total ridership, peak day, top mode)
  - Interactive daily ridership time series with rolling averages
  - Mode split area chart
  - YoY % change bar chart
  - Weekday × month ridership heatmap
  - STL decomposition + anomaly detection
  - Recovery index line chart
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_PATH, EVENTS, TRANSPORT_MODES
from etl.load import get_connection, query

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SUBE Transit Analytics",
    page_icon="🚇",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Colour palette (one per mode) ──────────────────────────────────────────
MODE_COLORS = {
    "COLECTIVO": "#2563EB",   # blue
    "TREN":      "#16A34A",   # green
    "SUBTE":     "#DC2626",   # red
    "PREMETRO":  "#D97706",   # amber
}

# ── Helpers ────────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    if not DB_PATH.exists():
        st.error(
            "Database not found. Please run the ETL pipeline first:\n\n"
            "```bash\npython run_pipeline.py\n```"
        )
        st.stop()
    return get_connection()


@st.cache_data(ttl=3600)   # re-query every hour so live-refreshes pick up new data
def load_daily() -> pd.DataFrame:
    conn = get_conn()
    return conn.execute("""
        SELECT fecha, year, month, day_of_week, modo, cantidad_usos, is_suspicious
        FROM daily_transactions
        WHERE NOT is_suspicious
        ORDER BY fecha
    """).df()


@st.cache_data(ttl=3600)
def load_monthly() -> pd.DataFrame:
    conn = get_conn()
    return conn.execute("""
        SELECT * FROM monthly_transactions ORDER BY month_start, modo
    """).df()


@st.cache_data(ttl=3600)
def load_yoy() -> pd.DataFrame:
    conn = get_conn()
    return conn.execute("SELECT * FROM v_yoy_monthly ORDER BY month_start, modo").df()


@st.cache_data(ttl=3600)
def load_modal_split() -> pd.DataFrame:
    conn = get_conn()
    return conn.execute("SELECT * FROM v_modal_split ORDER BY month_start, modo").df()


@st.cache_data(ttl=3600)
def load_heatmap() -> pd.DataFrame:
    conn = get_conn()
    return conn.execute("SELECT * FROM v_weekday_heatmap").df()


def add_event_annotations(fig, events=EVENTS):
    """Add vertical lines for known events."""
    for ev in events:
        fig.add_vline(
            x=ev["date"], line_dash="dot",
            line_color=ev["color"], opacity=0.5,
            annotation_text=ev["label"],
            annotation_position="top right",
            annotation_font_size=10,
        )
    return fig


# ── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/1/1a/SUBE_Logo.svg/320px-SUBE_Logo.svg.png",
             width=160)
    st.title("SUBE Analytics")
    st.caption("Datos: datos.transporte.gob.ar")
    st.divider()

    daily = load_daily()
    min_date = daily["fecha"].min()
    max_date = daily["fecha"].max()

    date_range = st.date_input(
        "Período",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    modes_available = sorted(daily["modo"].unique())
    selected_modes  = st.multiselect(
        "Modo de transporte",
        options=modes_available,
        default=modes_available,
        format_func=lambda m: TRANSPORT_MODES.get(m, m),
    )

    show_events = st.toggle("Mostrar eventos históricos", value=True)
    show_decomp = st.toggle("Mostrar descomposición STL", value=False)

    st.divider()
    if st.button("🔄 Actualizar datos", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.caption(f"Datos hasta: **{max_date.strftime('%d/%m/%Y')}**")


# ── Filter data ────────────────────────────────────────────────────────────
if len(date_range) == 2:
    start_date, end_date = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
else:
    start_date, end_date = daily["fecha"].min(), daily["fecha"].max()

mask   = (
    (daily["fecha"] >= start_date) &
    (daily["fecha"] <= end_date) &
    (daily["modo"].isin(selected_modes))
)
df_filt = daily[mask]

monthly   = load_monthly()
m_mask    = (
    (monthly["month_start"] >= start_date) &
    (monthly["month_start"] <= end_date) &
    (monthly["modo"].isin(selected_modes))
)
df_monthly = monthly[m_mask]


# ── KPI Cards ──────────────────────────────────────────────────────────────
st.header("🚇 SUBE — Dashboard de Ridership")

total_daily = df_filt.groupby("fecha")["cantidad_usos"].sum()

col1, col2, col3, col4 = st.columns(4)
with col1:
    total = df_filt["cantidad_usos"].sum()
    st.metric("Total de viajes (período)", f"{total/1e9:.2f}B")
with col2:
    if not total_daily.empty:
        peak_day  = total_daily.idxmax()
        peak_val  = total_daily.max()
        st.metric("Día pico", peak_day.strftime("%d/%m/%Y"), f"{peak_val/1e6:.1f}M viajes")
with col3:
    if not df_filt.empty:
        top_mode = df_filt.groupby("modo")["cantidad_usos"].sum().idxmax()
        st.metric("Modo dominante", TRANSPORT_MODES.get(top_mode, top_mode))
with col4:
    if not total_daily.empty:
        avg_day = total_daily.mean()
        st.metric("Promedio diario", f"{avg_day/1e6:.2f}M viajes")

st.divider()


# ── Tab layout ─────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Serie temporal",
    "🥧 Modal split",
    "📊 Año vs año",
    "🗓️ Heatmap semanal",
    "🔬 Descomposición STL",
])


# ━━ Tab 1: Time series ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab1:
    st.subheader("Ridership diario por modo")

    # Daily totals with 7-day rolling average
    daily_total = (
        df_filt.groupby(["fecha", "modo"])["cantidad_usos"]
        .sum().reset_index()
    )

    fig = px.line(
        daily_total, x="fecha", y="cantidad_usos", color="modo",
        color_discrete_map=MODE_COLORS,
        labels={"cantidad_usos": "Viajes", "fecha": "Fecha", "modo": "Modo"},
        template="plotly_white",
    )
    fig.update_traces(opacity=0.5, line_width=1)

    # 7-day rolling average overlay per mode
    for mode in selected_modes:
        mode_df = daily_total[daily_total["modo"] == mode].sort_values("fecha")
        mode_df["ma7"] = mode_df["cantidad_usos"].rolling(7, min_periods=1).mean()
        fig.add_scatter(
            x=mode_df["fecha"], y=mode_df["ma7"],
            mode="lines", line=dict(color=MODE_COLORS.get(mode, "grey"), width=2.5),
            name=f"{TRANSPORT_MODES.get(mode, mode)} (MA 7d)",
        )

    if show_events:
        fig = add_event_annotations(fig)

    fig.update_layout(height=500, legend_title="Modo", hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)


# ━━ Tab 2: Modal split ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab2:
    st.subheader("Participación por modo (modal split mensual)")

    split = load_modal_split()
    split = split[
        (split["month_start"] >= start_date) &
        (split["month_start"] <= end_date) &
        (split["modo"].isin(selected_modes))
    ]

    fig2 = px.area(
        split, x="month_start", y="mode_share_pct", color="modo",
        color_discrete_map=MODE_COLORS,
        labels={"mode_share_pct": "Participación (%)", "month_start": "Mes", "modo": "Modo"},
        template="plotly_white",
    )
    if show_events:
        fig2 = add_event_annotations(fig2)
    fig2.update_layout(height=450, yaxis_ticksuffix="%", hovermode="x unified")
    st.plotly_chart(fig2, use_container_width=True)


# ━━ Tab 3: YoY ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab3:
    st.subheader("Variación año a año (%) por modo")

    yoy = load_yoy()
    yoy = yoy[
        (yoy["month_start"] >= start_date) &
        (yoy["month_start"] <= end_date) &
        (yoy["modo"].isin(selected_modes))
    ].dropna(subset=["yoy_pct_change"])

    fig3 = px.bar(
        yoy, x="month_start", y="yoy_pct_change", color="modo",
        barmode="group",
        color_discrete_map=MODE_COLORS,
        labels={"yoy_pct_change": "Δ% vs año anterior", "month_start": "Mes", "modo": "Modo"},
        template="plotly_white",
    )
    fig3.add_hline(y=0, line_color="black", line_width=1)
    fig3.update_layout(height=450, yaxis_ticksuffix="%", hovermode="x unified")
    st.plotly_chart(fig3, use_container_width=True)


# ━━ Tab 4: Heatmap ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab4:
    st.subheader("Ridership promedio: día de semana × mes del año")
    st.caption("Promedio de viajes diarios agrupados por día de semana y mes calendario.")

    heatmap_df = load_heatmap()
    pivot = heatmap_df.pivot(index="day_of_week", columns="month", values="avg_usos")
    pivot.index = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
    pivot.columns = ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"]

    fig4 = px.imshow(
        pivot,
        color_continuous_scale="Blues",
        labels={"color": "Avg viajes/día"},
        template="plotly_white",
        aspect="auto",
    )
    fig4.update_layout(height=350)
    st.plotly_chart(fig4, use_container_width=True)


# ━━ Tab 5: STL decomposition ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab5:
    st.subheader("Descomposición STL de la serie temporal")
    st.caption(
        "STL (Seasonal-Trend decomposition using LOESS) separa la serie en tendencia, "
        "estacionalidad y residuos. Los residuos > 3σ se marcan como anomalías."
    )

    col_mode, col_period = st.columns(2)
    with col_mode:
        stl_mode = st.selectbox(
            "Modo", ["Todos"] + modes_available,
            format_func=lambda m: "Todos los modos" if m == "Todos" else TRANSPORT_MODES.get(m, m)
        )
    with col_period:
        stl_period = st.radio("Estacionalidad", [7, 365], format_func=lambda p: f"{'Semanal' if p==7 else 'Anual'} ({p}d)")

    if st.button("Calcular descomposición"):
        with st.spinner("Corriendo STL …"):
            try:
                from analytics.time_series import decompose_series, detect_anomalies
                conn = get_conn()
                result = decompose_series(
                    conn,
                    mode=None if stl_mode == "Todos" else stl_mode,
                    period=stl_period
                )
                if result:
                    anomalies = detect_anomalies(result["residual"])

                    fig5 = go.Figure()
                    components = [
                        ("original",  "Observado",    "rgba(100,100,200,0.3)", True),
                        ("trend",     "Tendencia",    "#2563EB",               False),
                        ("seasonal",  "Estacionalidad","#16A34A",              False),
                        ("residual",  "Residuo",      "#94a3b8",               False),
                    ]
                    for key, name, color, fill in components:
                        s = result[key]
                        fig5.add_scatter(
                            x=s.index, y=s.values, name=name,
                            line_color=color,
                            fill="tozeroy" if fill else None,
                            opacity=0.7 if fill else 1.0,
                        )

                    # Anomaly markers on residual
                    anom = anomalies[anomalies["is_anomaly"]]
                    fig5.add_scatter(
                        x=anom["fecha"], y=anom["residual"],
                        mode="markers", name="Anomalía",
                        marker=dict(color="red", size=7, symbol="x"),
                        hovertemplate="<b>%{x}</b><br>Residuo: %{y:,.0f}<br>%{text}",
                        text=anom["event_label"],
                    )

                    fig5.update_layout(height=550, template="plotly_white",
                                       hovermode="x unified", legend_title="Componente")
                    st.plotly_chart(fig5, use_container_width=True)

                    # Show anomaly table
                    if not anom.empty:
                        st.subheader(f"🚨 {len(anom)} anomalías detectadas")
                        st.dataframe(
                            anom[["fecha", "z_score", "event_label"]]
                            .sort_values("z_score", key=abs, ascending=False)
                            .head(20)
                            .rename(columns={
                                "fecha": "Fecha",
                                "z_score": "Z-score",
                                "event_label": "Evento conocido"
                            }),
                            use_container_width=True,
                        )
            except ImportError:
                st.error("statsmodels no instalado. Ejecuta: `pip install statsmodels`")

st.divider()
st.caption(
    "Fuente: [datos.transporte.gob.ar](https://datos.transporte.gob.ar) · "
    "Licencia Creative Commons 4.0 · "
    "Actualización automática diaria"
)
