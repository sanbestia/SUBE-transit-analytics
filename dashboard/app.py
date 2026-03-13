"""
dashboard/app.py — SUBE Transit Analytics Dashboard

Run with:
    streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DASHBOARD_MODES, DB_PATH, EVENTS, FARE_HIKES, MODE_COLORS, TRANSPORT_MODES
from etl.load import get_connection

# ── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SUBE Transit Analytics",
    page_icon="🚇",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Translations ───────────────────────────────────────────────────────────
STRINGS = {
    "es": {
        "page_title":       "SUBE — Análisis de Transporte Público",
        "sidebar_title":    "SUBE Analytics",
        "sidebar_source":   "Datos: datos.transporte.gob.ar",
        "periodo":          "Período",
        "modos":            "Modos de transporte",
        "show_events":      "Mostrar eventos históricos",
        "refresh":          "🔄 Actualizar datos",
        "data_until":       "Datos hasta",
        "kpi_total":        "Total de viajes",
        "kpi_peak":         "Día pico",
        "kpi_avg":          "Promedio diario",
        "kpi_top_mode":     "Modo dominante",
        "kpi_trips":        "viajes",
        "tab_overview":     "📊 Resumen",
        "tab_covid":        "🦠 COVID-19",
        "tab_modal":        "🔄 Sustitución Modal",
        "tab_resilience":   "💪 Resiliencia",
        "tab_analysis":     "🔬 Análisis",

        # ── Overview ──────────────────────────────────────────────────────
        "ov_series_title":    "Ridership diario por modo",
        "ov_series_explainer": "Cantidad de viajes registrados por día para cada modo de transporte. "
                               "La línea fina muestra el valor diario real; la línea gruesa es el **promedio móvil de 7 días**, "
                               "que suaviza las variaciones normales entre días de semana y fin de semana para revelar la tendencia subyacente. "
                               "Las líneas verticales punteadas marcan eventos históricos clave.",
        "ov_series_y":        "Viajes",
        "ov_split_title":     "Participación por modo (modal split mensual)",
        "ov_split_explainer": "El **modal split** muestra qué porcentaje del total de viajes corresponde a cada modo en cada mes. "
                               "Un valor constante indica que los modos crecen o caen al mismo ritmo. "
                               "Cambios en la participación revelan sustitución modal — por ejemplo, cuando el SUBTE cerró en 2020, "
                               "su porcentaje cayó casi a cero y el COLECTIVO absorbió la mayor parte de los viajes restantes.",
        "ov_split_y":         "Participación (%)",
        "ov_empresas_title":  "Top 10 empresas por ridership total (2020–presente)",
        "ov_empresas_explainer": "Las diez operadoras con mayor cantidad de viajes acumulados desde 2020. "
                                  "Cada empresa puede operar uno o varios modos. EMOVA (SUBTE) domina en términos absolutos "
                                  "porque concentra toda la red de subterráneos de Buenos Aires en una sola concesión.",
        "ov_empresas_y":      "Total de viajes",
        "ov_empresas_x":      "Empresa",

        # ── COVID ─────────────────────────────────────────────────────────
        "cv_collapse_title":    "Colapso COVID-19: ridership mensual por modo",
        "cv_collapse_explainer": "En marzo de 2020 el gobierno nacional decretó el ASPO (Aislamiento Social Preventivo y Obligatorio), "
                                  "el primer lockdown estricto de Argentina. El impacto fue inmediato pero **asimétrico entre modos**: "
                                  "el SUBTE colapsó un **92%** en abril 2020 porque depende casi exclusivamente de trabajadores de oficina en CABA, "
                                  "el TREN cayó un **87%**, pero el COLECTIVO sólo un **58%** — los colectivos siguieron transportando "
                                  "trabajadores esenciales (salud, logística, seguridad) que no podían trabajar desde casa.",
        "cv_subst_title":       "Sustitución modal: variación mensual % (SUBTE vs COLECTIVO)",
        "cv_subst_explainer":   "Este gráfico muestra la **variación mes a mes** de cada modo, no el volumen absoluto. "
                                 "Permite ver si un modo crece más rápido o más lento que otro en el mismo período. "
                                 "Durante 2021, el SUBTE creció consistentemente más rápido que el COLECTIVO — "
                                 "esto sugiere que cuando las restricciones se levantaron, los usuarios volvieron al subte "
                                 "con más entusiasmo que al colectivo, posiblemente por velocidad y confiabilidad.",
        "cv_yoy_title":         "Variación año a año (%) por modo",
        "cv_yoy_explainer":     "Compara cada mes con el mismo mes del año anterior, eliminando la estacionalidad natural "
                                 "(por ejemplo, enero siempre tiene menos viajes que marzo). "
                                 "Barras positivas = crecimiento respecto al año anterior. Barras negativas = caída. "
                                 "El año 2021 muestra barras enormemente positivas porque se compara contra el piso del lockdown de 2020.",
        "cv_yoy_y":             "Δ% vs año anterior",

        # ── Modal substitution ────────────────────────────────────────────
        "ms_mom_title":       "Variación mensual % por modo — serie completa",
        "ms_mom_explainer":   "Variación mes a mes de cada modo a lo largo de toda la serie. "
                               "Permite identificar cómo cada evento histórico afectó de manera diferente a cada modo: "
                               "el SUBTE reacciona más violentamente a shocks discretos (lockdowns, paros) porque tiene menor "
                               "sustitución posible; el COLECTIVO es más resiliente porque sirve zonas sin cobertura de subte o tren. "
                               "Los eventos históricos y tarifarios están anotados para facilitar la lectura.",
        "ms_share_title":     "Participación relativa por modo — evolución mensual",
        "ms_share_explainer": "El **modal split** muestra qué porcentaje del total corresponde a cada modo en cada mes. "
                               "Un cambio sostenido en la participación indica una sustitución estructural, no un shock puntual. "
                               "Por ejemplo, si el SUBTE recupera participación post-COVID más rápido que el COLECTIVO, "
                               "sugiere que los pasajeros que volvieron primero son los que no tienen alternativa al subte.",
        "ms_yoy_title":       "Variación año a año % — todos los modos",
        "ms_yoy_explainer":   "Compara cada mes con el mismo mes del año anterior para eliminar estacionalidad. "
                               "Muestra si los modos crecen o caen al mismo ritmo ante cada evento, "
                               "o si uno diverge del resto.",
        "rs_amba_title":       "Ridership mensual: AMBA vs Interior",
        "rs_amba_explainer":   "**AMBA** (Área Metropolitana de Buenos Aires) incluye CABA y el Gran Buenos Aires. "
                                "**Interior** son todas las demás provincias. "
                                "El gráfico muestra viajes mensuales totales en valores absolutos. "
                                "El AMBA concentra la mayor parte del ridership del país por densidad poblacional. "
                                "La caída del AMBA post-2024 refleja el impacto de la pérdida de subsidios nacionales; "
                                "el Interior muestra mayor estabilidad porque cada provincia gestionó sus tarifas de forma independiente.",
        "rs_milei_title":      "Efecto tarifario 2023–presente: AMBA vs Interior",
        "rs_milei_explainer":  "Zoom sobre el período desde enero 2023. "
                                "Las líneas verticales muestran cada aumento tarifario documentado. "
                                "El AMBA recibió los aumentos más abruptos (enero y febrero 2024); "
                                "el Interior absorbió el shock del Fondo de Compensación de forma más gradual y heterogénea.",
        "rs_seasonal_title":   "Amplitud estacional: AMBA vs Interior",
        "rs_seasonal_explainer": "La **amplitud estacional** mide cuánto varía el ridership a lo largo del año en relación a su promedio anual. "
                                  "Se calcula como el cociente entre el mes pico y el mes valle de cada año. "
                                  "Un valor de 1.5 significa que el mes más activo tiene un 50% más viajes que el mes más tranquilo. "
                                  "El AMBA tiene **menor amplitud estacional** que el Interior — "
                                  "esto se explica porque el AMBA tiene mayor proporción de viajes laborales formales, "
                                  "que son más uniformes a lo largo del año, mientras que el Interior tiene mayor peso relativo "
                                  "de viajes estacionales (turismo, economías regionales, ferias). "
                                  "Una hipótesis alternativa: el AMBA tiene mayor elasticidad al precio y las subas tarifarias "
                                  "afectan de forma despareja los meses de menor demanda (enero, julio), "
                                  "comprimiendo la variación estacional.",
        "rs_prov_explainer":   "Volumen total de viajes por provincia desde 2020. "
                                "Buenos Aires (provincia) lidera por tamaño poblacional y extensión del GBA. "
                                "Mendoza, Santa Fe y San Juan muestran redes urbanas consolidadas. "
                                "Nota: se excluyen los registros con jurisdicción nacional (trenes y subtes de CABA "
                                "aparecen bajo sus empresas operadoras, no bajo una provincia específica).",
        "rs_prov_caption":     "Excluye JN (Jurisdicción Nacional) y valores nulos.",

        # ── Analysis ──────────────────────────────────────────────────────
        "an_heatmap_title":    "Ridership promedio: día de semana × mes",
        "an_heatmap_explainer": "Cada celda muestra el **promedio de viajes diarios** para esa combinación de día de la semana y mes del año, "
                                 "calculado sobre todos los años disponibles (2020–presente). "
                                 "Los colores más oscuros indican más viajes. "
                                 "El patrón típico: los días hábiles (lunes a viernes) tienen más viajes que el fin de semana, "
                                 "y los meses de mayor actividad son marzo-abril y agosto-septiembre (picos de actividad laboral en Argentina). "
                                 "Enero y julio muestran caídas por vacaciones de verano e invierno respectivamente.",
        "an_heatmap_color":    "Avg viajes/día",
        "an_stl_title":        "Descomposición STL de la serie temporal",
        "an_stl_explainer":    """**¿Qué es STL?** STL *(Seasonal-Trend decomposition using LOESS)* es un método estadístico que descompone \
una serie temporal en tres componentes independientes:

- 🔵 **Tendencia**: la dirección de largo plazo, sin ruido ni estacionalidad. Refleja cambios estructurales \
como la pandemia, la recuperación económica o el impacto de las subas tarifarias.
- 🟢 **Estacionalidad**: el patrón que se repite regularmente en cada ciclo (semanal o anual). \
Por ejemplo, menos viajes los domingos o en enero por las vacaciones de verano.
- ⚫ **Residuo**: lo que queda después de quitar tendencia y estacionalidad. En condiciones normales es ruido aleatorio. \
Un residuo inusualmente alto o bajo indica un día que no sigue el patrón esperado.

**¿Qué es una anomalía?** Un día donde el residuo supera **3 desvíos estándar** de su media histórica. \
Esto señala eventos inesperados como huelgas de transporte, feriados no contemplados o cortes de servicio.

⚠️ **Importante**: el colapso de COVID-19 *no aparece como anomalía* porque fue tan prolongado y sostenido \
que el modelo lo absorbió en la tendencia. STL detecta sorpresas locales, no cambios estructurales graduales.""",
        "an_stl_mode":         "Modo",
        "an_stl_all":          "Todos los modos",
        "an_stl_season":       "Período de estacionalidad",
        "an_stl_weekly":       "Semanal (7 días)",
        "an_stl_annual":       "Anual (365 días)",
        "an_stl_run":          "Calcular descomposición",
        "an_stl_running":      "Corriendo STL… (puede tardar unos segundos)",
        "an_stl_observed":     "Observado",
        "an_stl_trend":        "Tendencia",
        "an_stl_seasonal":     "Estacionalidad",
        "an_stl_residual":     "Residuo",
        "an_stl_anomaly":      "Anomalía",
        "an_stl_component":    "Componente",
        "an_anom_title":       "anomalías detectadas",
        "an_anom_date":        "Fecha",
        "an_anom_z":           "Z-score",
        "an_anom_event":       "Evento conocido",
        "an_anom_explainer":   "Las anomalías sin evento conocido son las más interesantes — "
                                "corresponden a días donde algo inusual ocurrió pero no está registrado en nuestra lista de eventos. "
                                "Un z-score de 3 significa que el residuo está 3 desvíos estándar por encima o debajo de lo esperado.",

        "kpi_explainer": "Métricas calculadas sobre el período y los modos seleccionados en el panel lateral. "
                          "El **día pico** es el día individual con más viajes registrados. "
                          "El **promedio diario** incluye fines de semana y feriados, por eso es menor que un típico día hábil.",

        # ── Forecast ──────────────────────────────────────────────────────
        "tab_forecast":       "🔮 Predicción",
        "fc_run":             "Generar predicción",
        "fc_running":         "Entrenando modelos… (puede tardar 20–40 segundos)",
        "fc_horizon":         "Meses a predecir",
        "fc_title":           "Predicción de ridership — próximos {n} meses",
        "fc_actual":          "Histórico (real)",
        "fc_fitted":          "Histórico (modelo)",
        "fc_forecast":        "Predicción",
        "fc_band":            "Intervalo de confianza 80%",
        "fc_summary_title":   "Resumen de la predicción",
        "fc_mode":            "Modo",
        "fc_last":            "Último valor real",
        "fc_mean":            "Promedio predicho",
        "fc_change":          "Cambio estimado (vs últimos 6 meses)",
        "fc_direction_up":    "↑ Suba",
        "fc_direction_down":  "↓ Baja",
        "fc_direction_flat":  "→ Estable",
        "fc_explainer": """**¿Cómo funciona esta predicción?**

Se usa **Prophet**, un modelo de series temporales desarrollado por Meta, entrenado con los datos mensuales \
de ridership desde 2020. El modelo aprende tres cosas por separado:

- 📈 **Tendencia**: la dirección general de largo plazo (crecimiento o caída).
- 📅 **Estacionalidad anual**: los patrones que se repiten cada año \
(enero siempre tiene menos viajes; marzo y agosto son picos).
- 🎌 **Feriados argentinos**: los feriados nacionales se modelan explícitamente \
como caídas puntuales de demanda.

Además se incorporan dos **regresores externos**:

- 💸 **Presión tarifaria acumulada**: suma de todos los aumentos tarifarios que entraron en vigencia \
hasta cada mes (desde el congelamiento de 2022 hasta los aumentos escalonados de 2025–2026). \
No es un simple interruptor on/off — refleja la magnitud acumulada del ajuste tarifario.
- 📉 **Shock macroeconómico**: variable binaria que captura el cambio de régimen desde la \
devaluación de diciembre 2023 (+118%) y el recorte de subsidios, independientemente de las tarifas.

Los **puntos de cambio estructural** (changepoints) se fijan en las fechas exactas de cada aumento \
tarifario y evento macro, en lugar de dejarse descubrir automáticamente — esto mejora la precisión \
del modelo en períodos de alta volatilidad.

**¿Qué significa el intervalo de confianza?** La banda sombreada representa el rango \
donde el modelo espera que caigan el 80% de los valores futuros. Un intervalo más ancho \
indica mayor incertidumbre.

⚠️ **Limitación importante**: el modelo no puede anticipar eventos futuros desconocidos \
(nuevas subas de tarifas, paros, cambios de política). La predicción asume que el nivel \
actual de presión tarifaria se mantiene.""",

        "days":   ["Lun","Mar","Mié","Jue","Vie","Sáb","Dom"],
        "months": ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic"],
        "amba_labels": {"SI": "AMBA", "NO": "Interior"},
    },
    "en": {
        "page_title":       "SUBE — Public Transport Analytics",
        "sidebar_title":    "SUBE Analytics",
        "sidebar_source":   "Data: datos.transporte.gob.ar",
        "periodo":          "Period",
        "modos":            "Transport modes",
        "show_events":      "Show historical events",
        "refresh":          "🔄 Refresh data",
        "data_until":       "Data through",
        "kpi_total":        "Total trips",
        "kpi_peak":         "Peak day",
        "kpi_avg":          "Daily average",
        "kpi_top_mode":     "Dominant mode",
        "kpi_trips":        "trips",
        "tab_overview":     "📊 Overview",
        "tab_covid":        "🦠 COVID-19",
        "tab_modal":        "🔄 Modal Substitution",
        "tab_resilience":   "💪 Resilience",
        "tab_analysis":     "🔬 Analysis",

        # ── Overview ──────────────────────────────────────────────────────
        "ov_series_title":    "Daily ridership by mode",
        "ov_series_explainer": "Number of trips recorded per day for each transport mode. "
                               "The thin line shows the raw daily value; the thick line is the **7-day moving average**, "
                               "which smooths out normal weekday/weekend variation to reveal the underlying trend. "
                               "Dotted vertical lines mark key historical events.",
        "ov_series_y":        "Trips",
        "ov_split_title":     "Modal split (monthly)",
        "ov_split_explainer": "**Modal split** shows what percentage of total trips each mode accounts for each month. "
                               "A stable value means modes grow or fall at the same rate. "
                               "Shifts reveal modal substitution — for example, when the SUBTE closed in 2020, "
                               "its share dropped to near zero and COLECTIVO absorbed most of the remaining trips.",
        "ov_split_y":         "Share (%)",
        "ov_empresas_title":  "Top 10 operators by total ridership (2020–present)",
        "ov_empresas_explainer": "The ten operators with the highest cumulative trip count since 2020. "
                                  "Each company may operate one or more modes. EMOVA (SUBTE) dominates in absolute terms "
                                  "because it holds the concession for the entire Buenos Aires subway network.",
        "ov_empresas_y":      "Total trips",
        "ov_empresas_x":      "Operator",

        # ── COVID ─────────────────────────────────────────────────────────
        "cv_collapse_title":    "COVID-19 collapse: monthly ridership by mode",
        "cv_collapse_explainer": "In March 2020, the national government declared the ASPO (mandatory social isolation), "
                                  "Argentina's first strict lockdown. The impact was immediate but **asymmetric across modes**: "
                                  "SUBTE collapsed **92%** in April 2020 because it almost exclusively serves office workers in CABA, "
                                  "TREN fell **87%**, but COLECTIVO only **58%** — buses kept running for essential workers "
                                  "(healthcare, logistics, security) who couldn't work from home.",
        "cv_subst_title":       "Modal substitution: month-over-month % change (SUBTE vs COLECTIVO)",
        "cv_subst_explainer":   "This chart shows the **month-over-month change** for each mode, not absolute volume. "
                                 "It reveals whether one mode is growing faster or slower than another in the same period. "
                                 "Throughout 2021, SUBTE consistently grew faster than COLECTIVO — "
                                 "suggesting that once restrictions lifted, users returned to the subway more eagerly, "
                                 "possibly due to speed and reliability advantages.",
        "cv_yoy_title":         "Year-over-year % change by mode",
        "cv_yoy_explainer":     "Compares each month to the same month in the prior year, removing natural seasonality "
                                 "(e.g. January always has fewer trips than March). "
                                 "Positive bars = growth vs prior year. Negative bars = decline. "
                                 "2021 shows enormous positive bars because it compares against the 2020 lockdown floor.",
        "cv_yoy_y":             "Δ% vs previous year",

        # ── Modal substitution ────────────────────────────────────────────
        "ms_mom_title":       "Month-over-month % change by mode — full series",
        "ms_mom_explainer":   "Month-over-month change for each mode across the entire series. "
                               "Reveals how each historical event affected each mode differently: "
                               "SUBTE reacts more violently to discrete shocks (lockdowns, strikes) because it has fewer substitutes; "
                               "COLECTIVO is more resilient because it serves areas without subway or rail coverage. "
                               "Historical and fare events are annotated for easy reading.",
        "ms_share_title":     "Modal share — monthly evolution",
        "ms_share_explainer": "**Modal split** shows what percentage of total trips each mode accounts for each month. "
                               "A sustained shift indicates structural substitution, not a one-off shock. "
                               "For example, if SUBTE recovers share faster than COLECTIVO post-COVID, "
                               "it suggests the passengers who returned first had no alternative to the subway.",
        "ms_yoy_title":       "Year-over-year % — all modes",
        "ms_yoy_explainer":   "Compares each month to the same month in the prior year to remove seasonality. "
                               "Shows whether modes grow or fall at the same rate around each event, "
                               "or whether one diverges from the rest.",

        # ── Resilience ────────────────────────────────────────────────────
        "rs_amba_title":       "Monthly ridership: AMBA vs Interior",
        "rs_amba_explainer":   "**AMBA** (Buenos Aires Metropolitan Area) includes CABA and Greater Buenos Aires. "
                                "**Interior** refers to all other provinces. "
                                "The chart shows total monthly trips in absolute values. "
                                "AMBA concentrates most of the country's ridership due to population density. "
                                "The post-2024 AMBA decline reflects the loss of national subsidies; "
                                "Interior shows greater stability because each province managed its own fare schedule.",
        "rs_milei_title":      "Fare hike effect 2023–present: AMBA vs Interior",
        "rs_milei_explainer":  "Zoom on the period from January 2023. "
                                "Vertical lines show each documented fare hike. "
                                "AMBA received the most abrupt increases (January and February 2024); "
                                "Interior absorbed the Compensation Fund shock more gradually and unevenly.",
        "rs_seasonal_title":   "Seasonal amplitude: AMBA vs Interior",
        "rs_seasonal_explainer": "**Seasonal amplitude** measures how much ridership varies across the year relative to its annual average. "
                                  "Computed as the ratio of the peak month to the trough month for each year. "
                                  "A value of 1.5 means the busiest month has 50% more trips than the quietest. "
                                  "The AMBA has **lower seasonal amplitude** than the Interior — "
                                  "likely because AMBA trips are dominated by formal commuting, which is more uniform throughout the year, "
                                  "while Interior ridership has a higher relative weight of seasonal trips "
                                  "(tourism, regional economies, fairs). "
                                  "An alternative hypothesis: AMBA demand is more price-elastic, and fare hikes "
                                  "disproportionately suppress low-demand months (January, July), "
                                  "compressing the seasonal range.",
        "rs_prov_explainer":   "Total trip volume by province since 2020. "
                                "Buenos Aires province leads due to population size and the extent of Greater Buenos Aires. "
                                "Mendoza, Santa Fe, and San Juan show consolidated urban networks. "
                                "Note: records with national jurisdiction (CABA trains and subway) are excluded here — "
                                "they appear under their operating companies, not a specific province.",
        "rs_prov_caption":     "Excludes JN (National Jurisdiction) and null values.",

        # ── Analysis ──────────────────────────────────────────────────────
        "an_heatmap_title":    "Average ridership: weekday × month",
        "an_heatmap_explainer": "Each cell shows the **average daily trips** for that combination of weekday and calendar month, "
                                 "calculated across all available years (2020–present). "
                                 "Darker colors indicate more trips. "
                                 "The typical pattern: weekdays (Mon–Fri) have more trips than weekends, "
                                 "and peak months are March–April and August–September (Argentina's peak work periods). "
                                 "January and July show dips due to summer and winter school holidays respectively.",
        "an_heatmap_color":    "Avg trips/day",
        "an_stl_title":        "STL time series decomposition",
        "an_stl_explainer":    """**What is STL?** STL *(Seasonal-Trend decomposition using LOESS)* is a statistical method that \
separates a time series into three independent components:

- 🔵 **Trend**: the long-term direction, free of noise and seasonality. Captures structural changes \
like the pandemic, economic recovery, or the impact of fare hikes.
- 🟢 **Seasonality**: the regularly repeating pattern within each cycle (weekly or annual). \
For example, fewer trips on Sundays or in January during summer holidays.
- ⚫ **Residual**: what remains after removing trend and seasonality. Under normal conditions it looks like random noise. \
An unusually high or low residual flags a day that doesn't follow the expected pattern.

**What is an anomaly?** A day where the residual exceeds **3 standard deviations** from its historical mean. \
This highlights unexpected events such as transport strikes, unplanned holidays, or service disruptions.

⚠️ **Important**: the COVID-19 collapse *does not appear as an anomaly* because it was so prolonged and sustained \
that the model absorbed it into the trend. STL detects local surprises, not gradual structural shifts.""",
        "an_stl_mode":         "Mode",
        "an_stl_all":          "All modes",
        "an_stl_season":       "Seasonality period",
        "an_stl_weekly":       "Weekly (7 days)",
        "an_stl_annual":       "Annual (365 days)",
        "an_stl_run":          "Run decomposition",
        "an_stl_running":      "Running STL… (this may take a few seconds)",
        "an_stl_observed":     "Observed",
        "an_stl_trend":        "Trend",
        "an_stl_seasonal":     "Seasonality",
        "an_stl_residual":     "Residual",
        "an_stl_anomaly":      "Anomaly",
        "an_stl_component":    "Component",
        "an_anom_title":       "anomalies detected",
        "an_anom_date":        "Date",
        "an_anom_z":           "Z-score",
        "an_anom_event":       "Known event",
        "an_anom_explainer":   "Anomalies without a known event label are the most interesting — "
                                "they correspond to days where something unusual happened that isn't in our events list. "
                                "A z-score of 3 means the residual is 3 standard deviations above or below what was expected.",

        "kpi_explainer": "Metrics calculated over the period and modes selected in the sidebar. "
                          "**Peak day** is the single day with the highest recorded trips. "
                          "**Daily average** includes weekends and holidays, so it is lower than a typical workday.",

        # ── Forecast ──────────────────────────────────────────────────────
        "tab_forecast":       "🔮 Forecast",
        "fc_run":             "Generate forecast",
        "fc_running":         "Training models… (may take 20–40 seconds)",
        "fc_horizon":         "Months to forecast",
        "fc_title":           "Ridership forecast — next {n} months",
        "fc_actual":          "Actual (observed)",
        "fc_fitted":          "Actual (model fit)",
        "fc_forecast":        "Forecast",
        "fc_band":            "80% confidence interval",
        "fc_summary_title":   "Forecast summary",
        "fc_mode":            "Mode",
        "fc_last":            "Last actual value",
        "fc_mean":            "Mean forecast",
        "fc_change":          "Est. change (vs last 6 months)",
        "fc_direction_up":    "↑ Rising",
        "fc_direction_down":  "↓ Falling",
        "fc_direction_flat":  "→ Stable",
        "fc_explainer": """**How does this forecast work?**

This uses **Prophet**, a time series model developed by Meta, trained on monthly ridership data since 2020. \
The model learns three things separately:

- 📈 **Trend**: the general long-term direction (growth or decline).
- 📅 **Annual seasonality**: patterns that repeat each year \
(January always has fewer trips; March and August are peaks).
- 🎌 **Argentine public holidays**: national holidays are explicitly modelled \
as point-in-time demand drops.

Two **external regressors** are also included:

- 💸 **Cumulative fare pressure**: the sum of all fare increases in effect up to each month \
(from the end of the 3-year freeze in 2022 through the staged hikes of 2025–2026). \
Not a simple on/off switch — it reflects the accumulated magnitude of fare adjustments.
- 📉 **Macro shock**: a binary variable capturing the regime change triggered by the \
December 2023 devaluation (+118%) and subsidy cuts, independent of fare levels.

**Structural changepoints** are fixed at the exact dates of each fare hike and macro event, \
rather than being discovered automatically — this improves model accuracy during high-volatility periods.

**What does the confidence interval mean?** The shaded band shows the range where \
the model expects 80% of future values to fall. A wider band means higher uncertainty.

⚠️ **Important limitation**: the model cannot anticipate unknown future events \
(new fare increases, strikes, policy changes). The forecast assumes the current level \
of fare pressure remains unchanged.""",

        "days":   ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"],
        "months": ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],
        "amba_labels": {"SI": "AMBA", "NO": "Interior"},
    },
}

MODE_LABELS = {
    "es": {"COLECTIVO": "Colectivo (Bus)", "TREN": "Tren", "SUBTE": "Subte"},
    "en": {"COLECTIVO": "Bus", "TREN": "Train", "SUBTE": "Subway"},
}

# ── Session state ──────────────────────────────────────────────────────────
if "lang" not in st.session_state:
    st.session_state.lang = "es"


def t(key: str) -> str:
    return STRINGS[st.session_state.lang].get(key, key)


def mode_label(mode: str) -> str:
    return MODE_LABELS[st.session_state.lang].get(mode, mode)


def event_label(ev: dict) -> str:
    return ev[f"label_{st.session_state.lang}"]


# ── DB helpers ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_conn():
    if not DB_PATH.exists():
        st.error("Database not found. Run: python run_pipeline.py")
        st.stop()
    return get_connection()


@st.cache_data(ttl=3600)
def load_monthly() -> pd.DataFrame:
    return get_conn().execute("""
        SELECT * FROM monthly_transactions
        WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        ORDER BY month_start, modo
    """).df()


@st.cache_data(ttl=3600)
def load_daily_totals() -> pd.DataFrame:
    return get_conn().execute("""
        SELECT fecha, year, month, day_of_week, modo,
               SUM(cantidad_usos) AS cantidad_usos
        FROM daily_transactions
        WHERE NOT is_suspicious
          AND modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        GROUP BY fecha, year, month, day_of_week, modo
        ORDER BY fecha
    """).df()


@st.cache_data(ttl=3600)
def load_modal_split() -> pd.DataFrame:
    return get_conn().execute("""
        SELECT * FROM v_modal_split
        WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        ORDER BY month_start, modo
    """).df()


@st.cache_data(ttl=3600)
def load_yoy() -> pd.DataFrame:
    return get_conn().execute("""
        SELECT * FROM v_yoy_monthly
        WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        ORDER BY month_start, modo
    """).df()


@st.cache_data(ttl=3600)
def load_heatmap() -> pd.DataFrame:
    return get_conn().execute("SELECT * FROM v_weekday_heatmap").df()


@st.cache_data(ttl=3600)
def load_amba_recovery() -> pd.DataFrame:
    return get_conn().execute("""
        WITH base AS (
            SELECT amba, SUM(total_usos) AS jan2020
            FROM monthly_by_provincia
            WHERE month_start = '2020-01-01'
            GROUP BY amba
        )
        SELECT p.month_start, p.amba,
               SUM(p.total_usos) AS total,
               ROUND(100.0 * SUM(p.total_usos) / MAX(b.jan2020), 1) AS recovery_index
        FROM monthly_by_provincia p
        JOIN base b ON p.amba = b.amba
        WHERE p.amba IN ('SI', 'NO')
        GROUP BY p.month_start, p.amba
        ORDER BY p.month_start, p.amba
    """).df()


@st.cache_data(ttl=3600)
def load_top_empresas() -> pd.DataFrame:
    return get_conn().execute("""
        SELECT nombre_empresa, modo, total_usos
        FROM top_empresas
        WHERE modo IN ('COLECTIVO', 'TREN', 'SUBTE')
        ORDER BY total_usos DESC
        LIMIT 10
    """).df()


@st.cache_data(ttl=3600)
def load_by_provincia() -> pd.DataFrame:
    return get_conn().execute("""
        SELECT provincia, SUM(total_usos) AS total
        FROM monthly_by_provincia
        WHERE provincia NOT IN ('JN', 'NAN', 'SN', 'SD')
          AND provincia IS NOT NULL
        GROUP BY provincia
        ORDER BY total DESC
        LIMIT 20
    """).df()


# ── Chart helpers ──────────────────────────────────────────────────────────

def _staggered_annotations(fig, entries: list[dict], line_dash: str = "dot",
                            x_min=None, x_max=None,
                            position: str = "top") -> go.Figure:
    """
    Draw vertical lines with horizontal labels that never overlap each other.

    position="top"    — labels start at 0.98 and step downward  (events)
    position="bottom" — labels start at 0.02 and step upward    (fare hikes)

    The y-axis is expanded in the corresponding direction so labels never
    overlap the data.
    """
    # Estimate label width in days based on character count.
    # At font-size 9, each character is ~5.5px wide. A typical 5-year chart
    # at ~900px wide spans ~1825 days, so px-per-day ≈ 900/1825 ≈ 0.49.
    # days_per_char ≈ 5.5 / 0.49 ≈ 11 days per character.
    DAYS_PER_CHAR = 11
    LINE_STEP     = 0.05

    if position == "bottom":
        BASE_Y    = 0.02   # start near the bottom
        STEP_DIR  = +1     # step upward (increasing paper-y)
        yanchor   = "bottom"
    else:
        BASE_Y    = 0.98   # start near the top
        STEP_DIR  = -1     # step downward (decreasing paper-y)
        yanchor   = "top"

    # Infer x_min / x_max from existing data traces if not supplied
    if x_min is None or x_max is None:
        all_x = []
        for trace in fig.data:
            xs = getattr(trace, "x", None)
            if xs is not None:
                all_x.extend([v for v in xs if v is not None])
        if all_x:
            x_min = x_min or min(all_x)
            x_max = x_max or max(all_x)

    # Filter entries to only those within the chart's x range
    if x_min is not None and x_max is not None:
        entries = [
            e for e in entries
            if pd.Timestamp(x_min) <= e["ts"] <= pd.Timestamp(x_max)
        ]

    if not entries:
        return fig

    dates  = [e["ts"]    for e in entries]
    hovers = [e["hover"] for e in entries]
    colors = [e["color"] for e in entries]

    # placed: list of (ts, y_paper, label_width_days) for every committed label
    placed: list[tuple] = []

    def clashes(ts, y, label):
        """True if a label at (ts, y) overlaps any already-placed label."""
        new_width = len(label) * DAYS_PER_CHAR
        for p_ts, p_y, p_width in placed:
            x_overlap = abs((ts - p_ts).days) < (new_width + p_width) / 2
            y_overlap  = abs(y - p_y) < LINE_STEP * 0.9
            if x_overlap and y_overlap:
                return True
        return False

    for i, ev in enumerate(entries):
        ts = ev["ts"]

        y = BASE_Y
        while clashes(ts, y, ev["label"]):
            y += STEP_DIR * LINE_STEP

        placed.append((ts, y, len(ev["label"]) * DAYS_PER_CHAR))

        fig.add_vline(
            x=ts.timestamp() * 1000,
            line_dash=line_dash,
            line_color=ev["color"],
            opacity=0.45,
        )
        fig.add_annotation(
            x=ts,
            y=y,
            yref="paper",
            text=ev["label"],
            showarrow=False,
            font=dict(size=9, color=ev["color"]),
            xanchor="left",
            yanchor=yanchor,
            bgcolor=None,
            borderpad=2,
        )

    # Invisible scatter for rich hover — constrained to the chart's x range
    # xaxis="x" with no actual y keeps it off the data plane;
    # cliponaxis=True prevents it from extending the auto-range
    fig.add_trace(go.Scatter(
        x=dates,
        y=[None] * len(dates),
        mode="markers",
        marker=dict(symbol="line-ns-open", size=14, color=colors,
                    line=dict(width=2, color=colors)),
        text=hovers,
        hovertemplate="%{text}<extra></extra>",
        showlegend=False,
        cliponaxis=True,
    ))

    if x_min is not None and x_max is not None:
        fig.update_xaxes(range=[pd.Timestamp(x_min), pd.Timestamp(x_max)])

    # Push the y-axis top up so labels never overlap the data.
    # Collect all y-values from data traces (skip the annotation scatter
    # which has y=None) and set yaxis range max to 110% of data max,
    # leaving the top 20% of paper space for labels.
    all_y = []
    for trace in fig.data[:-1]:
        ys = getattr(trace, "y", None)
        if ys is not None:
            all_y.extend([v for v in ys if v is not None])

    if all_y and placed:
        data_max = max(all_y)
        data_min = min(v for v in all_y if v is not None)

        if position == "top":
            # Push y-axis ceiling up to give labels headroom above data
            lowest_label_y = min(y for _, y, _ in placed)
            label_fraction = 1.0 - lowest_label_y + LINE_STEP
            y_top = data_max / (1.0 - label_fraction) if label_fraction < 1.0 else data_max * 1.25
            current_range = fig.layout.yaxis.range
            y_bottom = current_range[0] if current_range else min(0, data_min)
            fig.update_yaxes(range=[y_bottom, y_top])
        else:
            # Push y-axis floor down to give labels headroom below data
            highest_label_y = max(y for _, y, _ in placed)
            label_fraction = highest_label_y + LINE_STEP
            y_bottom = data_min - abs(data_min) * (label_fraction / (1.0 - label_fraction + 1e-9))
            current_range = fig.layout.yaxis.range
            y_top = current_range[1] if current_range else data_max * 1.05
            fig.update_yaxes(range=[y_bottom, y_top])

    return fig


def add_event_annotations(fig, y_ref: float = 0):
    """
    Draw vertical dotted lines for key historical events (EVENTS).
    Labels are staggered vertically to prevent overlap.
    Hover tooltip shows date, label, and notes.
    """
    lang    = st.session_state.lang
    entries = []

    for ev in EVENTS:
        ts   = pd.Timestamp(ev["date"])
        lbl  = ev.get(f"label_{lang}", ev.get("label_es", ""))
        note = ev.get("notes", "")
        hover = f"<b>{ts.strftime('%d/%m/%Y')}</b><br>{lbl}"
        if note:
            hover += f"<br><i>{note}</i>"
        entries.append({"ts": ts, "label": lbl, "hover": hover, "color": ev["color"]})

    return _staggered_annotations(fig, entries, line_dash="dot")


def add_fare_annotations(fig, y_ref: float = 0, scope_filter: list | None = None):
    """
    Draw vertical dashed lines for fare hike events (FARE_HIKES).
    Labels are staggered vertically to prevent overlap.
    Hover tooltip shows date, scope, magnitude, and notes.
    scope_filter: if given, only draw hikes whose scope is in the list.
    """
    lang = st.session_state.lang
    scope_colors = {
        "national":   "#7C3AED",
        "amba":       "#9F67E8",
        "amba_local": "#BFA0E8",
        "interior":   "#C084FC",
    }
    entries = []

    for h in FARE_HIKES:
        if scope_filter and h["scope"] not in scope_filter:
            continue

        ts    = pd.Timestamp(h["date"])
        lbl   = h.get(f"label_{lang}", h.get("label_es", ""))
        scope = h["scope"]
        mag   = h["magnitude"]
        note  = h.get("notes", "")
        color = scope_colors.get(scope, "#7C3AED")

        mag_str = f"+{mag}%" if mag > 0 else ("congelamiento" if lang == "es" else "freeze")
        # Short label for the staggered tag: just the magnitude
        short_lbl = mag_str
        hover = f"<b>{ts.strftime('%d/%m/%Y')}</b> · {mag_str}<br>{lbl}<br><i>Scope: {scope}</i>"
        if note:
            hover += f"<br>{note}"

        entries.append({"ts": ts, "label": short_lbl, "hover": hover, "color": color})

    return _staggered_annotations(fig, entries, line_dash="dash", position="bottom")


def mode_color_map() -> dict:
    return {mode: MODE_COLORS[mode] for mode in DASHBOARD_MODES}


def explainer(key: str) -> None:
    """Render a collapsible explainer box for any chart."""
    with st.expander("ℹ️ " + ("¿Cómo leer este gráfico?" if st.session_state.lang == "es" else "How to read this chart?")):
        st.markdown(t(key))


# ── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    col_es, col_en = st.columns(2)
    with col_es:
        if st.button("🇦🇷 Español", type="primary" if st.session_state.lang == "es" else "secondary"):
            st.session_state.lang = "es"
            st.rerun()
    with col_en:
        if st.button("🇬🇧 English", type="primary" if st.session_state.lang == "en" else "secondary"):
            st.session_state.lang = "en"
            st.rerun()

    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/thumb/1/1a/SUBE_Logo.svg/320px-SUBE_Logo.svg.png",
        width=140,
    )
    st.title(t("sidebar_title"))
    st.caption(t("sidebar_source"))
    st.divider()

    daily = load_daily_totals()
    min_date = daily["fecha"].min()
    max_date = daily["fecha"].max()

    date_range = st.date_input(
        t("periodo"),
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    selected_modes = st.multiselect(
        t("modos"),
        options=DASHBOARD_MODES,
        default=DASHBOARD_MODES,
        format_func=mode_label,
    )

    show_events = st.toggle(t("show_events"), value=True)

    st.divider()
    if st.button(t("refresh"), width="stretch"):
        st.cache_data.clear()
        st.rerun()

    st.caption(f"{t('data_until')}: **{max_date.strftime('%d/%m/%Y')}**")


# ── Date / mode filtering ──────────────────────────────────────────────────
if len(date_range) == 2:
    start_date = pd.Timestamp(date_range[0])
    end_date   = pd.Timestamp(date_range[1])
else:
    start_date = daily["fecha"].min()
    end_date   = daily["fecha"].max()

if not selected_modes:
    st.warning("Seleccioná al menos un modo." if st.session_state.lang == "es" else "Please select at least one mode.")
    st.stop()

df_daily = daily[
    (daily["fecha"] >= start_date) &
    (daily["fecha"] <= end_date) &
    (daily["modo"].isin(selected_modes))
]

monthly = load_monthly()
df_monthly = monthly[
    (monthly["month_start"] >= start_date) &
    (monthly["month_start"] <= end_date) &
    (monthly["modo"].isin(selected_modes))
]

cmap = mode_color_map()


# ── KPI cards ──────────────────────────────────────────────────────────────
st.title(t("page_title"))

total_by_day = df_daily.groupby("fecha")["cantidad_usos"].sum()

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric(t("kpi_total"), f"{df_daily['cantidad_usos'].sum()/1e9:.2f}B")
with c2:
    if not total_by_day.empty:
        peak_day = total_by_day.idxmax()
        st.metric(t("kpi_peak"), peak_day.strftime("%d/%m/%Y"),
                  f"{total_by_day.max()/1e6:.1f}M {t('kpi_trips')}")
with c3:
    if not total_by_day.empty:
        st.metric(t("kpi_avg"), f"{total_by_day.mean()/1e6:.2f}M {t('kpi_trips')}")
with c4:
    if not df_daily.empty:
        top_mode = df_daily.groupby("modo")["cantidad_usos"].sum().idxmax()
        st.metric(t("kpi_top_mode"), mode_label(top_mode))

st.caption(t("kpi_explainer"))
st.divider()


# ── Tabs ───────────────────────────────────────────────────────────────────
tab_ov, tab_cv, tab_ms, tab_rs, tab_an, tab_fc = st.tabs([
    t("tab_overview"), t("tab_covid"), t("tab_modal"),
    t("tab_resilience"), t("tab_analysis"), t("tab_forecast"),
])


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 1 — OVERVIEW
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_ov:

    st.subheader(t("ov_series_title"))
    explainer("ov_series_explainer")

    fig = go.Figure()
    for mode in selected_modes:
        mode_df = df_daily[df_daily["modo"] == mode].sort_values("fecha")
        ma7 = mode_df["cantidad_usos"].rolling(7, min_periods=1).mean()
        fig.add_scatter(
            x=mode_df["fecha"], y=mode_df["cantidad_usos"],
            mode="lines", name=f"{mode_label(mode)} (raw)",
            line=dict(color=cmap[mode], width=1),
            opacity=0.2, showlegend=False,
        )
        fig.add_scatter(
            x=mode_df["fecha"], y=ma7,
            mode="lines", name=mode_label(mode),
            line=dict(color=cmap[mode], width=2.5),
        )

    if show_events:
        fig = add_event_annotations(fig)

    fig.update_layout(
        height=450, template="plotly_white",
        yaxis_title=t("ov_series_y"),
        legend_title=None, hovermode="x unified",
    )
    st.plotly_chart(fig, width="stretch")

    st.subheader(t("ov_split_title"))
    explainer("ov_split_explainer")

    split = load_modal_split()
    split = split[
        (split["month_start"] >= start_date) &
        (split["month_start"] <= end_date) &
        (split["modo"].isin(selected_modes))
    ].copy()
    split["modo_label"] = split["modo"].map(mode_label)

    fig2 = px.area(
        split, x="month_start", y="mode_share_pct",
        color="modo_label",
        color_discrete_map={mode_label(m): cmap[m] for m in selected_modes},
        labels={"mode_share_pct": t("ov_split_y"), "month_start": "", "modo_label": ""},
        template="plotly_white",
    )
    if show_events:
        fig2 = add_event_annotations(fig2)
    fig2.update_layout(height=350, yaxis_ticksuffix="%", hovermode="x unified")
    st.plotly_chart(fig2, width="stretch")

    st.subheader(t("ov_empresas_title"))
    explainer("ov_empresas_explainer")

    empresas = load_top_empresas()
    empresas = empresas[empresas["modo"].isin(selected_modes)].copy()
    empresas["modo_label"]    = empresas["modo"].map(mode_label)
    empresas["empresa_short"] = empresas["nombre_empresa"].str[:35]

    fig3 = px.bar(
        empresas.sort_values("total_usos"),
        x="total_usos", y="empresa_short",
        color="modo_label",
        color_discrete_map={mode_label(m): cmap[m] for m in selected_modes},
        orientation="h",
        labels={"total_usos": t("ov_empresas_y"), "empresa_short": t("ov_empresas_x"), "modo_label": ""},
        template="plotly_white",
    )
    fig3.update_layout(height=400)
    st.plotly_chart(fig3, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 2 — COVID-19
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_cv:

    st.subheader(t("cv_collapse_title"))
    explainer("cv_collapse_explainer")

    covid_monthly = monthly[
        (monthly["month_start"] >= "2020-01-01") &
        (monthly["month_start"] <= "2022-07-01") &
        (monthly["modo"].isin(selected_modes))
    ].copy()
    covid_monthly["modo_label"] = covid_monthly["modo"].map(mode_label)

    fig4 = px.line(
        covid_monthly, x="month_start", y="total_usos",
        color="modo_label",
        color_discrete_map={mode_label(m): cmap[m] for m in selected_modes},
        markers=True,
        labels={"total_usos": t("ov_series_y"), "month_start": "", "modo_label": ""},
        template="plotly_white",
    )
    fig4 = add_event_annotations(fig4)
    fig4.update_layout(height=420, hovermode="x unified")
    st.plotly_chart(fig4, width="stretch")

    st.divider()

    st.subheader(t("cv_yoy_title"))
    explainer("cv_yoy_explainer")

    yoy = load_yoy()
    yoy_covid = yoy[
        (yoy["month_start"] >= "2020-01-01") &
        (yoy["month_start"] <= "2022-07-01") &
        (yoy["modo"].isin(selected_modes))
    ].dropna(subset=["yoy_pct_change"]).copy()
    yoy_covid["modo_label"] = yoy_covid["modo"].map(mode_label)

    fig6 = px.bar(
        yoy_covid, x="month_start", y="yoy_pct_change",
        color="modo_label", barmode="group",
        color_discrete_map={mode_label(m): cmap[m] for m in selected_modes},
        labels={"yoy_pct_change": t("cv_yoy_y"), "month_start": "", "modo_label": ""},
        template="plotly_white",
    )
    fig6.add_hline(y=0, line_color="black", line_width=1)
    fig6.update_layout(height=400, yaxis_ticksuffix="%", hovermode="x unified")
    st.plotly_chart(fig6, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 3 — MODAL SUBSTITUTION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_ms:

    # MoM % — all modes, full date range, all events annotated
    st.subheader(t("ms_mom_title"))
    explainer("ms_mom_explainer")

    ms_df = monthly[
        (monthly["month_start"] >= start_date) &
        (monthly["month_start"] <= end_date) &
        (monthly["modo"].isin(selected_modes))
    ].copy().sort_values(["modo", "month_start"])
    ms_df["mom_pct"]    = ms_df.groupby("modo")["total_usos"].pct_change() * 100
    ms_df               = ms_df.dropna(subset=["mom_pct"])
    ms_df["modo_label"] = ms_df["modo"].map(mode_label)

    fig_ms1 = px.line(
        ms_df, x="month_start", y="mom_pct",
        color="modo_label",
        color_discrete_map={mode_label(m): cmap[m] for m in selected_modes},
        markers=True,
        labels={"mom_pct": "MoM %", "month_start": "", "modo_label": ""},
        template="plotly_white",
    )
    fig_ms1.add_hline(y=0, line_color="black", line_width=1, opacity=0.4)
    if show_events:
        fig_ms1 = add_event_annotations(fig_ms1)
        fig_ms1 = add_fare_annotations(fig_ms1)
    fig_ms1.update_layout(
        height=420, yaxis_ticksuffix="%", hovermode="x unified",
        yaxis=dict(range=[-100, 200]),  # clip outliers; full values still show on hover
    )
    st.plotly_chart(fig_ms1, width="stretch")

    st.divider()

    # Modal share — full series
    st.subheader(t("ms_share_title"))
    explainer("ms_share_explainer")

    share_df = load_modal_split()
    share_df = share_df[
        (share_df["month_start"] >= start_date) &
        (share_df["month_start"] <= end_date) &
        (share_df["modo"].isin(selected_modes))
    ].copy()
    share_df["modo_label"] = share_df["modo"].map(mode_label)

    fig_ms2 = px.area(
        share_df, x="month_start", y="mode_share_pct",
        color="modo_label",
        color_discrete_map={mode_label(m): cmap[m] for m in selected_modes},
        labels={"mode_share_pct": t("ov_split_y"), "month_start": "", "modo_label": ""},
        template="plotly_white",
    )
    if show_events:
        fig_ms2 = add_event_annotations(fig_ms2)
        fig_ms2 = add_fare_annotations(fig_ms2)
    fig_ms2.update_layout(height=380, yaxis_ticksuffix="%", hovermode="x unified")
    st.plotly_chart(fig_ms2, width="stretch")

    st.divider()

    # YoY % — full series
    st.subheader(t("ms_yoy_title"))
    explainer("ms_yoy_explainer")

    yoy_all = load_yoy()
    yoy_all = yoy_all[
        (yoy_all["month_start"] >= start_date) &
        (yoy_all["month_start"] <= end_date) &
        (yoy_all["modo"].isin(selected_modes))
    ].dropna(subset=["yoy_pct_change"]).copy()
    yoy_all["modo_label"] = yoy_all["modo"].map(mode_label)

    fig_ms3 = px.bar(
        yoy_all, x="month_start", y="yoy_pct_change",
        color="modo_label", barmode="group",
        color_discrete_map={mode_label(m): cmap[m] for m in selected_modes},
        labels={"yoy_pct_change": t("cv_yoy_y"), "month_start": "", "modo_label": ""},
        template="plotly_white",
    )
    fig_ms3.add_hline(y=0, line_color="black", line_width=1)
    if show_events:
        fig_ms3 = add_event_annotations(fig_ms3)
        fig_ms3 = add_fare_annotations(fig_ms3)
    fig_ms3.update_layout(height=420, yaxis_ticksuffix="%", hovermode="x unified")
    st.plotly_chart(fig_ms3, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 4 — RESILIENCE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_rs:

    lang        = st.session_state.lang
    amba_df     = load_amba_recovery()
    amba_labels = STRINGS[lang]["amba_labels"]
    amba_colors = {"SI": "#2563EB", "NO": "#F59E0B"}

    amba_plot           = amba_df.copy()
    amba_plot["region"] = amba_plot["amba"].map(amba_labels)

    # Dual y-axis so AMBA and Interior use independent scales
    st.subheader(t("rs_amba_title"))
    explainer("rs_amba_explainer")

    amba_series     = amba_plot[amba_plot["amba"] == "SI"].sort_values("month_start")
    interior_series = amba_plot[amba_plot["amba"] == "NO"].sort_values("month_start")

    fig7 = go.Figure()
    fig7.add_trace(go.Scatter(
        x=amba_series["month_start"], y=amba_series["total"],
        name=amba_labels["SI"],
        line=dict(color=amba_colors["SI"], width=2),
        yaxis="y1",
    ))
    fig7.add_trace(go.Scatter(
        x=interior_series["month_start"], y=interior_series["total"],
        name=amba_labels["NO"],
        line=dict(color=amba_colors["NO"], width=2),
        yaxis="y2",
    ))
    if show_events:
        fig7 = add_event_annotations(fig7)

    fig7.update_layout(
        height=430,
        hovermode="x unified",
        template="plotly_white",
        legend=dict(orientation="h", y=-0.15),
        yaxis=dict(
            title=dict(text=f"AMBA — {t('ov_series_y')}", font=dict(color=amba_colors["SI"])),
            tickfont=dict(color=amba_colors["SI"]),
        ),
        yaxis2=dict(
            title=dict(text=f"Interior — {t('ov_series_y')}", font=dict(color=amba_colors["NO"])),
            tickfont=dict(color=amba_colors["NO"]),
            overlaying="y",
            side="right",
        ),
    )
    st.plotly_chart(fig7, width="stretch")

    st.divider()

    st.subheader(t("rs_milei_title"))
    explainer("rs_milei_explainer")

    milei_df = amba_plot[amba_plot["month_start"] >= "2023-01-01"].copy()

    fig8 = px.line(
        milei_df, x="month_start", y="recovery_index",
        color="region",
        color_discrete_map={v: amba_colors[k] for k, v in amba_labels.items()},
        markers=True,
        labels={"recovery_index": "Índice (Ene 2020 = 100)" if lang == "es" else "Index (Jan 2020 = 100)",
                "month_start": "", "region": ""},
        template="plotly_white",
    )
    fig8.add_hline(y=100, line_dash="dash", line_color="grey", opacity=0.4,
                   annotation_text="Ene 2020" if lang == "es" else "Jan 2020")
    fig8 = add_fare_annotations(fig8)
    fig8.update_layout(height=380, hovermode="x unified")
    st.plotly_chart(fig8, width="stretch")

    st.divider()

    st.subheader(t("rs_seasonal_title"))
    explainer("rs_seasonal_explainer")

    # Compute peak/trough ratio per year per region (exclude 2020 — COVID distorts it)
    amba_plot["year"]  = pd.to_datetime(amba_plot["month_start"]).dt.year
    amba_plot["month"] = pd.to_datetime(amba_plot["month_start"]).dt.month
    seasonal_amp = (
        amba_plot[amba_plot["year"] >= 2021]
        .groupby(["year", "region"])["total"]
        .agg(peak="max", trough="min")
        .reset_index()
    )
    seasonal_amp["amplitude"] = seasonal_amp["peak"] / seasonal_amp["trough"]

    fig_sa = px.line(
        seasonal_amp, x="year", y="amplitude",
        color="region",
        color_discrete_map={v: amba_colors[k] for k, v in amba_labels.items()},
        markers=True,
        labels={"amplitude": "Pico / Valle" if lang == "es" else "Peak / Trough",
                "year": "", "region": ""},
        template="plotly_white",
    )
    fig_sa.add_hline(y=1.0, line_dash="dash", line_color="grey", opacity=0.4)
    fig_sa.update_layout(height=340, hovermode="x unified")
    st.plotly_chart(fig_sa, width="stretch")
    explainer("rs_prov_explainer")

    prov_df = load_by_provincia()
    fig9 = px.bar(
        prov_df.sort_values("total"),
        x="total", y="provincia",
        orientation="h",
        labels={"total": t("ov_empresas_y"), "provincia": "Provincia"},
        template="plotly_white",
        color="total",
        color_continuous_scale="Blues",
    )
    fig9.update_layout(height=500, coloraxis_showscale=False)
    st.plotly_chart(fig9, width="stretch")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 4 — ANALYSIS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
with tab_an:

    st.subheader(t("an_heatmap_title"))
    explainer("an_heatmap_explainer")

    heatmap_df = load_heatmap()
    pivot = heatmap_df.pivot(index="day_of_week", columns="month", values="avg_usos")
    pivot.index   = STRINGS[st.session_state.lang]["days"]
    pivot.columns = STRINGS[st.session_state.lang]["months"]

    fig10 = px.imshow(
        pivot,
        color_continuous_scale="Blues",
        labels={"color": t("an_heatmap_color")},
        template="plotly_white",
        aspect="auto",
    )
    fig10.update_layout(height=320)
    st.plotly_chart(fig10, width="stretch")

    st.divider()

    st.subheader(t("an_stl_title"))
    explainer("an_stl_explainer")

    col_l, col_r = st.columns(2)
    with col_l:
        stl_mode = st.selectbox(
            t("an_stl_mode"),
            ["ALL"] + [m for m in DASHBOARD_MODES if m in selected_modes],
            format_func=lambda m: t("an_stl_all") if m == "ALL" else mode_label(m),
        )
    with col_r:
        stl_period = st.radio(
            t("an_stl_season"),
            [7, 365],
            format_func=lambda p: t("an_stl_weekly") if p == 7 else t("an_stl_annual"),
            horizontal=True,
        )

    if st.button(t("an_stl_run"), type="primary"):
        with st.spinner(t("an_stl_running")):
            try:
                from analytics.time_series import decompose_series, detect_anomalies
                conn = get_conn()
                result = decompose_series(
                    conn,
                    mode=None if stl_mode == "ALL" else stl_mode,
                    period=stl_period,
                )
                if result:
                    anomalies = detect_anomalies(
                        result["residual"],
                        lang=st.session_state.lang,
                    )

                    fig11 = go.Figure()
                    for key, name, color, fill in [
                        ("original", t("an_stl_observed"), "rgba(100,100,200,0.25)", True),
                        ("trend",    t("an_stl_trend"),    "#2563EB",                False),
                        ("seasonal", t("an_stl_seasonal"), "#16A34A",                False),
                        ("residual", t("an_stl_residual"), "#94a3b8",                False),
                    ]:
                        s = result[key]
                        fig11.add_scatter(
                            x=s.index, y=s.values, name=name,
                            line_color=color,
                            fill="tozeroy" if fill else None,
                            opacity=0.8 if fill else 1.0,
                        )

                    anom = anomalies[anomalies["is_anomaly"]]
                    fig11.add_scatter(
                        x=anom["fecha"], y=anom["residual"],
                        mode="markers", name=t("an_stl_anomaly"),
                        marker=dict(color="red", size=7, symbol="x"),
                        hovertemplate="<b>%{x}</b><br>%{y:,.0f}<br>%{text}",
                        text=anom["event_label"],
                    )
                    fig11.update_layout(
                        height=550, template="plotly_white",
                        hovermode="x unified",
                        legend_title=t("an_stl_component"),
                    )
                    st.plotly_chart(fig11, width="stretch")

                    if not anom.empty:
                        st.subheader(f"🚨 {len(anom)} {t('an_anom_title')}")
                        st.caption(t("an_anom_explainer"))
                        st.dataframe(
                            anom[["fecha", "z_score", "event_label"]]
                            .sort_values("z_score", key=abs, ascending=False)
                            .head(20)
                            .rename(columns={
                                "fecha":       t("an_anom_date"),
                                "z_score":     t("an_anom_z"),
                                "event_label": t("an_anom_event"),
                            }),
                            width="stretch",
                        )
            except ImportError:
                st.error("statsmodels not installed. Run: uv add statsmodels")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# TAB 5 — FORECAST
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _hex_to_rgb(hex_color: str) -> tuple[float, float, float]:
    """Convert '#RRGGBB' to (r, g, b) floats in 0–1 range."""
    h = hex_color.lstrip("#")
    return tuple(int(h[i:i+2], 16) / 255 for i in (0, 2, 4))


with tab_fc:

    explainer("fc_explainer")

    horizon = st.slider(t("fc_horizon"), min_value=3, max_value=12, value=6, step=1)

    if st.button(t("fc_run"), type="primary"):
        with st.spinner(t("fc_running")):
            try:
                from analytics.ml import forecast_ridership, forecast_summary
                conn = get_conn()
                forecasts = forecast_ridership(
                    conn,
                    modes=[m for m in selected_modes if m in DASHBOARD_MODES],
                    horizon=horizon,
                )

                if not forecasts:
                    st.warning(
                        "No forecast results. Check that the pipeline has run and data is loaded."
                    )
                else:
                    st.subheader(t("fc_title").format(n=horizon))

                    for mode, fc in forecasts.items():
                        hist = fc[~fc["is_forecast"]]
                        pred = fc[fc["is_forecast"]]
                        r, g, b = _hex_to_rgb(cmap[mode])

                        fig = go.Figure()

                        # Confidence band
                        fig.add_scatter(
                            x=pd.concat([pred["ds"], pred["ds"].iloc[::-1]]),
                            y=pd.concat([pred["yhat_upper"], pred["yhat_lower"].iloc[::-1]]),
                            fill="toself",
                            fillcolor=f"rgba({int(r*255)},{int(g*255)},{int(b*255)},0.15)",
                            line=dict(width=0),
                            showlegend=True,
                            name=t("fc_band"),
                        )

                        # Raw actuals as faded dots — context without visual noise
                        fig.add_scatter(
                            x=hist["ds"], y=hist["actual"],
                            mode="markers",
                            marker=dict(color=cmap[mode], size=5, opacity=0.35),
                            name=t("fc_actual"),
                            showlegend=True,
                        )

                        # Fitted values (historical) — smooth line that leads into forecast
                        fig.add_scatter(
                            x=hist["ds"], y=hist["yhat"],
                            mode="lines",
                            line=dict(color=cmap[mode], width=2),
                            name=t("fc_fitted"),
                            showlegend=True,
                        )

                        # Forecast line — visually continuous from fitted
                        # Prepend the last fitted point so there's no gap
                        last_hist = hist.iloc[[-1]]
                        pred_with_join = pd.concat([last_hist, pred], ignore_index=True)
                        fig.add_scatter(
                            x=pred_with_join["ds"], y=pred_with_join["yhat"],
                            mode="lines+markers",
                            line=dict(color=cmap[mode], width=2, dash="dash"),
                            marker=dict(size=6),
                            name=t("fc_forecast"),
                        )

                        # Vertical marker at forecast start
                        fig.add_vline(
                            x=hist["ds"].max().timestamp() * 1000,
                            line_dash="dot", line_color="grey", opacity=0.6,
                            annotation_text=(
                                "→ predicción" if st.session_state.lang == "es" else "→ forecast"
                            ),
                            annotation_font_size=10,
                        )

                        if show_events:
                            fig = add_event_annotations(fig)
                            fig = add_fare_annotations(fig)

                        fig.update_layout(
                            height=320,
                            template="plotly_white",
                            title=mode_label(mode),
                            yaxis_title=t("ov_series_y"),
                            hovermode="x unified",
                            legend=dict(orientation="h", y=-0.25),
                            margin=dict(t=40, b=60),
                        )
                        st.plotly_chart(fig, width="stretch")

                    # ── Summary table ──────────────────────────────────────
                    st.divider()
                    st.subheader(t("fc_summary_title"))

                    summary = forecast_summary(forecasts)
                    if not summary.empty:
                        direction_map = {
                            "up":   t("fc_direction_up"),
                            "down": t("fc_direction_down"),
                            "flat": t("fc_direction_flat"),
                        }
                        summary["mode"]          = summary["mode"].map(mode_label)
                        summary["direction"]     = summary["direction"].map(direction_map)
                        summary["last_actual"]   = summary["last_actual"].apply(lambda x: f"{x/1e6:.1f}M")
                        summary["mean_forecast"] = summary["mean_forecast"].apply(lambda x: f"{x/1e6:.1f}M")
                        summary["pct_change"]    = summary["pct_change"].apply(lambda x: f"{x:+.1f}%")

                        st.dataframe(
                            summary.rename(columns={
                                "mode":          t("fc_mode"),
                                "last_actual":   t("fc_last"),
                                "mean_forecast": t("fc_mean"),
                                "pct_change":    t("fc_change"),
                                "direction":     "",
                            }),
                            width="stretch",
                            hide_index=True,
                        )

            except ImportError as e:
                st.error(f"Missing dependency: {e}. Run: uv add prophet")


st.divider()
st.caption(
    "Fuente / Source: [datos.transporte.gob.ar](https://datos.transporte.gob.ar) · "
    "CC Attribution 4.0 · Actualización automática diaria / Daily auto-update"
)