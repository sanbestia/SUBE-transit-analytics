"""
dashboard/strings.py — Bilingual UI strings and mode label translations.

Import from here instead of defining inline in app.py:
    from dashboard.strings import STRINGS, MODE_LABELS
"""

STRINGS = {
    "es": {
        "page_title":       "SUBE — Análisis de Transporte Público",
        "sidebar_title":    "SUBE Analytics",
        "sidebar_source":   "Datos: datos.transporte.gob.ar",
        "periodo":          "Período",
        "desde":            "Desde",
        "hasta":            "Hasta",
        "modos":            "Modos de transporte",
        "show_events":      "Mostrar eventos históricos",
        "refresh":          "🔄 Actualizar datos",
        "data_until":       "Datos hasta",
        "kpi_total":        "Total de viajes",
        "kpi_peak":         "Día pico",
        "kpi_avg":          "Promedio diario",
        "kpi_top_mode":     "Modo dominante",
        "kpi_trips":        "viajes",
        "tab_overview":     "📊 Estructura de datos",
        "tab_covid":        "🦠 COVID-19",
        "tab_modal":        "🔄 Sustitución Modal",
        "tab_resilience":   "🗺️ AMBA vs Interior",
        "tab_analysis":     "🔍 Anomalías",

        # ── Overview ──────────────────────────────────────────────────────
        "ov_series_title":    "Ridership diario",
        "ov_excl_lockdown":   "Excluir lockdown (abr 2020 – jul 2021)",
        "ov_series_explainer": "Cantidad de viajes por modo de transporte. "
                               "**Antes de 2020**: datos mensuales del sistema SUBE expresados como promedio diario (un punto por mes). "
                               "El COLECTIVO tiene datos desde 2013; SUBTE y TREN desde 2016, cuando la integración SUBE alcanzó cobertura completa. "
                               "**Desde 2020**: datos diarios reales — la línea fina muestra el valor diario; la línea gruesa es el **promedio móvil de 7 días**, "
                               "que suaviza las variaciones normales entre días de semana y fin de semana para revelar la tendencia subyacente. "
                               "Las líneas verticales punteadas marcan eventos históricos clave.",
        "ov_series_y":        "Viajes",
        "ov_split_title":     "Participación por modo (modal split mensual)",
        "ov_split_explainer": "El **modal split** muestra qué porcentaje del total de viajes corresponde a cada modo en cada mes. "
                               "La serie comienza en 2016, cuando los tres modos principales tenían cobertura SUBE completa. "
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
        "cv_subst_recovery_title":   "Recuperación modal desde el piso del lockdown (abr 2020 = 100)",
        "cv_subst_recovery_explainer": "Índice de ridership relativo al piso del lockdown (abril 2020, el mes de mayor caída). "
                                        "Muestra la velocidad de recuperación de cada modo desde el punto más bajo. "
                                        "El SUBTE creció más rápido que el COLECTIVO — sus pasajeros volvieron "
                                        "antes y con más consistencia, posiblemente por necesidad (sin alternativa viable).",
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
        "rs_milei_explainer":  "Zoom sobre el período desde enero 2023. El eje y muestra el **índice de ridership** relativo a enero 2020 (= 100). "
                                "Un valor de 80 significa que ese mes hubo un 20 % menos viajes que en enero 2020. "
                                "Las líneas verticales punteadas marcan cada aumento tarifario documentado. "
                                "La **línea punteada gruesa** superpuesta es el **promedio móvil interanual** (12 meses): "
                                "suaviza la estacionalidad y revela la tendencia estructural de mediano plazo. "
                                "El AMBA recibió los aumentos más concentrados (enero y febrero 2024, +45 % y +66 %); "
                                "el Interior absorbió el corte del Fondo de Compensación de forma más gradual. "
                                "La divergencia entre ambas curvas a partir de 2024 sugiere que el shock tarifario "
                                "afectó de manera diferencial a cada región.",
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

        "kpi_explainer": "Métricas calculadas sobre el total histórico disponible (todos los modos). "
                          "El **día pico** es el día individual con más viajes registrados. "
                          "El **promedio diario** incluye fines de semana y feriados, por eso es menor que un típico día hábil.",

        # ── Finding callouts ──────────────────────────────────────────────
        "finding_bus_drop":      "Bus (Colectivo)",
        "finding_bus_drop_sub":  "participación en el último mes",
        "finding_tren_drop":     "Tren",
        "finding_tren_drop_sub": "participación en el último mes",
        "finding_subte_drop":    "Subte",
        "finding_subte_drop_sub": "participación en el último mes",
        "finding_amba_shock":    "Aumento tarifario interanual acumulado",
        "finding_amba_shock_sub": "últimos 12 meses · líneas nacionales y AMBA",
        "finding_seasonal":      "Pico estacional",
        "finding_seasonal_sub":  "Marzo–abril y ago–sep; mínimos en enero y julio",
        "finding_recovery":      "Índice de recuperación AMBA",

        # ── Tab finding callouts (visible, non-collapsible) ───────────────
        "cv_finding": "**El colapso fue asimétrico.** El SUBTE cayó un **92 %** en abril 2020 porque "
                      "casi todos sus pasajeros son trabajadores de oficina en CABA que no podían salir. "
                      "El COLECTIVO cayó sólo un **58 %** — los colectivos siguieron transportando trabajadores "
                      "esenciales. Cuando las restricciones se levantaron, el SUBTE se recuperó más rápido.",
        "tab_its":            "📉 Impacto Tarifario",
        "rs_its_title":       "Impacto causal del shock tarifario (ITS)",
        "rs_its_explainer":   """**¿Cómo leer este gráfico?**

La línea **real** muestra el ridership observado desde enero 2024. \
La línea **contrafactual** es la proyección del modelo asumiendo que el shock tarifario *no* hubiera ocurrido \
— es decir, si la tendencia pre-2024 hubiera continuado sin interrupciones. \
La brecha sombreada entre ambas líneas representa el impacto estimado del shock.

**Método: Regresión segmentada (Interrupted Time Series)**. \
Se ajusta un modelo OLS con:
- β₂ (cambio de nivel): salto inmediato en el mes del tratamiento
- β₃ (cambio de pendiente): deriva mensual acumulada después del tratamiento
- Controles: estacionalidad mensual, COVID-19, tendencia de largo plazo

**Errores estándar**: OLS para Colectivo (sin autocorrelación); \
Newey-West HAC-12 para Subte y Tren (autocorrelación residual detectada).

**Limitación importante**: β₂ y β₃ no pueden separar el efecto del precio del boleto \
del colapso del ingreso real producto de la devaluación de diciembre 2023 (+118%). \
La elasticidad implícita es una cota superior de la elasticidad precio pura.""",
        "rs_its_finding":     "**El shock tarifario de 2024 produjo erosión gradual, no una caída abrupta.** "
                              "Ningún modo muestra un cambio de nivel estadísticamente significativo en enero 2024 — "
                              "el ridership resistió el impacto inicial. Sin embargo, **Colectivo** acumula una "
                              "caída de tendencia de −1,6 M viajes/mes (p=0,018) y **Subte** de −0,29 M viajes/mes "
                              "(p<0,001) desde el tratamiento. **Tren** no muestra efecto significativo en ninguna "
                              "dimensión, consistente con una demanda más cautiva y sin alternativas claras.",
        "rs_its_actual":          "Ridership real",
        "rs_its_cf":              "Tendencia esperada (sin shock)",
        "rs_its_gap":             "Diferencia",
        "rs_its_treatment":       "Inicio del shock (Ene 2024)",
        "rs_its_metric_lost":     "Viajes por debajo de lo esperado",
        "rs_its_metric_lost_sub": "acumulado desde ene 2024",
        "rs_its_metric_now":      "Diferencia en el último mes",
        "rs_its_metric_now_sub":  "vs la tendencia sin shock",
        "rs_its_metric_drift":    "Tendencia post-shock",
        "rs_its_drift_falling":   "cayendo {n}M viajes/mes adicionales",
        "rs_its_drift_rising":    "creciendo {n}M viajes/mes adicionales",
        "rs_its_drift_flat":      "sin cambio significativo de tendencia",
        "rs_its_note":            "⚠️ La diferencia entre la línea real y la esperada combina el efecto de la suba tarifaria "
                                  "con el impacto de la devaluación de diciembre 2023 (+118%). "
                                  "No es posible separar ambos efectos con estos datos.",

        "rs_finding": "**El shock tarifario de 2024 fue nacional, pero el AMBA lo recibió de forma más concentrada.** "
                      "Los aumentos de enero (+45 %) y febrero (+66 %) se aplicaron a líneas de jurisdicción nacional. "
                      "El Interior también fue afectado, pero absorbió el corte del Fondo de Compensación de manera gradual y heterogénea "
                      "— cada provincia gestionó su propio calendario. "
                      "El AMBA muestra además **menor amplitud estacional** que el Interior, "
                      "posiblemente porque sus viajes están más dominados por el empleo formal "
                      "(aunque esto es una hipótesis que los datos por sí solos no confirman).",
        "an_finding": "**Los patrones son consistentes entre años.** Los días hábiles tienen "
                      "entre un 60 % y un 80 % más viajes que los fines de semana. "
                      "Marzo y agosto son los meses pico; enero y julio son los valles por vacaciones.",

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
        "fc_explainer": """**¿Cómo leer este gráfico?**

Los valores proyectados responden a una pregunta concreta: \
*¿hacia dónde va el uso del sistema si las tarifas, las condiciones macroeconómicas \
y las políticas de servicio actuales se mantienen sin cambios?* \
No es una garantía de lo que ocurrirá — es un escenario de referencia que cuantifica \
la inercia del sistema bajo el statu quo.

**¿Cómo funciona el modelo?**

Se usa **Prophet**, un modelo de series temporales desarrollado por Meta, entrenado con datos mensuales \
de ridership históricos. El COLECTIVO se entrena desde 2013; SUBTE y TREN desde 2016 \
(antes de esas fechas la cobertura SUBE era incompleta y los datos no son comparables). \
El modelo aprende tres cosas por separado:

- 📈 **Tendencia**: la dirección general de largo plazo (crecimiento o caída).
- 📅 **Estacionalidad anual**: los patrones que se repiten cada año \
(enero siempre tiene menos viajes; marzo y agosto son picos).
- 🎌 **Feriados argentinos**: los feriados nacionales se modelan explícitamente \
como caídas puntuales de demanda.

Además se incorporan cuatro **regresores externos**:

- 🦠 **Impacto COVID**: variable binaria que marca el período de distorsión (mar 2020 – dic 2021), \
permitiendo al modelo aprender la caída sin absorberla en la tendencia de largo plazo.
- 💸 **Presión tarifaria acumulada**: suma de todos los aumentos tarifarios que entraron en vigencia \
hasta cada mes (desde los tarifazos Macri de 2016–2019 hasta los aumentos escalonados de 2025–2026). \
No es un simple interruptor on/off — refleja la magnitud acumulada del ajuste tarifario.
- 📉 **Shock macroeconómico**: variable binaria que captura el cambio de régimen desde la \
devaluación de diciembre 2023 (+118%) y el recorte de subsidios, independientemente de las tarifas.
- 🔄 **Momentum de recuperación**: captura la desaceleración gradual del rebote post-COVID \
(la demanda se recuperó rápido en 2022 y luego se estabilizó en un nuevo nivel de equilibrio).

Los **puntos de cambio estructural** (changepoints) se fijan en las fechas exactas de cada aumento \
tarifario y evento macro, en lugar de dejarse descubrir automáticamente — esto mejora la precisión \
del modelo en períodos de alta volatilidad.

**¿Qué significa la banda sombreada?** Muestra el rango donde el modelo espera que caigan \
el 80% de los valores futuros. La banda se ensancha progresivamente con el horizonte: \
predecir el próximo mes es mucho más certero que predecir dentro de dos años, \
y el gráfico lo refleja.

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
        "desde":            "From",
        "hasta":            "To",
        "modos":            "Transport modes",
        "show_events":      "Show historical events",
        "refresh":          "🔄 Refresh data",
        "data_until":       "Data through",
        "kpi_total":        "Total trips",
        "kpi_peak":         "Peak day",
        "kpi_avg":          "Daily average",
        "kpi_top_mode":     "Dominant mode",
        "kpi_trips":        "trips",
        "tab_overview":     "📊 Data structure",
        "tab_covid":        "🦠 COVID-19",
        "tab_modal":        "🔄 Modal Substitution",
        "tab_resilience":   "🗺️ AMBA vs Interior",
        "tab_analysis":     "🔍 Anomalies",

        # ── Overview ──────────────────────────────────────────────────────
        "ov_series_title":    "Daily ridership",
        "ov_excl_lockdown":   "Exclude lockdown (Apr 2020 – Jul 2021)",
        "ov_series_explainer": "Trip counts by transport mode. "
                               "**Before 2020**: monthly SUBE data expressed as a daily average (one point per month). "
                               "COLECTIVO data goes back to 2013; SUBTE and TREN from 2016, when SUBE integration reached full coverage. "
                               "**From 2020 onwards**: actual daily data — the thin line shows the raw daily value; the thick line is the **7-day moving average**, "
                               "which smooths out normal weekday/weekend variation to reveal the underlying trend. "
                               "Dotted vertical lines mark key historical events.",
        "ov_series_y":        "Trips",
        "ov_split_title":     "Modal split (monthly)",
        "ov_split_explainer": "**Modal split** shows what percentage of total trips each mode accounts for each month. "
                               "The series starts in 2016, when all three main modes had full SUBE coverage. "
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
        "cv_subst_recovery_title":   "Modal recovery from lockdown trough (Apr 2020 = 100)",
        "cv_subst_recovery_explainer": "Ridership index relative to the lockdown trough (April 2020, the sharpest drop). "
                                        "Shows the speed of recovery from the lowest point for each mode. "
                                        "SUBTE grew faster than COLECTIVO — its passengers returned earlier "
                                        "and more consistently. This suggests subway riders used it out of "
                                        "necessity (no viable alternative), not just preference.",
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
        "rs_milei_explainer":  "Zoom on the period from January 2023. The y-axis shows the **ridership index** relative to January 2020 (= 100). "
                                "A value of 80 means that month had 20% fewer trips than January 2020. "
                                "Dotted vertical lines mark each documented fare hike. "
                                "The **thick dotted overlay** is the **12-month rolling average**: "
                                "it smooths out seasonality to reveal the medium-term structural trend. "
                                "AMBA received the most concentrated increases (January and February 2024, +45% and +66%); "
                                "Interior absorbed the Compensation Fund cut more gradually. "
                                "The divergence between the two curves from 2024 onward suggests the fare shock "
                                "affected each region differently.",
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

        "kpi_explainer": "Metrics calculated over the full historical dataset (all modes). "
                          "**Peak day** is the single day with the highest recorded trips. "
                          "**Daily average** includes weekends and holidays, so it is lower than a typical workday.",

        # ── Finding callouts ──────────────────────────────────────────────
        "finding_bus_drop":      "Bus (Colectivo)",
        "finding_bus_drop_sub":  "share of trips last month",
        "finding_tren_drop":     "Train",
        "finding_tren_drop_sub": "share of trips last month",
        "finding_subte_drop":    "Subway",
        "finding_subte_drop_sub": "share of trips last month",
        "finding_amba_shock":    "Cumulative year-on-year fare increase",
        "finding_amba_shock_sub": "last 12 months · national & AMBA lines",
        "finding_seasonal":      "Seasonal peak",
        "finding_seasonal_sub":  "Mar–Apr and Aug–Sep; troughs in Jan and Jul",
        "finding_recovery":      "AMBA recovery index",

        # ── Tab finding callouts (visible, non-collapsible) ───────────────
        "cv_finding": "**The collapse was asymmetric.** SUBTE fell **92%** in April 2020 because "
                      "almost all its passengers are office workers in CABA who couldn't leave home. "
                      "COLECTIVO fell only **58%** — buses kept running for essential workers. "
                      "When restrictions lifted, SUBTE recovered faster.",
        "tab_its":            "📉 Fare Impact",
        "rs_its_title":       "Causal impact of the fare shock (ITS)",
        "rs_its_explainer":   """**How to read this chart**

The **actual** line shows observed ridership since January 2024. \
The **counterfactual** line is the model's projection assuming the fare shock *had not* occurred \
— i.e., if the pre-2024 trend had continued uninterrupted. \
The shaded gap between the two lines is the estimated impact of the shock.

**Method: Segmented regression (Interrupted Time Series)**. \
OLS model with:
- β₂ (level change): immediate step at the treatment month
- β₃ (slope change): cumulative monthly drift after treatment
- Controls: monthly seasonality, COVID-19, long-run trend

**Standard errors**: OLS for Bus (no autocorrelation detected); \
Newey-West HAC-12 for Subway and Train (residual autocorrelation detected).

**Important limitation**: β₂ and β₃ cannot separate the fare price effect from \
the real-income collapse caused by the December 2023 devaluation (+118%). \
The implied elasticity is an upper bound on the pure price elasticity of demand.""",
        "rs_its_finding":     "**The 2024 fare shock produced gradual erosion, not an abrupt drop.** "
                              "No mode shows a statistically significant level change at January 2024 — "
                              "ridership held on impact. However, **Bus** accumulates a trend decline of "
                              "−1.6M trips/month (p=0.018) and **Subway** of −0.29M trips/month (p<0.001) "
                              "from the treatment date onward. **Train** shows no significant effect in either "
                              "dimension, consistent with a more captive demand base with fewer alternatives.",
        "rs_its_actual":          "Actual ridership",
        "rs_its_cf":              "Expected trend (no shock)",
        "rs_its_gap":             "Gap",
        "rs_its_treatment":       "Shock onset (Jan 2024)",
        "rs_its_metric_lost":     "Trips below expectation",
        "rs_its_metric_lost_sub": "cumulative since Jan 2024",
        "rs_its_metric_now":      "Gap in the latest month",
        "rs_its_metric_now_sub":  "vs the trend without the shock",
        "rs_its_metric_drift":    "Post-shock trend",
        "rs_its_drift_falling":   "falling {n}M trips/month extra",
        "rs_its_drift_rising":    "growing {n}M trips/month extra",
        "rs_its_drift_flat":      "no significant trend change",
        "rs_its_note":            "⚠️ The gap between the actual and expected lines combines the fare hike effect "
                                  "with the impact of the December 2023 devaluation (+118%). "
                                  "It is not possible to separate the two with this data alone.",

        "rs_finding": "**The 2024 fare hike was national, but AMBA absorbed it more sharply.** "
                      "The January (+45%) and February (+66%) increases applied to nationally-operated lines. "
                      "Interior provinces were also affected, but absorbed the Compensation Fund cut more gradually and unevenly "
                      "— each province managed its own adjustment timeline. "
                      "AMBA also shows **lower seasonal amplitude** than the Interior, "
                      "possibly because its ridership is more dominated by formal commuting "
                      "(though this is a hypothesis the data alone cannot confirm).",
        "an_finding": "**Patterns are consistent across years.** Weekdays see between 60% and 80% "
                      "more trips than weekends. March and August are peak months; "
                      "January and July are troughs due to school holidays.",

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
        "fc_explainer": """**How to read this chart**

The projected values answer a specific question: \
*where is ridership heading if current fare levels, macroeconomic conditions, \
and service policies remain unchanged?* \
This is not a guarantee of what will happen — it is a baseline scenario that quantifies \
the system's inertia under the status quo.

**How the model works**

This uses **Prophet**, a time series model developed by Meta, trained on historical monthly ridership data. \
COLECTIVO is trained from 2013; SUBTE and TREN from 2016 \
(before those dates SUBE coverage was incomplete and the data is not comparable). \
The model learns three things separately:

- 📈 **Trend**: the general long-term direction (growth or decline).
- 📅 **Annual seasonality**: patterns that repeat each year \
(January always has fewer trips; March and August are peaks).
- 🎌 **Argentine public holidays**: national holidays are explicitly modelled \
as point-in-time demand drops.

Four **external regressors** are also included:

- 🦠 **COVID impact**: a binary variable marking the disruption period (Mar 2020 – Dec 2021), \
allowing the model to learn the collapse without absorbing it into the long-term trend.
- 💸 **Cumulative fare pressure**: the sum of all fare increases in effect up to each month \
(from the Macri-era hikes of 2016–2019 through the staged hikes of 2025–2026). \
Not a simple on/off switch — it reflects the accumulated magnitude of fare adjustments.
- 📉 **Macro shock**: a binary variable capturing the regime change triggered by the \
December 2023 devaluation (+118%) and subsidy cuts, independent of fare levels.
- 🔄 **Recovery momentum**: captures the decelerating post-COVID rebound \
(demand recovered quickly in 2022 then stabilised at a new equilibrium level).

**Structural changepoints** are fixed at the exact dates of each fare hike and macro event, \
rather than being discovered automatically — this improves model accuracy during high-volatility periods.

**What does the shaded band mean?** It shows the range where the model expects 80% of future values \
to fall. The band widens progressively with the forecast horizon: predicting next month is far more \
certain than predicting two years out, and the chart reflects that.

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