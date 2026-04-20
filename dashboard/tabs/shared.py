"""
Shared helper functions for all dashboard tab modules.
These all read from st.session_state.lang so they must not be called at module
import time — only from within a Streamlit execution context.
"""

import streamlit as st

from dashboard.strings import STRINGS, MODE_LABELS
from dashboard.utils import (
    add_event_annotations as _add_event_annotations,
    add_fare_annotations as _add_fare_annotations,
)


def t(key: str) -> str:
    return STRINGS[st.session_state.lang].get(key, key)


def mode_label(mode: str) -> str:
    return MODE_LABELS[st.session_state.lang].get(mode, mode)


def event_label(ev: dict) -> str:
    return ev[f"label_{st.session_state.lang}"]


def explainer(key: str) -> None:
    label = "¿Cómo leer este gráfico?" if st.session_state.lang == "es" else "How to read this chart?"
    with st.expander("ℹ️ " + label):
        st.markdown(t(key))


def finding(key: str) -> None:
    st.info(t(key), icon="💡")


def add_event_annotations(fig, y_ref: float = 0, x_min=None, x_max=None):
    return _add_event_annotations(fig, lang=st.session_state.lang, x_min=x_min, x_max=x_max)


def add_fare_annotations(fig, y_ref: float = 0, scope_filter=None, x_min=None, x_max=None):
    return _add_fare_annotations(
        fig, lang=st.session_state.lang,
        scope_filter=scope_filter, x_min=x_min, x_max=x_max,
    )
