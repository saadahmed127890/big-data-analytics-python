
import pandas as pd
import streamlit as st
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Topic8 Dashboard",layout="wide")

DATA_DIR=Path("data/serving")

kpi=pd.read_parquet(DATA_DIR/"kpi_daily.parquet")

st.title("Retail Clickstream Dashboard")

kpi["event_date"]=pd.to_datetime(kpi["event_date"])

st.metric("Total Sessions",int(kpi["sessions"].sum()))
st.metric("Total Revenue",float(kpi["revenue"].sum()))

fig=px.line(kpi,x="event_date",y="revenue",markers=True)
st.plotly_chart(fig,use_container_width=True)

st.dataframe(kpi)
