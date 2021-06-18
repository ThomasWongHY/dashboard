import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

def main():

    df1 = load_data1()      #get the data of CBBC
    df2 = load_data2()      #get the data of Southbound

    page = st.sidebar.selectbox("Choose a page", ['Heng Seng Index', 'Callable Bull/Bear Contracts', 'Southbound'])

    if page == 'Heng Seng Index':
        st.title('Heng Seng Index Graph')
        df_hsi = df2[['Date', 'hsi']]
        fig = px.line(df_hsi, x='Date', y='hsi', title='Time Series of Heng Seng Index')
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            )
        )
        st.write(fig)
        #st.dataframe(df_hsi)
        

    elif page == 'Callable Bull/Bear Contracts':
        st.title('Comparison Between Bull and Bear Contracts')
        st.markdown('### Analysis')
        df_cbbc = df1[['Trade Date', 'Bear', 'Bull']]
        fig = px.line(df_cbbc, x='Trade Date', y=df_cbbc.columns,
                      title='Time Series of Callable Bull/Bear Contracts')
        fig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            )
        )
        st.write(fig)
        #st.dataframe(df_cbbc)
        
    else:
        st.title('Southbound Capital vs HSI')
        df_sthbd = df2[['Date', 'hsi', 'net_southbound_mean']]
        subfig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig = px.line(df_sthbd, x='Date', y=df_sthbd.filter(items=['hsi']).columns)
        fig2 = px.line(df_sthbd, x='Date', y=df_sthbd.filter(items=['net_southbound_mean']).columns)
        
        fig2.update_traces(yaxis="y2")

        subfig.add_traces(fig.data + fig2.data)
        subfig.layout.xaxis.title="Date"
        subfig.update_yaxes(title_text="<b>Heng Seng Index</b>", secondary_y=False)
        subfig.update_yaxes(title_text="<b>Net Ssouthbound Mean</b> (HK$ million)", secondary_y=True)
        
        subfig.update_xaxes(
            rangeslider_visible=True,
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            )
        )
        
        subfig.for_each_trace(lambda t: t.update(line=dict(color=t.marker.color)))
        st.write(subfig)
        #st.dataframe(df_sthbd)

@st.cache
def load_data1():
    return pd.read_csv('Raw_data.csv')

def load_data2():
    return pd.read_csv('sthbd&hsi.csv')


if __name__ == '__main__':
    main()
