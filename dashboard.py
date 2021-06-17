import streamlit as st
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns


def main():

    df1 = load_data1()
    df2 = load_data2()

    page = st.sidebar.selectbox("Choose a page", ['Homepage', 'Callable Bull/Bear Contracts', 'South Bound'])

    if page == 'Homepage':
        st.title('Heng Seng Index')
        df_hsi = df2[['hsi']]
        chart_data = st.dataframe(df_hsi)
        st.line_chart(chart_data)
        st.dataframe(df_hsi)
        

    elif page == 'Callable Bull/Bear Contracts':
        st.title('Comparison Between Bull and Bear Contracts')
        if st.checkbox('Show column descriptions'):
            st.dataframe(df1.describe())
        
        st.markdown('### Analysing')
        df_cbbc = df1[['Bear', 'Bull']]
        chart_data = st.dataframe(df_cbbc)
        st.line_chart(chart_data)
        st.dataframe(df_cbbc)
        
    else:
        st.title('South Bound')
        df_sthbd = df2[['net_southbound_mean', 'hsi']]
        chart_data = st.dataframe(df_sthbd)
        st.line_chart(chart_data)
        st.dataframe(df_sthbd)

@st.cache
def load_data1():
    return pd.read_csv('Raw_data.csv')

def load_data2():
    return pd.read_csv('sthbd&hsi.csv')


if __name__ == '__main__':
    main()
