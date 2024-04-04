# main.py

import streamlit as st
import eda
import prediction

page = st.sidebar.selectbox('Pilih Halaman: ', ('EDA', 'Prediction'))
st.set_option('deprecation.showPyplotGlobalUse', False)

if page == 'EDA':
    eda.run()
else:
    prediction.run()