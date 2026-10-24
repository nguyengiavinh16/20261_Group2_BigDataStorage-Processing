import os
import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title='Fraud Detection Monitor',layout='wide')
st.title('Real-time Financial Fraud Detection')
st.caption('Academic prototype — not a production financial decision system.')
api=os.getenv('API_URL','http://localhost:8000')
with st.sidebar:
    st.header('API status')
    if st.button('Check API'):
        try: st.success(requests.get(f'{api}/health',timeout=3).json())
        except Exception as e: st.error(str(e))
st.subheader('Test a transaction')
with st.form('prediction'):
    c1,c2,c3=st.columns(3)
    step=c1.number_input('step',min_value=0,value=1)
    typ=c2.selectbox('type',['PAYMENT','TRANSFER','CASH_OUT','CASH_IN','DEBIT'])
    amount=c3.number_input('amount',min_value=0.0,value=1000.0)
    name_orig=st.text_input('nameOrig','C123456789')
    old_org=st.number_input('oldbalanceOrg',min_value=0.0,value=2000.0)
    new_org=st.number_input('newbalanceOrig',min_value=0.0,value=1000.0)
    name_dest=st.text_input('nameDest','M987654321')
    old_dest=st.number_input('oldbalanceDest',min_value=0.0,value=0.0)
    new_dest=st.number_input('newbalanceDest',min_value=0.0,value=1000.0)
    flagged=st.selectbox('isFlaggedFraud',[0,1])
    submitted=st.form_submit_button('Score transaction')
if submitted:
    payload={'step':step,'type':typ,'amount':amount,'nameOrig':name_orig,'oldbalanceOrg':old_org,
      'newbalanceOrig':new_org,'nameDest':name_dest,'oldbalanceDest':old_dest,'newbalanceDest':new_dest,'isFlaggedFraud':flagged}
    try:
        response=requests.post(f'{api}/predict',json=payload,timeout=15); response.raise_for_status(); result=response.json()
        st.metric('Fraud probability',f"{result['fraud_probability']:.2%}")
        if result['is_fraud']: st.error('Flagged as potentially fraudulent')
        else: st.success('Below configured threshold')
        st.json(result)
    except Exception as e: st.error(f'Prediction request failed: {e}')
