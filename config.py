import os
import streamlit as st

def get_db_config():
    try:
        return {
            "host": st.secrets["DB_HOST"],
            "port": st.secrets["DB_PORT"],
            "dbname": st.secrets["DB_NAME"],
            "user": st.secrets["DB_USER"],
            "password": st.secrets["DB_PASSWORD"]
        }
    except:
        from dotenv import load_dotenv
        load_dotenv()
        return {
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD")
        }
