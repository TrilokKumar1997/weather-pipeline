# Global Weather Pipeline

A real-time data engineering pipeline that fetches live weather data
for 15 global cities every hour and stores it in PostgreSQL.

**Live demo:** https://weather-pipeline-ibfdwfcrrhsgqxbwtdjpzu.streamlit.app/

## Architecture
Open-Meteo API → Python ETL → PostgreSQL (Supabase) → Streamlit Dashboard

## Automated scheduling
GitHub Actions runs the pipeline every hour automatically

## Cities tracked
New York · London · Tokyo · Paris · Dubai · Sydney · Toronto ·
Singapore · Berlin · Mumbai · Los Angeles · Chicago · Amsterdam ·
Seoul · Barcelona

## Tech stack
- Python · pandas · SQLAlchemy · PostgreSQL · Streamlit · Plotly
- Supabase (cloud PostgreSQL)
- GitHub Actions (hourly scheduler)
- Open-Meteo API (free weather data)

## How to run locally
git clone https://github.com/TrilokKumar1997/weather-pipeline.git
cd weather-pipeline
pip install -r requirements.txt
python etl_pipeline.py
streamlit run app.py

## Author
Trilok Kumar — Data Science MS, University of New Haven
