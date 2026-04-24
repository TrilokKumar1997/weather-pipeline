# Global Weather Pipeline

A real-time data engineering pipeline that fetches live weather data
for 15 global cities every hour and stores it in PostgreSQL.

**Live demo:** YOUR_STREAMLIT_URL

## Architecture
## Cities tracked
New York · London · Tokyo · Paris · Dubai · Sydney · Toronto ·
Singapore · Berlin · Mumbai · Los Angeles · Chicago · Amsterdam ·
Seoul · Barcelona

## Features
- Real-time weather data fetched every hour via scheduler
- PostgreSQL database storing historical weather records
- Interactive dashboard with temperature trends and comparisons
- Weather condition classification from WMO weather codes

## Tech stack
- Python · pandas · SQLAlchemy · PostgreSQL · Streamlit · Plotly

## Data source
Open-Meteo API — free, no API key required

## How to run locally
git clone https://github.com/TrilokKumar1997/weather-pipeline.git
cd weather-pipeline
pip install -r requirements.txt

# Set up PostgreSQL and create weatherdb database
# Create .env file with your database credentials

python etl_pipeline.py    # run pipeline once
streamlit run app.py      # launch dashboard
python scheduler.py       # run hourly scheduler

## Author
Trilok Kumar — Data Science MS, University of New Haven
