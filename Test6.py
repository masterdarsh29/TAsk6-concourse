

import requests
import pandas as pd
from bs4 import BeautifulSoup
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import argparse
import numpy as np
 
def login_to_screener(email, password):
    session = requests.Session()
    login_url = "https://www.screener.in/login/?"
    login_page = session.get(login_url)
    soup = BeautifulSoup(login_page.content, 'html.parser')
    csrf_token = soup.find('input', {'name': 'csrfmiddlewaretoken'})['value']
    login_payload = {
        'username': email,
        'password': password,
        'csrfmiddlewaretoken': csrf_token
    }
    headers = {
        'Referer': login_url,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
    }
    response = session.post(login_url, data=login_payload, headers=headers)
    if response.url == "https://www.screener.in/dash/":
        print("Login successful")
        return session
    else:
        print("Login failed")
        return None

def scrape_reliance_data(session):
    search_url = "https://www.screener.in/company/RELIANCE/consolidated/"
    search_response = session.get(search_url)
    if search_response.status_code == 200:
        print("Reliance data retrieved successfully")
        soup = BeautifulSoup(search_response.content, 'html.parser')
        table1 = soup.find('section', {'id': 'profit-loss'})
        table = table1.find('table')
        headers = [th.text.strip() or f'Column_{i}' for i, th in enumerate(table.find_all('th'))]
        rows = table.find_all('tr')
        print("Extracted Headers:", headers)
        row_data = []
        for row in rows[1:]:
            cols = row.find_all('td')
            cols = [col.text.strip() for col in cols]
            if len(cols) == len(headers):
                row_data.append(cols)
            else:
                print(f"Row data length mismatch: {cols}")
        df = pd.DataFrame(row_data, columns=headers)
        if not df.empty:
            df.columns = ['Year'] + df.columns[1:].tolist()
            df = df.rename(columns={'Narration': 'Year', 'Year': 'year'})
            # # Remove the narration row header
            # df = df.drop(df[df['year'] == 'Narration'].index, errors='ignore')
            # Remove the TTM row
            df = df.drop(df[df['year'] == 'TTM'].index, errors='ignore')
        df_transposed = df.set_index('year').T
        df_transposed = df_transposed.replace('', 0)  # Replace empty strings with 0
        df_transposed = df_transposed.replace(np.nan, 0)  # Replace null values with 0
        print(df_transposed.head())
        return df_transposed
    else:
        print("Failed to retrieve Reliance data")
        return None
 
def clean_data(value):
    if isinstance(value, str):
        value = value.replace("+", "").replace("%", "").replace(",", "").replace(" ", "").strip()
        if value.replace('.', '', 1).isdigit():
            try:
                return float(value)
            except ValueError:
                return 0.0  # Return 0.0 for non-numeric values
        return value
    return value

def save_to_postgres(df, table_name, db, user, password, host, port):
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    try:
        # Clean and convert data in all columns except the first one
        for col in df.columns[1:]:
            df[col] = df[col].apply(clean_data)
        # Handle missing or inappropriate values
        df = df.fillna(0)
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print("Data saved to Postgres")
    except SQLAlchemyError as e:
        print(f"Error: {e}")
    finally:
        engine.dispose()
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--email", default="darshan.patil@godigitaltc.com")
    parser.add_argument("--password", default="Darshan123")
    parser.add_argument("--table_name", default="profit_loss_data")
    parser.add_argument("--db", default="Task6")
    parser.add_argument("--user", default="Darshan")
    parser.add_argument("--pw", default="Darshan123")
    parser.add_argument("--host", default="192.168.3.45")
    parser.add_argument("--port", default="5432")
    args = parser.parse_args()
    session = login_to_screener(args.email, args.password)
    if session:
        df = scrape_reliance_data(session)
        if df is not None:
            save_to_postgres(df, args.table_name, args.db, args.user, args.pw, args.host, args.port)

