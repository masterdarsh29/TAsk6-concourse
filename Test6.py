
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from sqlalchemy import create_engine
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Web scraping
url = 'https://screener.in/company/RELIANCE/consolidated/'
logging.info(f"Fetching data from URL: {url}")
webpage = requests.get(url)
soup = bs(webpage.text, 'html.parser')
data = soup.find('section', id="profit-loss")

def clean_data(value):
    if isinstance(value, str):
        value = value.replace("+", "").replace("%", "").replace(",", "").strip()
        if value.replace('.', '', 1).isdigit():
            try:
                return float(value)
            except ValueError:
                return None
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
    except Exception as e:
        print(f"Error: {e}")
    finally:
        engine.dispose()

if data is not None:
    tdata = data.find("table")
    if tdata is not None:
        table_data = []
        for row in tdata.find_all('tr'):
            row_data = []
            for cell in row.find_all(['th', 'td']):
                row_data.append(cell.text.strip())
            table_data.append(row_data)

        # Convert the scraped table data to a DataFrame
        df_table = pd.DataFrame(table_data)
        df_table.iloc[0, 0] = 'Narration'  # Rename 'Section' to 'Narration'
        df_table.columns = df_table.iloc[0]
        df_table = df_table.iloc[1:, :-2]

        # Transpose the DataFrame to have columns as periods and rows as metrics
        df_table = df_table.set_index('Narration').transpose()

        # Reset index after transpose and add an 'id' column
        df_table.reset_index(inplace=True)
        df_table.rename(columns={'index': 'Period'}, inplace=True)
        df_table['id'] = range(1, len(df_table) + 1)

        # Rearrange columns to put 'id' at the beginning
        columns = ['id'] + [col for col in df_table.columns if col != 'id']
        df_table = df_table[columns]

        # Clean and convert data in all columns except the first one
        for col in df_table.columns[1:]:
            df_table[col] = df_table[col].apply(clean_data)

        # Handle missing or inappropriate values
        df_table = df_table.fillna(0)

        # Load data to Postgres
        db_host = "192.168.3.43"
        db_name = "Task6"
        db_user = "Darshan"
        db_password = "Darshan123"
        db_port = "5432"
        save_to_postgres(df_table, 'profit_loss_data', db_name, db_user, db_password, db_host, db_port)

        # Use the existing PostgreSQL connection
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # List the current columns in the table to verify names
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'profit_loss_data';
        """)
        columns = cursor.fetchall()
        logging.info("Columns in 'profit_loss_data' table:")
        for column in columns:
            print(column)

        # Rename columns one by one with error handling
        rename_queries = [
            """ALTER TABLE profit_loss_data RENAME COLUMN "Sales +" TO sales;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "0" TO month;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Expenses +" TO expenses;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Operating Profit" TO operating_profit;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "OPM %" TO operating_profit_margin;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Other Income +" TO other_income;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Interest" TO interest;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Depreciation" TO depreciation;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Profit before tax" TO profit_before_tax;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Tax %" TO tax_rate;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Net Profit +" TO net_profit;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "EPS in Rs" TO earnings_per_share;""",
            """ALTER TABLE profit_loss_data RENAME COLUMN "Dividend Payout %" TO dividend_payout_ratio;"""
        ]
 
        for query in rename_queries:
            try:
                cursor.execute(query)
                connection.commit()
                logging.info(f"Successfully executed query: {query}")
            except Exception as e:
                logging.error(f"Error with query: {query}\n{e}")
 
        # Close cursor and connection
        cursor.close()
        connection.close()
        logging.info("Data transformed and connections closed")
 
else:
    logging.error("No data found at the given URL or no Profit-Loss section available")
