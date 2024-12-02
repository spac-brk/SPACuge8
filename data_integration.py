import pyodbc
import pandas as pd
import requests
import json

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def rest_query(url, **kwargs):
    response_ = requests.get(url, params=kwargs)
    response_.raise_for_status()
    return response_


def odbc_init(db):
    # Specifying the ODBC driver, server name, database, etc. directly
    cnxn = pyodbc.connect('DRIVER={MySQL ODBC 8.0 ANSI Driver};SERVER=localhost;DATABASE='
                          + db + ';UID=brk;PWD=12345678;Charset=utf8')

    # Encoding and decoding
    cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='utf8')
    cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf8')
    cnxn.setencoding(encoding='utf8')
    return cnxn


# Get query as DataFrame
def df_from_sql(conn, query):
    cursor_ = conn.cursor()
    cursor_.execute(query)
    cols = [x[0] for x in cursor_.description]
    rows = cursor_.fetchall()
    return pd.DataFrame.from_records(rows, columns=cols)


def create_table_from_df(df: pd.DataFrame, connection: pyodbc.Connection, table_name: str):
    cursor = connection.cursor()

    # Dynamically create the SQL CREATE TABLE statement
    column_definitions = []
    for col_name, dtype in zip(df.columns, df.dtypes):
        # Map pandas data types to MySQL data types
        if pd.api.types.is_bool_dtype(dtype):
            sql_type = "BOOL"
        elif pd.api.types.is_integer_dtype(dtype):
            sql_type = "INT"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "DOUBLE"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_type = "DATETIME"
        else:
            sql_type = "VARCHAR(255)"
        if column_definitions == []:
            column_definitions.append(f"`{col_name}` {sql_type} NOT NULL")
            primary_key = f"`{col_name}`"
        else:
            column_definitions.append(f"`{col_name}` {sql_type}")

    column_definitions.append(f"PRIMARY KEY ({primary_key})")
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        {', '.join(column_definitions)}
    );
    """

    # Execute the CREATE TABLE statement
    try:
        cursor.execute(create_table_sql)
        connection.commit()
        print(f"Table '{table_name}' checked/created successfully.")
    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")


def list_foreign_keys(connection: pyodbc.Connection, write_db: str):
    cursor = connection.cursor()
    list_foreign_keys_sql = f"""
    """

    # Execute the statement to list foreign keys
    try:
        cursor.execute(list_foreign_keys_sql)
        cols = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        print(f"List of foreign keys read successfully.")
        return pd.DataFrame.from_records(rows, columns=cols)
    except Exception as e:
        print(f"Error reading list of foreign keys: {e}")


def add_foreign_key(connection: pyodbc.Connection, table_name: str, column_name: str,
                                                   foreign_table_name: str, foreign_column_name: str, fk_list):
    if f'fk_{table_name}_{column_name}' in fk_list:
        print(f'Foreign key `fk_{table_name}_{column_name}` already exists.')
        return
    cursor = connection.cursor()
    add_foreign_key_sql = f"""
    ALTER TABLE `{table_name}` ADD CONSTRAINT `fk_{table_name}_{column_name}`
    FOREIGN KEY (`{column_name}`) REFERENCES `{foreign_table_name}`(`{foreign_column_name}`);
    """

    # Execute the statement to add foreign key
    try:
        cursor.execute(add_foreign_key_sql)
        connection.commit()
        print(f"Foreign key created successfully on table {table_name}.")
    except Exception as e:
        print(f"Error creating foreign key on table '{table_name}': {e}")


def write_df_to_sql(df: pd.DataFrame, connection: pyodbc.Connection, table_name: str):
    cursor = connection.cursor()

    # Generate column names for SQL insert
    columns = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    # Write rows to the database
    try:
        for _, row in df.iterrows():
            cursor.execute(insert_sql, tuple(row))
        connection.commit()
        print(f"Data successfully written to {table_name}.")
    except Exception as e:
        connection.rollback()
        print(f"Error: {e}")
    finally:
        cursor.close()


# Figuring out how to do this...
def test():
    conn_read = odbc_init('world')
    sql_df = df_from_sql(conn_read, 'SELECT DISTINCT Continent FROM country')
    conn_read.close()
    csv_df = pd.read_csv('my_csv.csv')
    response = rest_query('https://fakerapi.it/api/v2/persons',
                          _quantity='10',
                          _locale="en_US",
                          _seed='12345678',
                          _gender='male',
                          _birthday_start='1951-01-01',
                          _birthday_end='2000-12-31')
    json_data = response.json()
    data = json_data['data']
    restapi_df = pd.DataFrame(data)
    restapi_df = restapi_df.drop('address', axis=1)
    print(sql_df)
    print(csv_df)
    print(restapi_df)

    conn_write = odbc_init('test')
    create_table_from_df(sql_df, conn_write, 'Continents')
    create_table_from_df(csv_df, conn_write, 'Associates')
    create_table_from_df(restapi_df, conn_write, 'Persons')
    write_df_to_sql(sql_df, conn_write, 'Continents')
    write_df_to_sql(csv_df, conn_write, 'Associates')
    write_df_to_sql(restapi_df, conn_write, 'Persons')
    conn_write.close()


def main():
    # test()

    # Using https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets/
    path = './data/finance/'

    # Read and adjust users_data
    users_data = pd.read_csv(path + 'users_data.csv', dtype='str')
    users_data[['per_capita_income', 'yearly_income', 'total_debt']] = (
        users_data[['per_capita_income', 'yearly_income', 'total_debt']].replace(r'\$', '', regex=True))
    users_data = users_data.rename(columns={'id':'client_id'})
    users_data = users_data.astype({'client_id': 'Int64',
                                    'current_age': 'Int64',
                                    'retirement_age': 'Int64',
                                    'birth_year': 'Int64',
                                    'birth_month': 'Int64',
                                    'latitude': 'Float64',
                                    'longitude': 'Float64',
                                    'per_capita_income': 'Float64',
                                    'yearly_income': 'Float64',
                                    'total_debt': 'Float64',
                                    'credit_score': 'Int64',
                                    'num_credit_cards': 'Int64'})

    # Read and adjust cards_data
    cards_data = pd.read_csv(path + 'cards_data.csv', dtype='str')
    cards_data[['credit_limit']] = cards_data[['credit_limit']].replace(r'\$', '', regex=True)
    cards_data['has_chip'] = cards_data['has_chip'].map({'YES': True, 'NO': False}).astype(bool)
    cards_data['card_on_dark_web'] = cards_data['card_on_dark_web'].map({'Yes': True, 'No': False}).astype(bool)
    cards_data['expires_month'] = cards_data['expires'].str[:2]
    cards_data['expires_year'] = cards_data['expires'].str[3:]
    cards_data = cards_data.drop('expires', axis=1)
    cards_data['acct_open_month'] = cards_data['acct_open_date'].str[:2]
    cards_data['acct_open_year'] = cards_data['acct_open_date'].str[3:]
    cards_data = cards_data.drop('acct_open_date', axis=1)
    cards_data = cards_data.rename(columns={'id':'card_id'})
    cards_data = cards_data.astype({'card_id': 'Int64',
                                    'client_id': 'Int64',
                                    'num_cards_issued': 'Int64',
                                    'credit_limit': 'Float64',
                                    'year_pin_last_changed': 'Int64',
                                    'expires_month': 'Int64',
                                    'expires_year': 'Int64',
                                    'acct_open_month': 'Int64',
                                    'acct_open_year': 'Int64'})

    # Read and adjust mcc_codes
    with open(path + 'mcc_codes.json') as f:
        mcc_codes_j = json.load(f)
    mcc_codes = pd.json_normalize(mcc_codes_j).transpose().reset_index()
    mcc_codes.columns = ['mcc', 'name']
    mcc_codes = mcc_codes.astype({'mcc': 'Int64'})

    # Read and adjust transactions_data
    transactions_data = pd.read_csv(path + 'transactions_data.csv', dtype='str')
    transactions_data['date'] = pd.to_datetime(transactions_data['date'])
    transactions_data[['amount']] = transactions_data[['amount']].replace(r'\$', '', regex=True)
    transactions_data['zip'] = transactions_data['zip'].str.replace('.0','').str.zfill(5)
    transactions_data = transactions_data.rename(columns={'id':'transaction_id'})
    transactions_data = transactions_data.astype({'transaction_id': 'Int64',
                                                  'client_id': 'Int64',
                                                  'card_id': 'Int64',
                                                  'amount': 'Float64',
                                                  'merchant_id': 'Int64',
                                                  'mcc': 'Int64'})

    # Read and adjust train_fraud_labels
    with open(path + 'train_fraud_labels.json') as f:
        train_fraud_labels = json.load(f)
    train_fraud_labels = pd.json_normalize(train_fraud_labels['target']).transpose().reset_index()
    train_fraud_labels.columns = ['transaction_id', 'isFraud']
    train_fraud_labels['isFraud'] = train_fraud_labels['isFraud'].map({'Yes': True, 'No': False}).astype(bool)
    train_fraud_labels = train_fraud_labels.astype({'transaction_id': 'Int64'})

    # Initialize connection for writing to database
    write_db = 'datamerge'
    conn_write = odbc_init(write_db)

    # Create tables
    create_table_from_df(users_data, conn_write, 'users_data')
    create_table_from_df(cards_data, conn_write, 'cards_data')
    create_table_from_df(mcc_codes, conn_write, 'mcc_codes')
    create_table_from_df(transactions_data, conn_write, 'transactions_data')
    create_table_from_df(train_fraud_labels, conn_write, 'train_fraud_labels')

    # Add foreign keys if not already added
    conn_is = odbc_init('INFORMATION_SCHEMA')
    all_foreign_keys = df_from_sql(conn_is,
        f"SELECT CONSTRAINT_NAME FROM KEY_COLUMN_USAGE WHERE REFERENCED_TABLE_SCHEMA = '{write_db}'")
    all_foreign_keys = all_foreign_keys['CONSTRAINT_NAME'].tolist()
    conn_is.close()

    add_foreign_key(conn_write,'cards_data','client_id',
                    'users_data', 'client_id', all_foreign_keys)
    add_foreign_key(conn_write,'transactions_data','client_id',
                    'users_data', 'client_id', all_foreign_keys)
    add_foreign_key(conn_write,'transactions_data','card_id',
                    'cards_data', 'card_id', all_foreign_keys)
    add_foreign_key(conn_write,'transactions_data','mcc',
                    'mcc_codes', 'mcc', all_foreign_keys)
    add_foreign_key(conn_write,'train_fraud_labels','transaction_id',
                    'transactions_data', 'transaction_id', all_foreign_keys)

    # Write data to database
    write_df_to_sql(users_data, conn_write, 'users_data')
    write_df_to_sql(cards_data, conn_write, 'cards_data')
    write_df_to_sql(mcc_codes, conn_write, 'mcc_codes')
    write_df_to_sql(transactions_data, conn_write, 'transactions_data')
    write_df_to_sql(train_fraud_labels, conn_write, 'train_fraud_labels')

    # Close connection
    conn_write.close()


if __name__ == '__main__':
    main()

# ***  dmiapi.govcloud.dk  ***
# dmi_country = rest_query('https://dmigw.govcloud.dk/v2/climateData/bulk/countryValue/',
#                          api-key='938f6692-328f-480a-955e-448334472b63')
#
# ***  docs.electricitymaps.com  ***  (api-portal in url instead?)
# el_maps = rest_query('https://api.electricitymaps.org/v3/zones',
#                      headers={'auth-token':'hkKjyiNyBLfg3'})
