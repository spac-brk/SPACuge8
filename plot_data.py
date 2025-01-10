import pandas as pd
import pyodbc
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.dates import AutoDateLocator, DateFormatter

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', None)
matplotlib.use('TkAgg')  # Use a GUI backend for rendering

# MySQL connection setup
conn = pyodbc.connect('DRIVER={MySQL ODBC 8.0 ANSI Driver};SERVER=localhost;DATABASE=weather;UID=brk;PWD=12345678')
cursor = conn.cursor()


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
    cursor = conn.cursor()
    cursor.execute(query)
    cols = [x[0] for x in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame.from_records(rows, columns=cols)


def main():
    path = 'data/Weather'
    # Read from database
    conn_read = odbc_init('weather')
    weather = df_from_sql(conn_read, "SELECT * FROM weather_data;")
    elmaps = df_from_sql(conn_read, "SELECT * FROM elmaps_data;")
    conn_read.close()

    # Adjust data
    time_resolution = 'month'

    wind_speed_tr = weather.loc[(weather['timeResolution'] == time_resolution) &
                                (weather['parameterId'] == 'mean_wind_speed'), ['from', 'value']]
    wind_speed_tr.columns = ['time', 'value']
    wind_speed_tr['time'] = pd.to_datetime(wind_speed_tr['time'], errors='coerce')
    wind_speed_tr.dropna(subset=['time'], inplace=True)
    print('wind_speed:')
    print(wind_speed_tr)

    cloud_cover_tr = weather.loc[(weather['timeResolution'] == time_resolution) &
                                 (weather['parameterId'] == 'mean_cloud_cover'), ['from', 'value']]
    cloud_cover_tr.columns = ['time', 'value']
    cloud_cover_tr['time'] = pd.to_datetime(cloud_cover_tr['time'], errors='coerce')
    cloud_cover_tr.dropna(subset=['time'], inplace=True)
    print('cloud_cover:')
    print(cloud_cover_tr)

    elmaps_tr = elmaps.loc[elmaps['time_resolution'] == time_resolution, ['datetime_utc', 'carbon_intensity_direct']]
    elmaps_tr.columns = ['time', 'value']
    elmaps_tr['time'] = pd.to_datetime(elmaps_tr['time'], errors='coerce')
    elmaps_tr.dropna(subset=['time'], inplace=True)
    print('elmaps:')
    print(elmaps_tr)

    # Plot
    fig, ax = plt.subplots()

    ax.plot(wind_speed_tr['time'], wind_speed_tr['value'], 'x', markeredgewidth=2, label='Wind Speed')
    ax.plot(elmaps_tr['time'], elmaps_tr['value'], linewidth=2.0, label='Carbon Intensity')
    ax.plot(cloud_cover_tr['time'], cloud_cover_tr['value'], 'o-', linewidth=2, label='Cloud Cover')

    # Configure axis labels and title
    ax.set_xlabel("Time")
    ax.set_ylabel("Value")
    ax.set_title("Weather and Energy Data Over Time")
    ax.legend()

    # Customize datetime x-axis
    locator = AutoDateLocator()  # Automatically adjust ticks based on data range
    formatter = DateFormatter('%Y-%m-%d')  # Customize the format to YYYY-MM-DD
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)

    # Rotate and align x-axis labels for better readability
    plt.gcf().autofmt_xdate()

    # plt.savefig(path + 'output_graph.png')  # Save the graph to a file for inspection
    plt.show()


if __name__ == '__main__':
    main()
