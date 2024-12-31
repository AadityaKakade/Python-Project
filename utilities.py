import os
import glob
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

TEMP_URL = 'https://results.eci.gov.in/ResultAcGenNov2024/candidateswise-xxxxx.htm'
MAIN_URL = 'https://results.eci.gov.in/ResultAcGenNov2024/partywiseresult-S13.htm'
HEADERS = {
    "authority": "results.eci.gov.in",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "max-age=0",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    "upgrade-insecure-requests": "1",
    "cookie": "RT=z=1&dm=results.eci.gov.in&si=f8d6d1ac-73eb-4c0d-9d5a-e46bb8e28d03&ss=m4kouhv0&sl=1&tt=53u&bcn=%2F%2F68794907.akstat.io%2F&ld=53z&ul=osf"
}
COL_NAMES = ['Constituency', 'CandidateName', 'Party', 'Result', 'Votes_Status']

PROJECT_DIR = 'C:\\Users\\91951\\PycharmProjects\\PythonProject2\\'
DOWNLOAD_DIR = 'C:\\Users\\91951\\PycharmProjects\\PythonProject2\\download_dir\\'
OUTPUT_DIR = 'C:\\Users\\91951\\PycharmProjects\\PythonProject2\\output_dir\\'

# Create an empty Dataframe
my_data_frame = pd.DataFrame()


def get_status(value):
    soup_1 = BeautifulSoup(str(value), 'html.parser')
    h5_element = soup_1.find('div')
    myname = h5_element.get_text()
    return myname


def get_candidate_name(value):
    soup_1 = BeautifulSoup(str(value), 'html.parser')
    h5_element = soup_1.find('h5')
    myname = h5_element.get_text()
    return myname


def get_party_name(value):
    soup_1 = BeautifulSoup(str(value), 'html.parser')
    h6_element = soup_1.find('h6')
    myname = h6_element.get_text()
    return myname


def get_votes(value):
    soup_1 = BeautifulSoup(str(value), 'html.parser')
    ele = soup_1.find('div')
    return str(ele.get_text())


def parse_votes(value):
    soup_1 = BeautifulSoup(str(value), 'html.parser')
    ele = soup_1.find('div')
    retu_val = get_votes(list(ele)[3])
    return retu_val


def download_and_save_data():
    current_dir = os.getcwd()
    os.chdir(current_dir + "\\download_dir")

    response = requests.get(MAIN_URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')
    option_tags = soup.select('select option')

    top_list = []

    for option in option_tags:
        mylist = []
        constituency_name = option.text
        constituency_name = constituency_name[:constituency_name.find("-")]
        constituency_code = option['value']
        mylist.append(constituency_name.strip())
        mylist.append(constituency_code.strip())
        top_list.append(mylist)

    del top_list[0]

    for items in top_list:
        constituency_name = items[0]
        constituency_code = items[1]
        temp_url_v1 = TEMP_URL.replace("xxxxx", constituency_code)
        print(f"Working on {constituency_name}")
        process_each_constituency(temp_url_v1, constituency_name)


def process_each_constituency(temp_url_v1, constituency_name):
    response = requests.get(temp_url_v1, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')
    html_content = soup.findAll('div', class_='cand-info')

    file_name = constituency_name + ".csv"

    with open(file_name, "w", encoding="utf-8") as f:
        for item_c in html_content:
            nested_div = item_c.findAll('div')
            votes = parse_votes(nested_div[0])
            result = get_status(nested_div[1])
            candidate = get_candidate_name(nested_div[3])
            party = get_party_name(nested_div[3])
            row = (f"{constituency_name}|{candidate}|{party}|{result}|{votes}\n")
            f.write(row)


def load_data_into_dataframe():
    pd.set_option('display.max_rows', 5000)
    data_frame_list = []
    os.chdir(os.getcwd() + "/download_dir")
    file_list = glob.glob("*.csv")
    for file in file_list:
        temp_data_frame = pd.DataFrame()
        temp_data_frame = pd.read_csv(file, delimiter="|", encoding="utf-8", names=COL_NAMES)
        data_frame_list.append(temp_data_frame)
    my_data_frame = pd.concat(data_frame_list, ignore_index=True)

    ser_1 = my_data_frame['Votes_Status'].str.split("(")
    votes = ser_1.apply(lambda x: x[0])
    my_data_frame['Votes'] = votes
    votes_margin = ser_1.apply(lambda x: x[1])
    my_data_frame['votes_margin'] = votes_margin
    my_data_frame['votes_margin'] = my_data_frame['votes_margin'].str.replace(" ", "")
    my_data_frame['votes_margin'] = my_data_frame['votes_margin'].str.replace(")", "")

    my_data_frame['Votes'] = my_data_frame['Votes'].astype(int)
    my_data_frame['votes_margin'] = my_data_frame['votes_margin'].astype(int)

    print(my_data_frame.head(5))
    return my_data_frame


def get_Winning_List(data_frame):
    filt_won = data_frame['Result'] == "won"
    winning_full_list = data_frame[filt_won]
    return winning_full_list


def get_party_wise_winning_count(data_frame):
    group_by_party = data_frame.groupby('Party')
    result = group_by_party['Party'].count().sort_values(ascending=False)
    df_out = result.reset_index(name='count')
    return df_out


def show_winning_parties(data_frame):
    pd.set_option('display.max_rows', 5000)
    pd.set_option('display.max_columns', 15)

    winning_full_list = get_Winning_List(data_frame)
    result = get_party_wise_winning_count(winning_full_list)
    print(result)


def save_data_to_excel_file(data_frame):
    os.chdir( OUTPUT_DIR )
    winning_full_list = get_Winning_List(data_frame)
    df_out = get_party_wise_winning_count(winning_full_list)
    df_out.to_csv('WinningList.csv', index=False)

def show_winning_parties_pie(data_frame):
    winning_full_list = get_Winning_List(data_frame)
    df_out = get_party_wise_winning_count(winning_full_list)
    plt.pie(df_out['count'], labels=df_out['Party'], autopct='%1.2f%%')
    plt.show()

def save_to_oracle(data_frame):
    engine = create_engine(
        "oracle+oracledb://analytics:password123@localhost:1521?service_name=FREEPDB1"
    )
    data_frame.to_sql("maha_elec_results_raw_data", engine, schema='analytics', if_exists="replace")
    print("Data Saved to Oracle Database")