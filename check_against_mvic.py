import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas
from bs4 import BeautifulSoup
import argparse
import concurrent.futures
import threading

thread_local = threading.local()


def get_req_session():
    if not hasattr(thread_local, 'session'):
        retry_strategy = Retry(total=10, read=10, connect=10, backoff_factor=0.3,
                               method_whitelist=frozenset(['GET', 'POST']))
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        thread_local.session = session
    return thread_local.session


def load_raw_data(filename) -> pandas.DataFrame:
    """Load raw data from Phocaean Dionysius's list"""
    return pandas.read_csv(filename)


def is_registered(html: str):
    """check if this person is registered"""
    if html.find('Yes, you are registered!') == -1:
        return False
    else:
        return True


def has_absentee_ballot(html: str):
    """check if this person has requested absentee ballot"""
    if html.find('Your clerk has not recorded receiving your AV Application.') == -1:
        return True
    else:
        return False


def absentee_ballot_info(html: str):
    """get voting info"""
    soup = BeautifulSoup(html, 'lxml')
    x = soup.find(id='lblAbsenteeVoterInformation')
    text = [txt for txt in x.stripped_strings]
    voting_info = {}
    for field in ['Election date', 'Application received', 'Ballot sent', 'Ballot received']:
        try:
            i = text.index(field)
            voting_info[field] = text[i + 1]
        except ValueError:
            voting_info = None
            break
        except IndexError:
            voting_info = None
            break
    return voting_info


def post_data(first_name, last_name, birth_year, birth_month, zip_code):
    session = get_req_session()
    res = session.post('https://mvic.sos.state.mi.us/Voter/SearchByName', data={
        'FirstName': first_name,
        'LastName': last_name,
        'NameBirthMonth': str(birth_month),
        'NameBirthYear': str(birth_year),
        'ZipCode': str(zip_code),
        'Dln': '',
        'DlnBirthMonth': '0',
        'DlnBirthYear': '',
        'DpaID': '0',
        'Months': '',
        'VoterNotFound': 'false',
        'TransistionVoter': 'false'
    }, timeout=5)
    return res.text


def check_person(first, last, year, zip_code):
    birth_month = 0
    has_registered_to_vote = False
    has_requested_absentee_ballot = False
    voting_info = None
    for i in range(1, 13):
        html = post_data(first, last, year, i, zip_code)
        if is_registered(html):
            has_registered_to_vote = True
            birth_month = i
            if has_absentee_ballot(html):
                has_requested_absentee_ballot = True
                voting_info = absentee_ballot_info(html)
            return birth_month, has_registered_to_vote, has_requested_absentee_ballot, voting_info
    return birth_month, has_registered_to_vote, has_requested_absentee_ballot, voting_info


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check voter registration against Michigan Voter Information Center')
    parser.add_argument('--proxy', help='set http proxy')
    parser.add_argument('--input', help='input file')
    parser.add_argument('--output', help='output file')
    parser.add_argument('--skip', help='skip records that are already checked', type=bool, default=True)
    parser.add_argument('--workers', help='number of workers', type=int, default=5)
    args = parser.parse_args()

    df = load_raw_data(args.input or './data/detroit_index.txt')
    out_file = args.output or './data/detroit_index_checked.txt'

    if 'BIRTH_MONTH' not in df.columns:
        df['BIRTH_MONTH'] = int(0)
    if 'REGISTERED' not in df.columns:
        df['REGISTERED'] = False
    if 'ABSENTEE' not in df.columns:
        df['ABSENTEE'] = False

    count_total = len(df.index)
    count_checked = 0
    count_voted = 0

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            if args.skip:
                futures = {executor.submit(check_person, row['FIRST_NAME'], row['LAST_NAME'], row['YEAR_OF_BIRTH'],
                                           row['ZIP_CODE']): idx for idx, row in
                           df.loc[df['BIRTH_MONTH'] <= 0].iterrows()}
            else:
                futures = {executor.submit(check_person, row['FIRST_NAME'], row['LAST_NAME'], row['YEAR_OF_BIRTH'],
                                           row['ZIP_CODE']): idx for idx, row in df.iterrows()}
            for future in concurrent.futures.as_completed(futures):
                idx = futures[future]
                month, registered, absentee, info = future.result()
                df.loc[idx, 'BIRTH_MONTH'] = int(month)
                df.loc[idx, 'REGISTERED'] = registered
                df.loc[idx, 'ABSENTEE'] = absentee
                df.loc[idx, 'ELECTION_DATE'] = info['Election date'] if info is not None else ''
                df.loc[idx, 'APPLICATION_RECEIVED'] = info['Application received'] if info is not None else ''
                df.loc[idx, 'BALLOT_SENT'] = info['Ballot sent'] if info is not None else ''
                df.loc[idx, 'BALLOT_RECEIVED'] = info['Ballot received'] if info is not None else ''

                count_checked = len(df.loc[df['BIRTH_MONTH'] > 0])
                count_voted = len(df.loc[df['ABSENTEE']])
                print('Total: ', count_total, ' / ', 'Checked: ', count_checked, ' / ', 'Voted: ', count_voted)

                if count_checked % 5 == 0:
                    df.to_csv(out_file, index=False)
    except KeyboardInterrupt:
        df.to_csv(out_file, index=False)
    finally:
        df_voted = df.loc[df['ABSENTEE']]
        df_voted.to_csv('./data/voted.csv', index=False)
