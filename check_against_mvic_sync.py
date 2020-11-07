import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas
from bs4 import BeautifulSoup
import argparse

VERBOSE = False

retry_strategy = Retry(total=5, read=5, connect=5, backoff_factor=0.3, method_whitelist=frozenset(['GET', 'POST']))
adapter = HTTPAdapter(max_retries=retry_strategy)
http_client = requests.Session()
http_client.mount('https://', adapter)
http_client.mount('http://', adapter)


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
    res = http_client.post('https://mvic.sos.state.mi.us/Voter/SearchByName', data={
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
    if VERBOSE:
        print(f'Checking {first} {last}')
        print(f'Trying to find birth month')
    birth_month = 0
    has_registered_to_vote = False
    has_requested_absentee_ballot = False
    voting_info = None
    for i in range(1, 13):
        html = post_data(first, last, year, i, zip_code)
        if is_registered(html):
            has_registered_to_vote = True
            birth_month = i
            if VERBOSE:
                print(f'Birth month of {first} {last} is {birth_month}')
            if has_absentee_ballot(html):
                has_requested_absentee_ballot = True
                voting_info = absentee_ballot_info(html)
            return birth_month, has_registered_to_vote, has_requested_absentee_ballot, voting_info
        elif VERBOSE:
            print(f'Birth month of {first} {last} is not {i}')
    return birth_month, has_registered_to_vote, has_requested_absentee_ballot, voting_info


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check voter registration against Michigan Voter Information Center')
    parser.add_argument('--proxy', help='set http proxy')
    parser.add_argument('--input', help='input file')
    parser.add_argument('--output', help='output file')
    parser.add_argument('--skip', help='skip records that are already checked', type=bool, default=True)
    parser.add_argument('--verbose', help='display more info', type=bool, default=False)
    args = parser.parse_args()

    df = load_raw_data(args.input or './data/detroit_index.txt')
    out_file = args.output or './data/detroit_index_checked.txt'
    VERBOSE = args.verbose

    if 'BIRTH_MONTH' not in df.columns:
        df['BIRTH_MONTH'] = int(0)
    if 'REGISTERED' not in df.columns:
        df['REGISTERED'] = False
    if 'ABSENTEE' not in df.columns:
        df['ABSENTEE'] = False

    count_total = len(df.index)
    count_checked = 0
    count_voted = 0

    for idx, row in df.iterrows():
        if row['BIRTH_MONTH'] <= 0:
            month, registered, absentee, info = check_person(row['FIRST_NAME'], row['LAST_NAME'], row['YEAR_OF_BIRTH'],
                                                             row['ZIP_CODE'])
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

            if count_checked % 50 == 0:
                df.to_csv(out_file, index=False)

    df_voted = df.loc[df['ABSENTEE']]
    df_voted.to_csv('./data/voted.csv', index=False)
