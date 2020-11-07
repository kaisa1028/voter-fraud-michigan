import requests
import pandas
from bs4 import BeautifulSoup
import argparse

VERBOSE = False


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
    res = requests.post('https://mvic.sos.state.mi.us/Voter/SearchByName', data={
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
    })
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
    parser.add_argument('--skip', help='skip first x records', type=int, default=0)
    parser.add_argument('--verbose', help='display more info', type=bool, default=False)
    args = parser.parse_args()

    df = load_raw_data(args.input or './data/detroit_index.txt')
    out_file = args.output or './data/detroit_index_checked.txt'
    VERBOSE = args.verbose

    count_total = len(df.index)
    count_checked = 0
    count_registered = 0
    count_voted = 0

    for idx, row in df[args.skip:].iterrows():
        month, registered, absentee, info = check_person(row['FIRST_NAME'], row['LAST_NAME'], row['YEAR_OF_BIRTH'],
                                                         row['ZIP_CODE'])
        df.loc[idx, 'BIRTH_MONTH'] = str(month)
        df.loc[idx, 'REGISTERED'] = registered
        df.loc[idx, 'ABSENTEE'] = absentee
        df.loc[idx, 'ELECTION_DATE'] = info['Election date'] if info is not None else ''
        df.loc[idx, 'APPLICATION_RECEIVED'] = info['Application received'] if info is not None else ''
        df.loc[idx, 'BALLOT_SENT'] = info['Ballot sent'] if info is not None else ''
        df.loc[idx, 'BALLOT_RECEIVED'] = info['Ballot received'] if info is not None else ''

        count_checked = count_checked + 1
        if registered:
            count_registered = count_registered + 1
        if absentee:
            count_voted = count_voted + 1
        print('Total: ', count_total, ' / ', 'Checked: ', count_checked, ' / ', 'Registered: ',
              count_registered, ' / ', 'Voted: ', count_voted)

        if count_checked % 50 == 0:
            df.to_csv(out_file, index=False)

    df_voted = df.loc[df['ABSENTEE']]
    df_voted.to_csv('./data/voted.csv', index=False)
