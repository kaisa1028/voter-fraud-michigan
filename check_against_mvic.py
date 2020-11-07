import pandas
import pathlib
import aiohttp
from aiohttp.client_exceptions import ServerConnectionError, ServerDisconnectedError
import asyncio
import argparse
from bs4 import BeautifulSoup


def load_raw_data() -> pandas.DataFrame:
    """Load raw data from Phocaean Dionysius's list"""
    return pandas.read_csv(pathlib.Path(__file__).parent.joinpath('./data/detroit_index.txt').absolute())


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
    info = {}
    for field in ['Election date', 'Application received', 'Ballot sent', 'Ballot received']:
        try:
            idx = text.index(field)
            info[field] = text[idx + 1]
        except ValueError:
            info = None
            break
        except IndexError:
            info = None
            break
    return info


async def post_data(session, first, last, year, month, zip_code, proxy):
    retry_count = 0
    while retry_count <= 10:
        try:
            retry_count = retry_count + 1
            async with session.post('https://mvic.sos.state.mi.us/Voter/SearchByName', data={
                'FirstName': first,
                'LastName': last,
                'NameBirthMonth': str(month),
                'NameBirthYear': str(year),
                'ZipCode': str(zip_code),
                'Dln': '',
                'DlnBirthMonth': '0',
                'DlnBirthYear': '',
                'DpaID': '0',
                'Months': '',
                'VoterNotFound': 'false',
                'TransistionVoter': 'false'
            }, proxy=proxy) as res:
                return await res.text()
        except ServerDisconnectedError as e:
            if retry_count >= 10:
                raise e
        except ServerConnectionError as e:
            if retry_count >= 10:
                raise e
    return ''


async def check_person(session, dataframe, idx, proxy):
    month = 0
    registered = False
    absentee = False
    info = None
    row = dataframe.loc[idx]
    # raw data don't have birth month, I have to guess the birth month
    for i in range(1, 13):
        html = await post_data(session, row['FIRST_NAME'], row['LAST_NAME'], row['YEAR_OF_BIRTH'], i,
                               row['ZIP_CODE'],
                               proxy)
        if is_registered(html):
            registered = True
            month = i
            if has_absentee_ballot(html):
                absentee = True
                info = absentee_ballot_info(html)
            return idx, month, registered, absentee, info
    return idx, month, registered, absentee, info


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check voter registration against Michigan Voter Information Center')
    parser.add_argument('--proxy', help='set http proxy')
    parser.add_argument('--connections', help='limit the number of concurrent connections', type=int, default=4)
    args = parser.parse_args()

    df = load_raw_data()


    def save_csv():
        df.to_csv('./data/detroit_index_checked.csv', index=False)
        df_voted = df.loc[df['ABSENTEE']]
        df_voted.to_csv('./data/detroit_index_voted.csv', index=False)


    async def do():
        count_total = len(df.index)
        count_checked = 0
        count_registered = 0
        count_voted = 0
        try:
            async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=False, limit=args.connections)) as session:
                tasks = [asyncio.create_task(check_person(session, df, idx, args.proxy)) for idx in df.index]
                for co in asyncio.as_completed(tasks):
                    try:
                        idx, month, registered, absentee, info = await co

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
                    except asyncio.TimeoutError:
                        pass
                    print('Total: ', count_total, ' / ', 'Checked: ', count_checked, ' / ', 'Registered: ',
                          count_registered, ' / ', 'Voted: ', count_voted)
                    if count_checked % 10 == 0:
                        save_csv()
        except ServerDisconnectedError:
            print('Server disconnected')
        except ServerConnectionError:
            print('Error connecting to server')
        finally:
            save_csv()


    asyncio.run(do())
