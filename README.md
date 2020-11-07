# voter-fraud-michigan
####/data/detroit_index.txt
Phocaean Dionysius's original list of old/dead voters

####/data/detroit_index_checked.csv
Checked against Michigan Voter Information Center website. Added BIRTH_MONTH column so people can check voter registration information without guessing the birth month

####/data/voted.csv
Filtered list of people whose absentee ballots were received.

## Verifying the lists
#### Manually
Visit https://mvic.sos.state.mi.us/Voter/Index, fill in the form with information from the list and submit the form

#### Using a script
Install dependencies
```
pip install -r requirements.txt
```
Run
```
python check_against_mvic.py --input ./data/detroit_index.txt --output ./data/detroit_index_checkeck.csv --workers 40
```
Or
```
python check_against_mvic_sync.py --input ./data/detroit_index.txt --output ./data/detroit_index_checkeck.csv
```
Use `-h` to check more options