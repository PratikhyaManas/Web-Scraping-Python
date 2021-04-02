# Libraries To Import
import requests
import pandas as pd
import csv

# # API Parameters
# mmsi = "269057489"
# API_KEY = "4ac31961578b87a6b40114fba326fb966087365a"
# period = "hourly"
# fromdate = '2021-03-14'
# todate = '2021-03-16'
# protocol = "csv"
# response = requests.get(f"https://services.marinetraffic.com/api/exportvesseltrack/{API_KEY}/v:3/period:{period}/fromdate:{fromdate}/todate:{todate}/mmsi:{mmsi}/protocol:{protocol}")
# data = response.text
# destname = 'exportvesseltrack_123.csv'
# with open(destname, 'w') as wf:
#     wf.write(data)

# Path of the CSV Data
vessel = './exportvesseltrack.csv'
pos_data = './position_data.csv'
engi = './ship_engines.csv'
owner = './ships_per_owner.csv' 

# Reading all the CSV Data and storing in Dataframe
df_vessel =  pd.read_csv(vessel,sep=',')
df_pos_data =  pd.read_csv(pos_data,sep=',')
df_engi =  pd.read_csv(engi,sep=',')
df_owner =  pd.read_csv(owner,sep=',')

# Joining the API Vessel Data with Engine Data
api_c = df_vessel.merge(df_engi, how='left', left_on='MMSI', right_on='mmsi').drop(columns='mmsi')

# Joining the Position Data with the Combined API Data and Engine Data
pos_with_ship = api_c.merge(df_pos_data, how='left', left_on='ship_name', right_on='SHIP').drop(columns=['SHIP','TIMESTAMP','SPEED','LON','LAT'])

# # Add a column to the postion data 'OWNER' and update 
# # the 'OWNER' from owner data(name of the owners with the ship data) 

pos_with_ship['OWNER'] = 'NAN'
pos_with_ship_n = pos_with_ship
for i in range(len(df_owner.columns.values)):
    clmns = df_owner.columns.values[i]
    
    pdfr = df_owner['{}'.format(clmns)]
    for sh in pdfr:
        pos_with_ship_n.loc[pos_with_ship['ship_name']=='{}'.format(sh),'OWNER'] = clmns

# Storing the Final Combined Data in CSV
pos_with_ship_n.to_csv('./final_data_latest.csv',index=False, header=pos_with_ship_n.columns.values[1])

# Store the Data in SQLite
import csv, sqlite3
con = sqlite3.connect("vessels.tb") 
cur = con.cursor()
cur.execute("DROP TABLE VESSELS_TABLE_TEST;")
cur.execute("CREATE TABLE VESSELS_TABLE_TEST (MMSI, IMO, STATUS, SPEED, LON, LAT, COURSE, HEADING, TIMESTAMP, SHIP_ID,SHIP_NAME,ENGINE1_ID,ENGINE1_NAME,ENGINE2_ID,ENGINE2_NAME,ENGINE3_ID,ENGINE3_NAME,OWNER);") 

with open('./final_data_latest.csv','r') as fin: 
    dr = csv.DictReader(fin) 
    to_db = [(i['MMSI'], i[' IMO'],i[' STATUS'], i[' SPEED'],i[' LON'], i[' LAT'],i[' COURSE'], i[' HEADING'],i[' TIMESTAMP'], i[' SHIP_ID'],i['ship_name'], i['engine1_id'],i['engine1_name'], i['engine2_id'],i['engine2_name'], i['engine3_id'],i['engine3_name'], i['OWNER']) for i in dr]

cur.executemany("INSERT INTO VESSELS_TABLE_TEST (MMSI, IMO, STATUS, SPEED, LON, LAT, COURSE, HEADING, TIMESTAMP, SHIP_ID,SHIP_NAME,ENGINE1_ID,ENGINE1_NAME,ENGINE2_ID,ENGINE2_NAME,ENGINE3_ID,ENGINE3_NAME,OWNER) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", to_db)
con.commit()
con.close()