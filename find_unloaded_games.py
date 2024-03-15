#!/home/slex/ncaam/ncaam-env/bin/python

import pandas as pd 
import numpy as np
import cbbpy.mens_scraper as s
import os, re, itertools
from sqlalchemy import create_engine
from datetime import date, timedelta


engine = create_engine('postgresql+psycopg2://postgres:packer123@localhost/ncaam')
conn = engine.connect() 
try : 
    game_id = pd.read_sql('''
                        select distinct 
                            game_id
                        from 
                            ncaam_possession_vw
                        where 
                            possession_end_time = 0 
                        '''
                        ,con=conn)
    conn.close()
except Exception as e : 
    print(e)
    conn.close()



completed_ids = [str(i) for i in game_id['game_id'].values]

dateList = []
startDate = date(2023, 10, 1) 
endDate = date.today()

delta = endDate - startDate   # returns timedelta

for i in range(delta.days):
    dateList.append(startDate + timedelta(days=i))

gameIds = []
for i in dateList : 
    gameIds.append(s.get_game_ids(i))


merged = list(itertools.chain(*gameIds))
master_game_id = np.array(merged)


updated_game_ids = list(set(master_game_id) - set(completed_ids))

ax_date = str(np.max(dateList))
filename = f'/home/slex/ncaam/csv_files/game_ids_prod.csv'

np.savetxt(filename, updated_game_ids, delimiter=",",fmt='%s')