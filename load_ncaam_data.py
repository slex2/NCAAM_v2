#!/home/slex/ncaam/ncaam-env/bin/python

import pandas as pd 
import numpy as np 
import os
from sqlalchemy import create_engine

import os
path = '/home/slex/ncaam/csv_files/game_id_pbp/'
arr = os.listdir(path)

output_df = pd.DataFrame() 

for i in arr[:-1] : 
    df = pd.read_feather(f'{path}{i}')
    output_df = (df.copy() if output_df.empty else pd.concat([df, output_df]))

 
engine = create_engine('postgresql+psycopg2://postgres:packer123@192.168.1.22/ncaam')

conn = engine.connect()
output_df.to_sql('ncaam_pbp', con=conn, if_exists='append')