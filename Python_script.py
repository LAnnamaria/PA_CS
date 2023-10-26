import pandas as pd
import datetime

# Define file paths
cdhdr_file = "CDHDR.csv"
cdpos_file = "CDPOS.csv"
ekko_file = "EKKO.csv"
ekpo_file = "EKPO.csv"

# Read CSV files into DataFrames
cdhdr_df = pd.read_csv(cdhdr_file)
cdpos_df = pd.read_csv(cdpos_file)
ekko_df = pd.read_csv(ekko_file)
ekpo_df = pd.read_csv(ekpo_file)

def po_exist(mandt, ebeln, d_date):
    if isinstance(d_date, datetime.datetime):
        ekko_df.AEDAT = pd.to_datetime(ekko_df.AEDAT).dt.normalize()
        checking_date = ekko_df[(ekko_df.EBELN == ebeln) & (ekko_df.MANDT == mandt)]['AEDAT'][0]
        if checking_date < d_date:
            return 'PO did not exist on the given date'
        else:
            return d_date
    else:
        return "Your given date is not an actual date or you typed it in the wrong format."
    

def merge_ch(mandt, ebeln):
    cdpos_df = cdpos_df[(cdpos_df.FNAME == 'NETWR') & (cdpos_df.MANDANT == mandt)]

    to_keep = ['MANDANT','OBJECTID','CHANGENR','UDATE','UTIME','TABKEY','FNAME','VALUE_NEW','VALUE_OLD']
    merge_c = cdhdr_df.merge(cdpos_df, how= 'right', on=['CHANGENR','MANDANT','OBJECTID','OBJECTCLAS'])[to_keep]
    
    merge_c.UDATE = pd.to_datetime(merge_c['UDATE']).dt.normalize()
    merge_c.UTIME = pd.to_datetime(merge_c.UTIME).dt.time
    
    merge_c['EBELN'] = merge_c['TABKEY'].astype(str)
    merge_c.EBELN = merge_c.EBELN.str[2:-5]
    merge_c.EBELN = merge_c.EBELN.apply(int)
    
    merge_c['EBELP'] = merge_c['TABKEY'].astype(str)
    merge_c.EBELP = merge_c.EBELP.str[-5:]
    merge_c.EBELP = merge_c.EBELP.apply(int)
    
    merge_c = merge_c[merge_c.EBELN == ebeln].sort_values(by= ['EBELP','UDATE','UTIME'], ascending=False)
    
    return merge_c


def price_date(d_date):
    merge_c = merge_ch()
    for x in merge_c.EBELP:
        for y in merge_c.UDATE:
            if d_date > y:
                price_on = merge_c[(merge_c.EBELP == x) & (merge_c.UDATE == y)]
                ekpo_df = ekpo_df.merge(price_on, how = 'left', on = 'EBELP')
    return ekpo_df
                
    
'''
def merge_ekpo(ebeln):
    merge_c = merge_ch()
    ekpo_df = ekpo_df[ekpo_df.EBELN == ebeln]
    merge_ekpo = ekpo_df.merge(merge_c, on=['EBELN','EBELP']).drop(columns= ['BUKRS','WERKS','MANDANT','NETWR','NETPR']).sort_values(by= ['EBELN','EBELP','UDATE','UTIME'], ascending=False)

    ekpo_df['TABKEY'] = ekpo_df.MANDT.apply(str) + ekpo_df.EBELN.apply(str) + '000' + ekpo_df.EBELP.apply(str)
    ekpo_df.TABKEY = ekpo_df.TABKEY.apply(int)
    merge_ekpo = ekpo_df.merge(merge_c, on='TABKEY')
    return merge_ekpo
'''
