#Imports
import pandas as pd
import datetime
import numpy as np

class TotalNetWR:
    '''
    input: mandatnr, PO# and date
    output: total netwr
    '''
    def __init__(self, mandt, ebeln, d_date):
        try:
            self.mandt = mandt
            self.ebeln = ebeln
            #making the date to datetime
            self.d_date = pd.to_datetime(d_date)
            self.merge_c = None
            self.ekpo_df = None
            self.prices = None
            
            # Define file paths
            cdhdr_file = "CDHDR.csv"
            cdpos_file = "CDPOS.csv"
            ekko_file = "EKKO.csv"
            ekpo_file = "EKPO.csv"

            # Read CSV files into pandas DataFrames
            self.cdhdr_df = pd.read_csv(cdhdr_file)
            self.cdpos_df = pd.read_csv(cdpos_file)
            self.ekko_df = pd.read_csv(ekko_file)
            self.ekpo_df = pd.read_csv(ekpo_file)
            
        except ValueError:
            # In case the date cannot be converted to datetime
            print("Your given date is not an actual date or you typed it in the wrong format.")

    def po_exist(self):
        
        # checking whether the PO has already existed on the date we are requesting the total_netwr for
        
        self.ekko_df.AEDAT = pd.to_datetime(self.ekko_df.AEDAT).dt.normalize()
        checking_date = self.ekko_df[(self.ekko_df.EBELN == self.ebeln) & (self.ekko_df.MANDT == self.mandt)]['AEDAT'].values
        
        if checking_date > self.d_date:
            return 'PO did not exist on the given date'
        else:
            return self.d_date
        

    def merge_ch(self):
        
        # merging CDHDR and CDPOS to have the dates for the value changes. Also filtering rows out that do not belong to the requested PO
        
        self.cdpos_df = self.cdpos_df[(self.cdpos_df.FNAME == 'NETWR') & (self.cdpos_df.MANDANT == self.mandt)]

        to_keep = ['MANDANT','OBJECTID','CHANGENR','UDATE','UTIME','TABKEY','FNAME','VALUE_NEW','VALUE_OLD']
        merge_c = self.cdhdr_df.merge(self.cdpos_df, how= 'right', on=['CHANGENR','MANDANT','OBJECTID','OBJECTCLAS'])[to_keep]
        
        merge_c.UDATE = pd.to_datetime(merge_c['UDATE']).dt.normalize()
        merge_c.UTIME = pd.to_datetime(merge_c.UTIME).dt.time
        
        merge_c['EBELN'] = merge_c['TABKEY'].astype(str)
        merge_c.EBELN = merge_c.EBELN.str[2:-5]
        merge_c.EBELN = merge_c.EBELN.apply(int)
        
        merge_c['EBELP'] = merge_c['TABKEY'].astype(str)
        merge_c.EBELP = merge_c.EBELP.str[-5:]
        merge_c.EBELP = merge_c.EBELP.apply(int)
        
        self.merge_c = merge_c[merge_c.EBELN == self.ebeln].sort_values(by= ['EBELP','UDATE','UTIME'], ascending=False)

        return self.merge_c


    def price_date(self):
        
        price_on_df = pd.DataFrame()
        for x in self.merge_c.EBELP.unique():
            # Boolean to track whether a valid price_on is found
            found_valid_price = False  
            
            for y in self.merge_c[(self.merge_c.EBELP == x)].UDATE.unique():
                if self.d_date > y:
                    price_on = self.merge_c[(self.merge_c.EBELP == x) & (self.merge_c.UDATE == y)]
                    price_on = price_on[price_on.UTIME == price_on.UTIME.max()]
                    
                    if not price_on.empty:
                        price_on_df = pd.concat([price_on_df, price_on], ignore_index=True).drop_duplicates()
                        # Change boolean in case price found
                        found_valid_price = True  
                        
                        # Exit the loop
                        break  

                if found_valid_price == False: 
                    price_on = self.merge_c[self.merge_c.EBELP == x]
                    
                    price_on = price_on[price_on.UDATE == price_on.UDATE.min()]
                    price_on = price_on[price_on.UTIME == price_on.UTIME.min()]
                    price_on_df = pd.concat([price_on_df, price_on], ignore_index=True).drop_duplicates()

           
         # Merge the price_on and ekpo dfs
        self.ekpo_df = self.ekpo_df[(self.ekpo_df.MANDT == self.mandt) & (self.ekpo_df.EBELN == self.ebeln)]
        self.ekpo_df = self.ekpo_df.merge(price_on_df, how='left', on=['EBELN', 'EBELP'])

        return self.ekpo_df
        
    def sum_po(self):
        '''
        first I thought we need to multiply the value with the requested 
        amount but my assumption after looking into the data is that the value takes the amount into consideration already
        sometimes there are items in EKPO with no mention of them in the CDPOS. As I am not 100% aware of what that means, all items that were not in CDPOS,
        I am taking the price mentioned in EKPO. If that means that we should exclude them of the sum, we can change the ekpo_df and price_on_df merge from left to inner.
        In that case we would only have the PO items in the sum that are mentioned in the CDPOS.
        '''
        prices = []
        #print(self.ekpo_df.EBELP.unique())
        for x in self.ekpo_df.EBELP.unique():
            
            # in case the item is not in CDPOS
            if np.isnan(self.ekpo_df[self.ekpo_df.EBELP == x]['UDATE'].values[0]):
                prices.append(float(self.ekpo_df[self.ekpo_df.EBELP == x]['NETWR'].values[0]))
            
            elif self.d_date > self.ekpo_df[self.ekpo_df.EBELP == x]['UDATE'].values[0]:
                prices.append(float(self.ekpo_df[self.ekpo_df.EBELP == x]['VALUE_NEW'].values[0]))
            else:
                prices.append(float(self.ekpo_df[self.ekpo_df.EBELP == x]['VALUE_OLD'].values[0]))
        self.prices = sum(prices)
        return print(self.prices)
    
    
#Trying the class out with random mandat#, PO# and a date
TWR = TotalNetWR(10,71157250,'2019-01-21')
if TWR.po_exist() == TWR.d_date:
    TWR.merge_ch()
    TWR.price_date()
    TWR.sum_po()
else:
    print(TWR.po_exist())
