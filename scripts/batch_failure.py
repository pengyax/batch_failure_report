import pandas as pd
import numpy as np
from sql_engine import connect

def extract_division(x):
    # 如果是以ISP开头的，保留原值
    if x.startswith("ISP"):
        return x
    # 如果是包含数字的，提取数字
    else:
        # 使用正则表达式匹配数字
        import re
        match = re.search(r"\d+", x)
        # 如果匹配成功，返回数字
        if match:
            return match.group()
        # 否则，返回空值
        else:
            return None

def generate_remark(disposition):
    # 把disposition参数转换成小写
    disposition = disposition.lower()
    # 判断是否包含Rework, PIC或Reinspection
    if 'rework' in disposition or 'return' in disposition or 'reinspection' in disposition:
        # 如果是，返回Reworked
        return 'Reworked'
    # 判断是否包含deviation, PIC或accept
    elif 'deviation' in disposition or 'pic' in disposition or 'accept' in disposition:
        # 如果是，返回Accepted
        return 'Accepted'
    # 其他情况
    else:
        # 返回Other
        return 'Other'

def get_market(x):
    # 如果Division是以ISP开头，Market的值是ISP
    if x.startswith("ISP"):
        return "ISP"
    # 如果Division是数字，Market的值是US
    elif x.isdigit():
        return "US"
    # 否则，Market的值是Unknown
    else:
        return "Unknown"  

def process(df_add,df_old):
    df_QIM_duplicate = (
    df_add
    .query('Path.str.startswith("QIM",na=False)')
    .sort_values(['ID','Inspection Date'],ascending=[False,False])
    .drop_duplicates(subset=['ID'],keep='last')
    )
    
    df_offline_duplicate = (
    df_add
    .query('~Path.str.startswith("QIM",na=False)')
    .sort_values(['ID','Inspection Date'],ascending=[False,False])
    .drop_duplicates(subset=['Lot Number','Vendor','Item Number','Inspector','Inspection Date'],keep='last')
    )

    df_duplicate = pd.concat([df_QIM_duplicate,df_offline_duplicate],ignore_index=True)

    df_duplicate = (
    df_duplicate
    .rename(columns={'Comments':'Disposition'})
    .assign(Supervisor = np.nan)
    .loc[:,['ID','Inspection Date','Supervisor','Inspector','Vendor Code','Vendor','Division','Item Number','PO Number','Lot Number','Reject Code','Reject Description','Disposition']]
    )
    
    (
    df_old
    .loc[:,['ID','Inspection Date','Supervisor','Inspector','Vendor Code','Vendor','Division','Item Number','PO Number','Lot Number','Reject Code','Reject Description','Disposition']]
    .query('`Reject Description`.str.contains("100%",na=False,case=False)')
    .pipe(lambda d : pd.concat([d,df_duplicate],ignore_index=True))
    .assign(Division = lambda d : d.Division.astype(str))
    .assign(Division = lambda d : d.Division.apply(extract_division),
            Comments = lambda d : d['Disposition'].apply(generate_remark),
            Supervisor = lambda d : d.apply(lambda s : vendor_mapping_dict.get(int(s['Vendor Code'])) if pd.isna(s['Supervisor']) else s['Supervisor'], axis = 1),
            Market = lambda d : d.Division.apply(get_market)
            )
    .to_excel('../data_output/out1.xlsx',index= False)
    )

if __name__ == "__main__":
    
    vendor_mapping = pd.read_excel(r'C:\Medline\2. CPM\data\vendor_mapping\Vendor _mapping 2024_v1.xlsx')
    vendor_mapping_dict = dict(zip(vendor_mapping['Vendor Number'],vendor_mapping['Supervisor']))
     
    fn_engine = connect('fn_mysql')
    sql_query = r'''
                    select
                    ID,
                    `PO Number`,
                    `Lot Number`,
                    `Vendor Code`,
                    Vendor,
                    Division,
                    `Inspection Date`,
                    Inspector,
                    `Item Number`,
                    Results,
                    `Reject Code`,
                    `Reject Description`,
                    `Comments`,
                    `Path`
                from
                    inspection_data_all
                where 1 = 1
                and `Inspection Date` >= '2023-07-01'
                and Results = 'R'
                and `Reject Description` like '%%100\%%%%'
                order by `Inspection Date`   
                '''
    df_inspection = pd.read_sql(sql_query,fn_engine)
    df_old = pd.read_excel('../data_input/Rejection_data.xlsx')
    
    process(df_inspection,df_old)