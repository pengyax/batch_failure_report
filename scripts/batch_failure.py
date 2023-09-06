import pandas as pd
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
    # 判断是否包含deviation, PIC或accept
    if 'deviation' in disposition or 'pic' in disposition or 'accept' in disposition:
        # 如果是，返回Accepted
        return 'Accepted'
    # 判断是否包含Rework, PIC或Reinspection
    elif 'rework' in disposition or 'pic' in disposition or 'reinspection' in disposition:
        # 如果是，返回Reworked
        return 'Reworked'
    # 其他情况
    else:
        # 返回Other
        return 'Other'

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

    return df_duplicate,df_old









if __name__ == "__main__":
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