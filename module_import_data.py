import time
from sqlalchemy import create_engine

def df2db(df, tablename, type, engine,schemaname):
    print(tablename + "导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    df.to_sql(name=tablename, con=engine, schema=schemaname,if_exists=type, index=False)
    print(tablename + "导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

if __name__ == '__main__':
    engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306")
