import pandas as pd
import datetime,pymysql,os,time
import sqlalchemy
from sqlalchemy import create_engine

# 定义已出样型号list，需要出样list，未出样list
list_result=[]
list_sampling_rule=[]
unsampled=[]

engine=create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods")

# 获取数据库出样数据
sql_sampled='''  select distinct c.area as 区域,c.center as 中心, a.门店编码, case when a.门店等级 in ('B级', 'A级', 'A+级') then 1
                else 0 end as 是否B及以上, a.二级分类, a.经营状态 , case when b.型号编码 = '71038120Z00461' then 'BVL1D100EY' 
                when b.型号编码 is null then '未出样'
                else SUBSTRING_INDEX(b.型号, ' ', 1) end as 已出型号
                from ods.终端门店数据 a
                left join ods.样机上样数据 b on
                a.门店编码 = b.门店编码
                left join ods.area_center_zhihuanwang c on
                a.所属分部 = c.center_name
                where a.二级分类 in('TOP', '国美常规店', '苏宁常规店', '五星常规店')
                and a.经营状态 = '经营中'
                and a.门店编码 not in ('S00013753', 'S00013747', 'S00013751', 'S00013752', 'S00013746', 'S00013758',
                 'S00013750', 'S00013749', 'S00013757', 'S00013754', 'S00013756', 'S00013755', 'S00190884', 'S00013748', 
                 'S00111481', 'S00207977', 'S00205785', 'S00216807', 'S00220694', 'S00111482', 'S00215147', 'S00238752', 
                 'S00195313', 'S00221661', 'S00254173', 'S00245156', 'S00204008', 'S00220052', 'S00218703', 'S00248791', 
                 'S00248792', 'S00245264', 'S00220645', 'S00240236', 'S00222345', 'S00247886', 'S00212587', 'S00211049', 
                 'S00211050', 'S00240577', 'S00206528', 'S00226319', 'S00241471', 'S00242485', 'S00244248', 'S00247887', 
                 'S00246243', 'S00242660', 'S00251059', 'S00257684', 'S00225996', 'S00248278', 'S00218704', 'S00258410', 
                 'S00254449', 'S00252714', 'S00222344', 'S00244642', 'S00248793', 'S00246244', 'S00249285', 'S00252380', 
                 'S00254450', 'S00260621', 'S00261494', 'S00265661', 'S00014514', 'S00014530', 'S00081933', 'S00018930', 
                 'S00018931', 'S00014529', 'S00014532', 'S00014531', 'S00202394', 'S00120176', 'S00014527', 'S00018932', 
                 'S00014533', 'S00019237', 'S00014520', 'S00014519', 'S00014528', 'S00014525', 'S00014515', 'S00014526', 
                 'S00014523', 'S00014516', 'S00014517', 'S00146763', 'S00014524', 'S00014522', 'S00226357', 'S00262313', 
                 'S00119853', 'S00124124', 'S00180652', 'S00014097', 'S00014087', 'S00101749', 'S00014091', 'S00014092', 
                 'S00014098', 'S00156115', 'S00014095', 'S00014086', 'S00134544', 'S00089281', 'S00101700', 'S00102731', 
                 'S00014090', 'S00014099', 'S00257716', 'S00238750', 'S00260474', 'S00014125', 'S00014121', 'S00014122', 
                 'S00014126', 'S00018737', 'S00014118', 'S00081609', 'S00014123', 'S00014116', 'S00116153', 'S00014119', 
                 'S00145219', 'S00014120', 'S00116283', 'S00204321', 'S00130551', 'S00214319', 'S00081634', 'S00259266', 
                 'S00069000', 'S00014117', 'S00143619', 'S00263164', 'S00089291', 'S00014132', 'S00014128', 'S00014131', 
                 'S00201167', 'S00196555', 'S00089293', 'S00014134', 'S00014133', 'S00219678', 'S00013843', 'S00128270', 
                 'S00081028', 'S00013841', 'S00013840', 'S00206925', 'S00197058', 'S00013839', 'S00197057', 'S00013842', 
                 'S00215833', 'S00081894', 'S00021513', 'S00100104', 'S00132999', 'S00021726', 'S00204397', 'S00247530', 
                 'S00254183', 'S00261141', 'S00081365', 'S00081368', 'S00081320', 'S00081183', 'S00084059', 'S00193864', 
                 'S00081852', 'S00081669', 'S00084994', 'S00036689', 'S00090617', 'S00090618', 'S00126349', 'S00082059', 
                 'S00014538', 'S00018615', 'S00019092', 'S00019234', 'S00019090', 'S00150195', 'S00116310', 'S00076956', 
                 'S00205344', 'S00261897', 'S00014112', 'S00019091', 'S00014275', 'S00014274', 'S00014276', 'S00014279', 
                 'S00141691', 'S00014278', 'S00014277', 'S00261334', 'S00013676', 'S00013674', 'S00013671', 'S00013675', 
                 'S00013672', 'S00250171', 'S00095003', 'S00083903', 'S00086919', 'S00084797', 'S00087040', 'S00199704', 
                 'S00198543', 'S00100469', 'S00226610', 'S00013978', 'S00013979', 'S00013980', 'S00013981', 'S00190845', 
                 'S00013982', 'S00014081', 'S00014082', 'S00216509', 'S00184044', 'S00014084', 'S00206100', 'S00014083', 
                 'S00279239', 'S00014137', 'S00018634', 'S00014136', 'S00014135', 'S00081053', 'S00081030', 'S00081035', 
                 'S00090216', 'S00081029', 'S00157665', 'S00081034', 'S00227640', 'S00090213', 'S00014404', 'S00014400', 
                 'S00014405', 'S00014401', 'S00084077', 'S00095803', 'S00014406', 'S00254185', 'S00095918', 'S00014403', 
                 'S00014399', 'S00014397', 'S00014407', 'S00081525', 'S00084008', 'S00087138', 'S00013991', 'S00013989', 
                 'S00013988', 'S00013990', 'S00079540', 'S00079543', 'S00079542', 'S00079541', 'S00090011', 'S00103284', 
                 'S00013835', 'S00013832', 'S00013834', 'S00013833', 'S00013837', 'S00213719', 'S00083007', 'S00014195', 
                 'S00014192', 'S00081651', 'S00014196', 'S00189827', 'S00014193', 'S00081135', 'S00074456', 'S00081033', 
                 'S00090168', 'S00190705', 'S00090167', 'S00152701', 'S00013995', 'S00013997', 'S00013998', 'S00195665', 
                 'S00095495', 'S00013994', 'S00013962', 'S00013961', 'S00013963', 'S00062789', 'S00081806', 'S00013836', 
                 'S00081848', 'S00084070', 'S00090380', 'S00014402', 'S00014109', 'S00014085', 'S00041107', 'S00088752', 
                 'S00013838', 'S00132739', 'S00258073', 'S00123376', 'S00261778', 'S00261559', 'S00261959', 'S00251701', 
                 'S00263403', 'S00177699', 'S00268694', 'S00268744', 'S00270009', 'S00268745', 'S00269185', 'S00271046', 
                 'S00271045', 'S00272871', 'S00267946', 'S00273856', 'S00274254', 'S00276815', 'S00090683', 'S00014547', 
                 'S00014548' )
                '''

# 获取数据库出样规则
sql_sampling='''SELECT 渠道,是否B及以上,型号 as 必出型号
                FROM ods.出样规则
                where 是否B及以上='0'
                union all 
                SELECT 渠道,'1',型号 as 必出型号
                FROM ods.出样规则 '''

# 定义型号匹配表
replace_dic={'BVL1D100NET': '国米系列', 'BVL1G100NET': '国米系列', 'BVL1D100EY': '小骑士系列', 'BVL1D80EY': '小骑士系列',
             'TD100-14266WMADT': '14266系列', 'TG100-14266WMADT': '14266系列', 'TD100-14266WMIADT': '14266系列',
             'TG100-14266WMIADT': '14266系列', 'TD100PM02T': 'PM02T系列', 'TG100PM02T': 'PM02T系列',
             'TBJ110-8088WUADCLT': '8088一桶洗系列', 'TBJ90-8088WUADCLT': '8088一桶洗系列', 'TB100-6388WACLY': '6388系列',
             'TB100-6388WADCLY': '6388系列', 'TB90-6388WACLY': '6388系列', 'TB90-6388WADCLY': '6388系列',
             'TBM90PMU06DT': 'PM水魔方波轮系列', 'TB100PM02T': 'PM水魔方波轮系列', 'MG100T2WADQCY+MH100-H1WY': 'MG100+MH100',
             'MG100T1WDQC+MH100-H1W': 'MG100+MH100', 'MD100-1455WDY': '1455系列', 'MG100-1455WDY': '1455系列',
             'MD100PD3QCT': 'PD3直驱系列', 'MG100PD3QCT': 'PD3直驱系列', 'MD100N07Y': 'N07系列', 'MG100N07Y': 'N07系列',
             'MD100-1451WDY': '1451系列', 'MG100-1451WDY': '1451系列', 'MBS100PT2WADT': '双驱波轮P系列',
             'MBS90PT2WADT': '双驱波轮P系列', 'MBS100T2WADY': '双驱波轮全流通系列', 'MBS90T2WADY': '双驱波轮全流通系列'
             }
set_suit1={'MG100T2WADQCY', 'MH100-H1WY'}
set_suit2={'MG100T1WDQC', 'MH100-H1W'}


def group_concat(df, col):
    '''
    将df进行分组合并
    :param df:dataframe
    :param col:column
    :return: df
    '''
    df[col]=','.join(set(df[col]))
    return df.drop_duplicates()


df_sampled=pd.read_sql(sql=sql_sampled, con=engine)
df_sampled['已出型号']=df_sampled['已出型号'].replace(replace_dic)
df_sampled=df_sampled.groupby(['区域','中心', '门店编码', '是否B及以上', '二级分类', '经营状态'], group_keys=False, sort=False). \
    apply(group_concat, col='已出型号')

df_sampling=pd.read_sql(sql=sql_sampling, con=engine)
df_sampling['必出型号']=df_sampling['必出型号'].replace(replace_dic)
df_sampling=df_sampling.groupby(['渠道', '是否B及以上'], group_keys=False, sort=False).apply(group_concat, col='必出型号')


def unsampled_rule(sampled_set, sampling_set):
    """
    需要出样型号等于必须出样型号减去规则内已经出样的样机型号
    :param set1:sampled_set
    :param set2:sampling_set
    :return: 转换为字符串，返回未出样清单
    """
    return ','.join(sampling_set - (sampled_set & sampling_set))


def sample_rule(channel, level):
    """
    规则判断，返回必出型号
    :param channel:渠道
    :param level:门店等级
    :return: 规则必出型号
    """
    for i in df_sampling.index:
        if channel == df_sampling['渠道'][i] and str(level) == df_sampling['是否B及以上'][i]:
            return df_sampling['必出型号'][i]


def sample_judge():
    """
    1.判断所有门店是否都满足洗烘二选一
    2.获取不同门店渠道和等级对应规则
    """
    for i in df_sampled.index:
        sampled_list=df_sampled['已出型号'][i].split(',')
        sampled_set=set(sampled_list)
        sampling_str=sample_rule(channel=df_sampled['二级分类'][i], level=df_sampled['是否B及以上'][i])
        sampling_list=sampling_str.split(',')
        sampling_set=set(sampling_list)
        if set_suit1.issubset(sampled_set) or set_suit2.issubset(sampled_set):
            list_sampling_rule.append(sampling_str)
            list_result.append(sampling_set.issubset(sampled_set))
            unsampled.append(unsampled_rule(sampled_set, sampling_set))
        else:
            list_sampling_rule.append('洗烘套装二选一,' + sampling_str)
            unsampled.append(unsampled_rule(sampled_set, sampling_set))
            list_result.append(False)


if __name__ == '__main__':
    sample_judge()
    df_sampled['出样规则']=list_sampling_rule
    df_sampled['是否合格']=list_result
    df_sampled['二级分类']=df_sampled['二级分类'].str.replace('常规店','')
    # df_sampled['二级分类'].map(lambda x: x.replace('常规店',''))
    df_sampled['是否合格']=df_sampled['是否合格'].replace({False:0,True:1})
    df_sampled['还需出样']=unsampled
    # 定义路径，包含时间
    date=time.strftime("%Y%m%d", time.localtime(time.time()))
    print('正在导出'+date +'-出样结果.xlsx')
    df_sampled.to_excel('E:/Share/' + date + r'-出样结果.xlsx', sheet_name='出样结果', index=False)
    print('导出' + date + '-出样结果.xlsx 完成')
    df_unsampled=df_sampled.drop('还需出样', 1).join(df_sampled['还需出样'].str.split(',', expand=True).stack(). \
        reset_index().set_index('level_0').drop('level_1', 1).rename(columns={0: '还需出样'})).drop('出样规则', 1). \
        drop('已出型号', 1)
    print('正在导出' + date + '-出样明细.xlsx')
    df_unsampled.to_excel('E:/Share/' + date + r'-出样明细.xlsx', sheet_name='出样明细', index=False)
    print('导出' + date + '-出样明细.xlsx 完成')