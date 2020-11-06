# -*- coding: utf-8 -*-
from typing import List, Union

import pandas as pd

from zvt import IntervalLevel, AdjustType
from zvt.api import get_kdata
from zvt.contract import EntityMixin
from zvt.domain import Stock
from zvt.factors import Transformer, TechnicalFactor, Accumulator


def a_include_b(a, b):
    return (a['high'] >= b['high']) and (a['low'] <= b['low'])


def is_including(kdata1, kdata2):
    return a_include_b(kdata1, kdata2) or a_include_b(kdata2, kdata1)


class ZenTransformer(Transformer):
    def __init__(self) -> None:
        super().__init__()
        # 算法和概念

        # 上涨: k线高点比之前高
        # 下跌: k线低点比之前低
        # 处理k线包含关系：上涨时，取高点，下跌时，取低点。
        # 处理完包含关系后
        # [候选顶分型]: 中间k线比两边的高点高,是一条特定的k线
        # [候选底分型]: 中间k线比两边的低点低，是一条特定的k线
        # [连接k线]: 除此之外的所有k线,n条k线,n>1

        # 确定 [顶分型] 的条件: [连接k线]---[候选顶分型]---[连接k线]---[候选底分型],在这之前只能算是 [候选顶分型]，反之，亦然。

        # 以先有 [候选顶分型]为例， 连接的变化如下:
        # [连接k线]---[候选顶分型]---[连接k线]->

        # 1)[候选顶分型]---[连接k线]---[候选顶分型] ->
        # 选择顶高的作为 [候选顶分型] 另外一个变成 [连接k线] ->
        # [连接k线]---[候选顶分型] or [候选顶分型]---[连接k线]---[连接k线]

        # 2)[候选顶分型]---[连接k线]---[候选底分型] ->
        # [顶分型]---[连接k线]---[候选底分型]---[连接k线]

        # 顶分型
        self.indicators.append('is_ding')
        self.indicators.append('is_tmp_ding')
        # 顶分型力度
        self.indicators.append('ding_power')

        # 底分型
        self.indicators.append('is_di')
        self.indicators.append('is_tmp_di')
        # 底分型力度
        self.indicators.append('di_power')

        # 斜率
        self.indicators.append('slope')

    def transform_one(self, one_df: pd.DataFrame) -> pd.DataFrame:
        # ding di normal tmp_ding tmp_di
        # 终态: ding di normal
        # 中间态:
        # tmp_ding -> ding | normal
        # tmp_di -> di | normal
        one_df = one_df.reset_index(drop=True)

        one_df['fenxing'] = 'normal'

        # 记录候选，不变
        one_df['is_tmp_ding'] = False
        one_df['is_tmp_di'] = False

        # 取前11条k线，至多出现一个顶分型+底分型
        df = one_df.iloc[:11]
        ding_kdata = df[df['high'].max() == df['high']]
        ding_index = ding_kdata.index[-1]

        di_kdata = df[df['low'].min() == df['low']]
        di_index = di_kdata.index[-1]

        # 确定第一个分型
        start_index = 0
        if ding_index > di_index:
            one_df.loc[di_index, 'fenxing'] = 'di'
            start_index = ding_index
            direction = 'up'
        elif ding_index < di_index:
            one_df.loc[ding_index, 'fenxing'] = 'ding'
            start_index = di_index
            direction = 'down'

        pre_kdata = one_df.iloc[start_index - 1]
        pre_index = start_index - 1

        for index, kdata in one_df.iloc[start_index:].iterrows():
            # 包含关系
            if is_including(kdata, pre_kdata):
                if direction == 'up':
                    high = max(kdata['high'], pre_kdata['high'])
                    low = max(kdata['low'], pre_kdata['low'])
                else:
                    high = min(kdata['high'], pre_kdata['high'])
                    low = min(kdata['low'], pre_kdata['low'])

                # 设置处理后的高低点
                one_df.loc[index, 'high'] = high
                one_df.loc[index, 'low'] = low
                one_df.loc[pre_index, 'high'] = high
                one_df.loc[pre_index, 'low'] = low
            pre_kdata = kdata
            pre_index = index

        return one_df


class ZenFactor(TechnicalFactor):

    def __init__(self, entity_schema: EntityMixin = Stock, provider: str = None, entity_provider: str = None,
                 entity_ids: List[str] = None, exchanges: List[str] = None, codes: List[str] = None,
                 the_timestamp: Union[str, pd.Timestamp] = None, start_timestamp: Union[str, pd.Timestamp] = None,
                 end_timestamp: Union[str, pd.Timestamp] = None, columns: List = None, filters: List = None,
                 order: object = None, limit: int = None, level: Union[str, IntervalLevel] = IntervalLevel.LEVEL_1DAY,
                 category_field: str = 'entity_id', time_field: str = 'timestamp', computing_window: int = None,
                 keep_all_timestamp: bool = False, fill_method: str = 'ffill', effective_number: int = None,
                 transformer: Transformer = ZenTransformer(), accumulator: Accumulator = None,
                 need_persist: bool = False, dry_run: bool = False, adjust_type: Union[AdjustType, str] = None) -> None:
        super().__init__(entity_schema, provider, entity_provider, entity_ids, exchanges, codes, the_timestamp,
                         start_timestamp, end_timestamp, columns, filters, order, limit, level, category_field,
                         time_field, computing_window, keep_all_timestamp, fill_method, effective_number, transformer,
                         accumulator, need_persist, dry_run, adjust_type)


if __name__ == '__main__':
    df = get_kdata(entity_ids=['stock_sz_000338'])
    t = ZenTransformer()
    t.transform_one(df)
