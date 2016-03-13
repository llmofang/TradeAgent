# -*- coding: utf-8 -*-
import numpy as np
from datetime import datetime
from datetime import timedelta
from ResponseHandler import ResponseHandler


class ZXResponseHandler(ResponseHandler):
    def __init__(self, events):
        super(ZXResponseHandler, self).__init__(events)
        self.status = {u'待报': 0, u'未报': 0, u'已报': 1, u'未成': 2, u'部成': 2, u'已报待撤': 3, u'待撤': 3,
                       u'已成': 4, u'已撤': 5, u'部撤': 5, u'废单': 6}

    # 更新已标记了委托号且未完成的委托
    def tagged_changes(self, new_orders):
        try:
            # 未完成的委托的status<4
            tagged_unfinished = self.orders[(self.orders['tagged'] == 1) &
                                            (self.orders['status'] < 4)]
            print('tagged and unfinished orders: tagged_unfinished=%s', tagged_unfinished)
            for i in range(len(tagged_unfinished)):
                match = new_orders[new_orders[u'申请编号'] == tagged_unfinished['entrustno'].iloc[i]]
                changed = 0
                if len(match) > 0:
                    if tagged_unfinished['bidprice'].iloc[i] != match[u'成交价格'].iloc[0]:
                        tagged_unfinished['bidprice'].iloc[i] = match[u'成交价格'].iloc[0]
                        changed = 1
                    ratio = 1 if tagged_unfinished['askvol'].iloc[i] > 0 else -1
                    if tagged_unfinished['bidvol'].iloc[i] != ratio * match[u'成交数量'].iloc[0]:
                        tagged_unfinished['bidvol'].iloc[i] = ratio * match[u'成交数量'].iloc[0]
                        changed = 1
                    if tagged_unfinished['withdraw'].iloc[i] != match[u'已撤数量'].iloc[0]:
                        tagged_unfinished['withdraw'].iloc[i] = match[u'已撤数量'].iloc[0]
                        changed = 1
                    if tagged_unfinished['status'].iloc[i] != self.status[match[u'委托状态'].iloc[0]]:
                        tagged_unfinished['status'].iloc[i] = self.status[match[u'委托状态'].iloc[0]]
                        changed = 1
                    tagged_unfinished['changed'].iloc[i] = changed
            changes = tagged_unfinished[tagged_unfinished['changed'] == 1]

            if len(changes) > 0:
                print('tagged and unfinished orders: changes=%s', changes.to_string())
                print('sending changes to kdb')
                self.send_changes(changes)
                print('before update changes: orders=%s', self.orders.to_string())
                self.orders.update(changes)
                print('after update changes: orders=%s', self.orders.to_string())
        except Exception, e:
            print(e)

    # 标记委托号，并更新成交信息
    def untagged_changes(self, new_orders, nearest_min):
        try:

            # 从orders中查找出最近几分钟以内（更早时间的不做处理），没有被标记的委托记录
            nearest = datetime.now() - timedelta(minutes=nearest_min)
            untagged = self.orders[(self.orders['tagged'] == 0) & (self.orders['time'] > nearest)]

            print('untagged orders: untagged=%s', untagged.to_string())
            # 从new_orders中查找出没有匹配委托编号的记录
            to_tag = new_orders[(new_orders[u'申请编号'].isin(self.orders['entrustno']) == False) &
                                (new_orders[u'业务名称'] != u'撤单')]
            to_tag['tagged'] = np.zeros(len(to_tag))
            print('to_tag = %s', to_tag.to_string())
            # to_tag = to_tag.set_index([u'委托时间'])
            for i in range(len(untagged)):
                old = untagged['time'].iloc[i] - timedelta(minutes=2)
                new = untagged['time'].iloc[i] + timedelta(minutes=2)

                # 时间上匹配上下2分钟的
                time_match = to_tag.between_time(old, new)
                print('time_match = %s', time_match.to_string())
                print('time_match: dtypes = %s', time_match.dtypes)
                match = time_match[(time_match[u'证券代码'] == int(untagged['stockcode'].iloc[i])) &
                                   (time_match[u'委托价格'] == untagged['askprice'].iloc[i]) &
                                   (time_match[u'委托数量'] == abs(int(untagged['askvol'].iloc[i]))) &
                                   (time_match[u'买卖'] == (u'买入' if int(untagged['askvol'].iloc[i]) > 0 else u'卖出')) &
                                   (time_match['tagged'] == 0)]
                print('find order match untagged order: match=%s', match.to_string())
                match = match.sort_index(ascending=True)
                changed = 0
                if len(match) > 0:
                    changed = 1
                    if len(match) == 1:
                        print('Perfect match row!')
                    else:
                        print('Find more than 1 for row!')
                    entrustno = match[u'申请编号'].iloc[0]
                    if untagged['entrustno'].iloc[i] != entrustno:
                        untagged['entrustno'].iloc[i] = entrustno
                    if untagged['bidprice'].iloc[i] != match[u'成交价格'].iloc[0]:
                        untagged['bidprice'].iloc[i] = match[u'成交价格'].iloc[0]
                    ratio = 1 if untagged['askvol'].iloc[i] > 0 else -1
                    if untagged['bidvol'].iloc[i] != ratio * match[u'成交数量'].iloc[0]:
                        untagged['bidvol'].iloc[i] = ratio * match[u'成交数量'].iloc[0]
                    if untagged['withdraw'].iloc[i] != match[u'已撤数量'].iloc[0]:
                        untagged['withdraw'].iloc[i] = match[u'已撤数量'].iloc[0]
                    if untagged['status'].iloc[i] != self.status[match[u'委托状态'].iloc[0]]:
                        untagged['status'].iloc[i] = self.status[match[u'委托状态'].iloc[0]]
                    to_tag.loc[to_tag[u'申请编号'] == entrustno, 'tagged'] = 1
                    untagged['changed'].iloc[i] = changed

                else:
                    print('Can not find matched order!')

            changes = untagged[untagged['changed'] == 1]
            if len(changes) > 0:
                print('untagged orders: changes=%s', changes.to_string())
                print('sending changes to kdb')
                self.send_changes(changes)
                print('before update changes: orders=%s', self.orders.to_string())
                self.orders.update(changes)
                print('after update changes: orders=%s', self.orders.to_string())

        except Exception, e:
            print(e)
