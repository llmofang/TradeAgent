# -*- coding: utf-8 -*-
import numpy as np
from datetime import datetime
from datetime import timedelta
from ResponseHandler import ResponseHandler


class HTResponseHandler(ResponseHandler):

    def __init__(self, q, events, response_table, logger):
        super(HTResponseHandler, self).__init__(q, events, response_table, logger)
        # todo 未报 已报 未成 已报待撤 暂未捕获
        # 部撤是部分成交,部分已撤
        self.status = {u'未报': 0, u'已报': 1, u'未成': 2, u'已报待撤': 3,  u'已成': 4,  u'已撤': 5, u'部撤': 5,  u'废单': 6}


    # 更新已标记了委托号且未完成的委托
    def tagged_changes(self, new_orders):
        try:
            # 未完成的委托的status<4
            tagged_unfinished = self.orders[(self.orders['tagged'] == 1) &
                                            (self.orders['status'] < 4)]
            self.logger.debug('tagged and unfinished orders: tagged_unfinished=%s', tagged_unfinished)
            for i in range(len(tagged_unfinished)):
                match = new_orders[new_orders[u'合同编号'] == tagged_unfinished['entrustno'].iloc[i]]
                changed = 0
                if len(match) > 0:
                    if tagged_unfinished['bidprice'].iloc[i] != match[u'成交均价'].iloc[0]:
                        tagged_unfinished['bidprice'].iloc[i] = match[u'成交均价'].iloc[0]
                        changed = 1
                    ratio = 1 if tagged_unfinished['askvol'].iloc[i] > 0 else -1
                    if tagged_unfinished['bidvol'].iloc[i] != ratio * match[u'成交数量'].iloc[0]:
                        tagged_unfinished['bidvol'].iloc[i] = ratio * match[u'成交数量'].iloc[0]
                        changed = 1
                    # 没有已撤数量
                    # if tagged_unfinished['withdraw'].iloc[i] != match[u'已撤数量'].iloc[0]:
                    #     tagged_unfinished['withdraw'].iloc[i] = match[u'已撤数量'].iloc[0]
                    #     changed = 1
                    # TODO
                    status = match[u'备注'].iloc[0]
                    status = status[:2]
                    self.logger.debug('status = %s', status)
                    if tagged_unfinished['status'].iloc[i] != self.status[status]:
                        tagged_unfinished['status'].iloc[i] = self.status[status]
                        changed = 1
                    tagged_unfinished['changed'].iloc[i] = changed
            changes = tagged_unfinished[tagged_unfinished['changed'] == 1]

            if len(changes) > 0:
                self.logger.debug('tagged and unfinished orders: changes=%s', changes.to_string())
                self.logger.debug('sending changes to kdb')
                self.send_changes(changes)
                self.logger.debug('before update changes: orders=%s', self.orders.to_string())
                self.orders.update(changes)
                self.logger.debug('after update changes: orders=%s', self.orders.to_string())
        except Exception, e:
            self.logger.error(e)

    # 标记委托号，并更新成交信息
    def untagged_changes(self, new_orders, nearest_min):
        try:

            # 从orders中查找出最近三分钟以内（更早时间的不做处理），没有被标记的委托记录
            nearest = datetime.now() - timedelta(minutes=nearest_min)
            untagged = self.orders[(self.orders['tagged'] == 0) & (self.orders['time'] > nearest)]

            self.logger.debug('untagged orders: untagged=%s', untagged.to_string())
            # 从new_orders中查找出没有匹配委托编号的记录
            to_tag = new_orders[new_orders[u'合同编号'].isin(self.orders['entrustno']) == False ]
            to_tag['tagged'] = np.zeros(len(to_tag))
            to_tag[u'买卖'] = to_tag[u'操作'].apply(lambda x: u'证券买入' if x.find(u'买') else u'证券卖出')
            to_tag[u'委托状态'] = to_tag[u'备注'].str[:2]
            self.logger.debug('to_tag = %s', to_tag.to_string())
            # to_tag = to_tag.set_index([u'委托时间'])
            for i in range(len(untagged)):
                old = untagged['time'].iloc[i] - timedelta(minutes=2)
                new = untagged['time'].iloc[i] + timedelta(minutes=2)

                # 时间上匹配上下2分钟的
                time_match = to_tag.between_time(old, new)
                self.logger.debug('time_match = %s', time_match.to_string())
                self.logger.debug('time_match: dtypes = %s', time_match.dtypes)
                # TODO
                match = time_match[(time_match[u'证券代码'] == int(untagged['stockcode'].iloc[i])) &
                                   (time_match[u'委托价格'] == untagged['askprice'].iloc[i]) &
                                   (time_match[u'委托数量'] == abs(int(untagged['askvol'].iloc[i]))) &
                                   (time_match[u'买卖'] == (u'证券买入' if int(untagged['askvol'].iloc[i]) > 0 else u'证券卖出')) &
                                   (time_match['tagged'] == 0)]
                self.logger.debug('find order match untagged order: match=%s', match.to_string())
                match = match.sort_index(ascending=True)
                changed = 0
                if len(match) > 0:
                    changed = 1
                    if len(match) == 1:
                        self.logger.debug('Perfect match row!')
                    else:
                        self.logger.info('Find more than 1 for row!')
                    entrustno = match[u'合同编号'].iloc[0]
                    if untagged['entrustno'].iloc[i] != entrustno:
                        untagged['entrustno'].iloc[i] = entrustno
                    if untagged['bidprice'].iloc[i] != match[u'成交均价'].iloc[0]:
                        untagged['bidprice'].iloc[i] = match[u'成交均价'].iloc[0]
                    ratio = 1 if untagged['askvol'].iloc[i] > 0 else -1
                    if untagged['bidvol'].iloc[i] != ratio * match[u'成交数量'].iloc[0]:
                        untagged['bidvol'].iloc[i] = ratio * match[u'成交数量'].iloc[0]
                    # if untagged['withdraw'].iloc[i] != match[u'已撤数量'].iloc[0]:
                    #     untagged['withdraw'].iloc[i] = match[u'已撤数量'].iloc[0]
                    # TODO
                    status = match[u'备注'].iloc[0]
                    status = status[:2]
                    if untagged['status'].iloc[i] != self.status[status]:
                        untagged['status'].iloc[i] = self.status[status]
                    to_tag.loc[to_tag[u'合同编号'] == entrustno, 'tagged'] = 1
                    untagged['changed'].iloc[i] = changed

                else:
                    self.logger.error('Can not find matched order!')

            changes = untagged[untagged['changed'] == 1]
            if len(changes) > 0:
                self.logger.debug('untagged orders: changes=%s', changes.to_string())
                self.logger.debug('sending changes to kdb')
                self.send_changes(changes)
                self.logger.debug('before update changes: orders=%s', self.orders.to_string())
                self.orders.update(changes)
                self.logger.debug('after update changes: orders=%s', self.orders.to_string())

        except Exception, e:
            self.logger.error(e)
