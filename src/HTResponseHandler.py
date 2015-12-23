# -*- coding: cp936 -*-
import numpy as np
from datetime import datetime
from datetime import timedelta
from ResponseHandler import ResponseHandler


class ZXResponseHandler(ResponseHandler):

    def __init__(self, q, events, response_table, logger):
        super(ZXResponseHandler, self).__init__(q, events, response_table, logger)
        # todo δ�� �ѱ� δ�� �ѱ����� ��δ����
        # �����ǲ��ֳɽ�,�����ѳ�
        self.status = {u'δ��': 0, u'�ѱ�': 1, u'δ��': 2, u'�ѱ�����': 3,  u'�ѳ�': 4,  u'�ѳ�': 5, u'����': 5,  u'�ϵ�': 6}


    # �����ѱ����ί�к���δ��ɵ�ί��
    def tagged_changes(self, new_orders):
        try:
            # δ��ɵ�ί�е�status<4
            tagged_unfinished = self.orders[(self.orders['tagged'] == 1) &
                                            (self.orders['status'] < 4)]
            self.logger.debug('tagged and unfinished orders: tagged_unfinished=%s', tagged_unfinished)
            for i in range(len(tagged_unfinished)):
                match = new_orders[new_orders[u'��ͬ���'] == tagged_unfinished['entrustno'].iloc[i]]
                changed = 0
                if len(match) > 0:
                    if tagged_unfinished['bidprice'].iloc[i] != match[u'�ɽ�����'].iloc[0]:
                        tagged_unfinished['bidprice'].iloc[i] = match[u'�ɽ�����'].iloc[0]
                        changed = 1
                    ratio = 1 if tagged_unfinished['askvol'].iloc[i] > 0 else -1
                    if tagged_unfinished['bidvol'].iloc[i] != ratio * match[u'�ɽ�����'].iloc[0]:
                        tagged_unfinished['bidvol'].iloc[i] = ratio * match[u'�ɽ�����'].iloc[0]
                        changed = 1
                    # û���ѳ�����
                    # if tagged_unfinished['withdraw'].iloc[i] != match[u'�ѳ�����'].iloc[0]:
                    #     tagged_unfinished['withdraw'].iloc[i] = match[u'�ѳ�����'].iloc[0]
                    #     changed = 1
                    # TODO
                    status = match[u'��ע'].iloc[0]
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

    # ���ί�кţ������³ɽ���Ϣ
    def untagged_changes(self, new_orders, nearest_min):
        try:

            # ��orders�в��ҳ�������������ڣ�����ʱ��Ĳ���������û�б���ǵ�ί�м�¼
            nearest = datetime.now() - timedelta(minutes=nearest_min)
            untagged = self.orders[(self.orders['tagged'] == 0) & (self.orders['time'] > nearest)]

            self.logger.debug('untagged orders: untagged=%s', untagged.to_string())
            # ��new_orders�в��ҳ�û��ƥ��ί�б�ŵļ�¼
            to_tag = new_orders[new_orders[u'��ͬ���'].isin(self.orders['entrustno']) == False ]
            to_tag['tagged'] = np.zeros(len(to_tag))
            self.logger.debug('to_tag = %s', to_tag.to_string())
            # to_tag = to_tag.set_index([u'ί��ʱ��'])
            for i in range(len(untagged)):
                old = untagged['time'].iloc[i] - timedelta(minutes=2)
                new = untagged['time'].iloc[i] + timedelta(minutes=2)

                # ʱ����ƥ������2���ӵ�
                time_match = to_tag.between_time(old, new)
                self.logger.debug('time_match = %s', time_match.to_string())
                self.logger.debug('time_match: dtypes = %s', time_match.dtypes)
                # TODO
                match = time_match[(time_match[u'֤ȯ����'] == int(untagged['stockcode'].iloc[i])) &
                                   (time_match[u'ί�м۸�'] == untagged['askprice'].iloc[i]) &
                                   (time_match[u'ί������'] == abs(int(untagged['askvol'].iloc[i]))) &
                                   (time_match[u'����'].find(u'��' if int(untagged['askvol'].iloc[i]) > 0 else u'��')) &
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
                    entrustno = match[u'��ͬ���'].iloc[0]
                    if untagged['entrustno'].iloc[i] != entrustno:
                        untagged['entrustno'].iloc[i] = entrustno
                    if untagged['bidprice'].iloc[i] != match[u'�ɽ�����'].iloc[0]:
                        untagged['bidprice'].iloc[i] = match[u'�ɽ�����'].iloc[0]
                    ratio = 1 if untagged['askvol'].iloc[i] > 0 else -1
                    if untagged['bidvol'].iloc[i] != ratio * match[u'�ɽ�����'].iloc[0]:
                        untagged['bidvol'].iloc[i] = ratio * match[u'�ɽ�����'].iloc[0]
                    # if untagged['withdraw'].iloc[i] != match[u'�ѳ�����'].iloc[0]:
                    #     untagged['withdraw'].iloc[i] = match[u'�ѳ�����'].iloc[0]
                    # TODO
                    status = match[u'��ע'].iloc[0]
                    status = status[:2]
                    if untagged['status'].iloc[i] != self.status[status]:
                        untagged['status'].iloc[i] = self.status[status]
                    to_tag.loc[to_tag[u'��ͬ���'] == entrustno, 'tagged'] = 1
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
