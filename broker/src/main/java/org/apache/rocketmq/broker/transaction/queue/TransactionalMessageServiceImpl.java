/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
    }

    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            if (checkTime >= transactionCheckMax) {
                return true;
            } else {
                checkTime++;
            }
        }
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    private boolean needSkip(MessageExt msgExt) {
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        if (valueOfCurrentMinusBorn
            > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
            * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        if (putMessageResult != null
            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            msgExt.setQueueOffset(
                putMessageResult.getAppendMessageResult().getLogicsOffset());
            msgExt.setCommitLogOffset(
                putMessageResult.getAppendMessageResult().getWroteOffset());
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.debug(
                "Send check message, the offset={} restored in queueOffset={} "
                    + "commitLogOffset={} "
                    + "newMsgId={} realMsgId={} topic={}",
                offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                msgExt.getTopic());
            return true;
        } else {
            log.error(
                "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                    + "msgId: {}",
                msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
        AbstractTransactionalMessageCheckListener listener) {
        try {
            // RMQ_SYS_TRANS_HALF_TOPIC：prepare消息的主题，事务消息首先进入该主题
            // RMQ_SYS_TRANS_OP_HALF_TOPIC：当消息服务器收到事务消息的提交或回滚
            // 请求后，会将消息存储在该主题下
            String topic = MixAll.RMQ_SYS_TRANS_HALF_TOPIC;
            // 获取所有事务消息队列
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.debug("Check topic={}, queues={}", topic, msgQueues);
            for (MessageQueue messageQueue : msgQueues) {
                long startTime = System.currentTimeMillis();
                // 拿到对应opQueue
                MessageQueue opQueue = getOpQueue(messageQueue);
                // 获取prepare消息的起始偏移量，这个偏移量起始是存储config/consumerOffset.json中
                // 基本格式就是"offsetTable":{"RMQ_SYS_TRANS_HALF_TOPIC@CID_RMQ_SYS_TRANS":{0:9}}
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);
                // 已处理消息，这里是去获取已处理消息的offset
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                        halfOffset, opOffset);
                    continue;
                }

                List<Long> doneOpOffset = new ArrayList<>();
                HashMap<Long, Long> removeMap = new HashMap<>();
                // 根据当前进度依次从已处理队列拉取32条消息，方便判断当前处理的消息是否已经处理过
                // 如果处理过则无须再次发送事务状态回查请求
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                if (null == pullResult) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                        messageQueue, halfOffset, opOffset);
                    continue;
                }
                // single thread
                int getMessageNullCount = 1;
                long newOffset = halfOffset;
                long i = halfOffset;
                while (true) {
                    // 事务消息状态回查，每次不超过一分钟
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    // 已经处理过，跳过
                    if (removeMap.containsKey(i)) {
                        log.info("Half offset {} has been committed/rolled back", i);
                        removeMap.remove(i);
                    } else {
                        GetResult getResult = getHalfMsg(messageQueue, i);
                        MessageExt msgExt = getResult.getMsg();
                        if (msgExt == null) {
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                    messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                    i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }
                        // 超过15次丢弃,或者消息过期来（超过来设置的文件保存时间:3天）
                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            listener.resolveDiscardMsg(msgExt);
                            newOffset = i + 1;
                            i++;
                            continue;
                        }
                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.debug("Fresh stored. the miss offset={}, check it later, store={}", i,
                                new Date(msgExt.getStoreTimestamp()));
                            break;
                        }

                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        long checkImmunityTime = transactionTimeout;
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        if (null != checkImmunityTimeStr) {
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt)) {
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                    checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                break;
                            }
                        }
                        List<MessageExt> opMsg = pullResult.getMsgFoundList();
                        // 判断是否超过了transactionTimeout
                        boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)
                            || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout))
                            || (valueOfCurrentMinusBorn <= -1);

                        if (isNeedCheck) {
                            // 这里就是将消息重新拷贝一份追加到MappedFile末尾
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            // 发送检查事务状态的请求
                            listener.resolveHalfMsg(msgExt);
                        } else {
                            pullResult = fillOpRemoveMap(removeMap, opQueue, pullResult.getNextBeginOffset(), halfOffset, doneOpOffset);
                            log.info("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}", i,
                                messageQueue, pullResult);
                            continue;
                        }
                    }
                    newOffset = i + 1;
                    i++;
                }
                if (newOffset != halfOffset) {
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Check error", e);
        }
    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * Read op message, parse op message, and fill removeMap
     *
     * @param removeMap Half message to be remove, key:halfOffset, value: opOffset.
     * @param opQueue Op message queue.
     * @param pullOffsetOfOp The begin offset of op message queue.
     * @param miniOffset The current minimum offset of half message queue.
     * @param doneOpOffset Stored op messages that have been processed.
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap,
        MessageQueue opQueue, long pullOffsetOfOp, long miniOffset, List<Long> doneOpOffset) {
        // 拉取32条消息
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, 32);
        if (null == pullResult) {
            return null;
        }
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
            || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            return pullResult;
        }
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }
        for (MessageExt opMessageExt : opMsg) {
            // 这里取出来的是half queue offset
            Long queueOffset = getLong(new String(opMessageExt.getBody(), TransactionalMessageUtil.charset));
            log.info("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffset);
            // 已打标的消息，都是要删除的半消息
            if (TransactionalMessageUtil.REMOVETAG.equals(opMessageExt.getTags())) {
                // 如果当前half offset小于最小的half offset，那么表明此消息已处理完毕，防止重复处理
                // 处理完待删除的op消息
                if (queueOffset < miniOffset) {
                    doneOpOffset.add(opMessageExt.getQueueOffset());
                } else {
                    // 否则存入removeMap,key:halfQueueOffset,value:opQueueOffset
                    // 消息已处理过
                    removeMap.put(queueOffset, opMessageExt.getQueueOffset());
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }
        }
        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset,
        MessageExt msgExt) {
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null == prepareQueueOffsetStr) {
            return putImmunityMsgBackToHalfQueue(msgExt);
        } else {
            long prepareQueueOffset = getLong(prepareQueueOffsetStr);
            if (-1 == prepareQueueOffset) {
                return false;
            } else {
                if (removeMap.containsKey(prepareQueueOffset)) {
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    doneOpOffset.add(tmpOpOffset);
                    return true;
                } else {
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq Target Message Queue
     * @param offset Offset in the message queue
     * @param nums Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.valueOf(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.valueOf(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        // 第一次检查需要创建一个opQueue
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                messageQueue.getQueueId());
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        getResult.setPullResult(result);
        List<MessageExt> messageExts = result.getMsgFoundList();
        if (messageExts == null) {
            return getResult;
        }
        getResult.setMsg(messageExts.get(0));
        return getResult;
    }

    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    @Override
    public boolean deletePrepareMessage(MessageExt msgExt) {
        if (this.transactionalMessageBridge.putOpMessage(msgExt, TransactionalMessageUtil.REMOVETAG)) {
            log.info("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

}
