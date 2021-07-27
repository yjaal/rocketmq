package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 发送同步消息
 *
 * @author YJ
 * @date 2021/7/21
 **/
public class SyncProducer {

    public static void main(String[] args) throws Exception {
        // 实例化消息生产者Producer，这里需要指定一个分组
        DefaultMQProducer producer = new DefaultMQProducer("SyncProducerGroup");
        // 设置NameServer的地址
        producer.setNamesrvAddr("192.168.67.2:9876");
        // 启动Producer实例
        producer.start();

        for (int i = 0; i < 100; i++) {
            // 创建消息，并指定Topic，Tag和消息体(消息体会转化为二进制)
            Message msg = new Message("TopicTest", "TagA",
                ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

            // 发送消息到一个Broker
            SendResult sendResult = producer.send(msg);
            // 通过sendResult返回消息是否成功送达
            System.out.printf("%s%n", sendResult);
        }
        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
