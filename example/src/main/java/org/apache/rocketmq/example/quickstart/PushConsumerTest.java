package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class PushConsumerTest {
    public static void main(String[] args) {
        // 初始化consumer，并设置consumer group name
        DefaultMQPushConsumer consumerOne = new DefaultMQPushConsumer("source_code_reading_group");
        // DefaultMQPushConsumer consumerTwo = new DefaultMQPushConsumer("source_code_reading_group");

        // 设置NameServer地址
        consumerOne.setNamesrvAddr("127.0.0.1:9876");
        // consumerTwo.setNamesrvAddr("127.0.0.1:9876");
        try {
            //订阅一个或多个topic，并指定tag过滤条件，这里指定*表示接收所有tag的消息
            consumerOne.subscribe("source_code_reading", "TagA");
            consumerOne.setMessageModel(MessageModel.CLUSTERING);
            consumerOne.setMaxReconsumeTimes(2);
            //注册回调接口来处理从Broker中收到的消息
            consumerOne.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    System.out.printf("%s Receive New Messages: %s %s %n", Thread.currentThread().getName(), msgs, new String(msgs.get(0).getBody(), StandardCharsets.UTF_8));
                    // 返回消息消费状态，ConsumeConcurrentlyStatus.CONSUME_SUCCESS为消费成功
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            // //订阅一个或多个topic，并指定tag过滤条件，这里指定*表示接收所有tag的消息
            // consumerTwo.subscribe("source_code_reading", "TagA");
            // consumerTwo.setMessageModel(MessageModel.CLUSTERING);
            // consumerTwo.setMaxReconsumeTimes(2);
            // //注册回调接口来处理从Broker中收到的消息
            // consumerTwo.registerMessageListener(new MessageListenerConcurrently() {
            //     @Override
            //     public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            //         System.out.printf("%s Receive New Messages: %s %s %n", Thread.currentThread().getName(), msgs, new String(msgs.get(0).getBody(), StandardCharsets.UTF_8));
            //         // 返回消息消费状态，ConsumeConcurrentlyStatus.CONSUME_SUCCESS为消费成功
            //         return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            //     }
            // });
            // 启动Consumer
            consumerOne.start();
            // consumerTwo.start();
            System.out.printf("consumerOne Started.%n");
            System.out.printf("consumerTwo Started.%n");
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
