package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class SyncProducerTest {
    public static void main(String[] args) {
        //（1） 初始化一个producer并设置Producer group name
        DefaultMQProducer producer = new DefaultMQProducer("source_code_reading_group");
        //（2）设置NameServer地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 启动producer
        try {
            producer.start();
            for (int i = 0; i < 2; i++) {
                // 创建一条消息，并指定topic、tag、body等信息，tag可以理解成标签，对消息进行再归类，RocketMQ可以在消费端对tag进行过滤
                Message msg = new Message("source_code_reading",
                        "TagA",
                        ("你好-hwy-" + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
                );
                //（3）利用producer进行发送，并同步等待发送结果
                SendResult sendResult = producer.send(msg);
                //（4）输出发送结果
                System.out.printf("%s%n", sendResult);
            }
        } catch (Exception e) {
            System.out.println(e);
        }
        // 一旦producer不再使用，关闭producer
        // producer.shutdown();
    }
}
