package carefulhuo.rocketmq.rebalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 根据 Broker 部署机房名，对每个消费者负载不同的 Broker 上的队列
 * 即，根据指定的 Broker 名，从队列中选出属于这些 Broker 的队列 平均分配给消费者
 * Computer room Hashing queue algorithm, such as Alipay logic room
 *
 * 每个消费者平均分配队列，如果队列数不能被消费者数整除，则是多余的结尾部分，分配给前 mqAll.size % cidAll.size() 个 Consumer
 * 举例：有 8个消息队列 q1, q2, q3, q4, q5, q6, q7, q8，3个消费者 c1, c2, c3，
 * 分配到每个消费者的队列如下：
 * c1:q1,q2,q7
 * c2:q3,q4,q8
 * c3:q5,q6
 */
public class AllocateMessageQueueByMachineRoom {

    public static void main(String[] args) {
                 List<Integer> mqAll = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
//        List<Integer> mqAll = Arrays.asList(1, 2);
//        List<String> cidAll = Arrays.asList("c1", "c2", "c3");
        List<String> cidAll = Arrays.asList("c1", "c2", "c3");
        String consumerGroup = "cg";
        String currentCID = "c2";

        List<Integer> result = allocate(consumerGroup, currentCID, mqAll, cidAll);
        System.out.println(result);
    }


    public static List<Integer> allocate(String consumerGroup, String currentCID, List<Integer> mqAll,
                                       List<String> cidAll) {
        List<Integer> result = new ArrayList<Integer>();
        int currentIndex = cidAll.indexOf(currentCID);
        if (currentIndex < 0) {
            return result;
        }

        // 计算可消费的 Broker 对应的消息队列，即当前配置的消费者数组('consumeidcs')对应的消息队列
        List<Integer> premqAll = new ArrayList<Integer>();
        premqAll.addAll(mqAll);
//        for (Integer mq : mqAll) {
//            // 获取消息队列对应的 Broker
//            String[] temp = mq.getBrokerName().split("@");
//            // 如果消息队列对应的 Broker 在消费者消费 BrokerName 集合中，则添加到 premAll 集合中
//            if (temp.length == 2 && consumeridcs.contains(temp[0])) {
//                premqAll.add(mq);
//            }
//        }

        // 平均分配方式：平均分配队列，每个消费者平均分配队列，如果队列数不能被消费者数整除，则余数队列分配给最后一个消费者
        // 此处也是平均分配，但该平均分配方式与 AllocateMessageQueueAveragely 略有不同，是将多余的结尾部分分配给前 rem 个 Consumer
        int mod = premqAll.size() / cidAll.size(); // 取整
        int rem = premqAll.size() % cidAll.size(); // 余数
        int startIndex = mod * currentIndex;
        int endIndex = startIndex + mod;
        for (int i = startIndex; i < endIndex; i++) {
            result.add(premqAll.get(i));
        }
        // 将多余的结尾部分分配给前 rem 个 Consumer
        if (rem > currentIndex) {
            result.add(premqAll.get(currentIndex + mod * cidAll.size()));
        }
        return result;
    }
}
