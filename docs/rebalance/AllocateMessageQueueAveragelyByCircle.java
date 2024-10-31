package carefulhuo.rocketmq.rebalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 环型分配 -- 平均轮询分配
 * 举例：8个队列 q1,q2,q3,q4,q5,q6,q7,q8 ，消费者3个：c1,c2,c3
 * 分配如下：
 * c1:q1,q4,q7
 * c2:q2,q5,q8
 * c3:q3,q6
 *
 * Cycle average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragelyByCircle {

    public static void main(String[] args) {
//         List<Integer> mqAll = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        List<Integer> mqAll = Arrays.asList(1, 2);
         List<String> cidAll = Arrays.asList("c1", "c2", "c3");
//        List<String> cidAll = Arrays.asList("c1", "c2", "c3");
        String consumerGroup = "cg";
        String currentCID = "c2";

        List<Integer> result = allocate(consumerGroup, currentCID, mqAll, cidAll);
        System.out.println(result);
    }

    /**
     * 平均轮询分配
     *
     * @param consumerGroup current consumer group 消费组
     * @param currentCID    current consumer id  消费者Id
     * @param mqAll         message queue set in current topic Topic下的消息队列
     * @param cidAll        consumer set in current consumer group 消费者集合
     * @return
     */
    public static List<Integer> allocate(String consumerGroup, String currentCID, List<Integer> mqAll,
                                       List<String> cidAll) {
        // 检查参数
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<Integer> result = new ArrayList<Integer>();
        if (!cidAll.contains(currentCID)) {
            return result;
        }

        // 获取当前消费者在消费者集合中的索引
        int index = cidAll.indexOf(currentCID);

        // 从 index 开始遍历 mqAll，如果 cidAll.size() > mqAll.size()，则直接返回空的消息队列集合
        for (int i = index; i < mqAll.size(); i++) {
            // 对满足 i % cidAll.size() 求余的结果 == index 的消息队列分配给消费者
            if (i % cidAll.size() == index) {
                result.add(mqAll.get(i));
            }
        }
        return result;
    }
}
