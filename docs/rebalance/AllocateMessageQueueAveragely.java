package carefulhuo.rocketmq.rebalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 队列分配策略 -- 平均分配，这个是默认的分配策略
 * 如果队列数和消费者数量相除有余数时，余数按照顺序 1个 1个 分配消费者
 * 比如：8个队列 q1,q2,q3,q4,q5,q6,q7,q8 消费者3个：c1,c2,c3
 * 分配如下：
 * c1:q1,q2,q3
 * c2:q4,q5,q6
 * c3:q7,q8
 * todo 特别声明
 *  平均分配算法，类似于分页的算法，将所有MessageQueue排好序类似于记录，将所有消费端Consumer排好序类似页数，并求出每一页需要包含的平均size和每个页面记录的范围range，
 *  最后遍历整个range而计算出当前Consumer端应该分配到的记录（这里即为：MessageQueue）。
 *  平均分配方式：平均分配队列，每个消费者平均分配队列，如果队列数不能被消费者数整除，则余数队列分配给最后一个消费者
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely {

    public static void main(String[] args) {
//        List<Integer> mqAll = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);
        List<Integer> mqAll = Arrays.asList(1, 2);
//        List<String> cidAll = Arrays.asList("c1", "c2", "c3");
        List<String> cidAll = Arrays.asList("c1", "c2", "c3");
        String consumerGroup = "cg";
        String currentCID = "c3";

        List<Integer> result = allocate(consumerGroup, currentCID, mqAll, cidAll);
        System.out.println(result);
    }


    /**
     * 平均分配队列策略
     *
     * @param consumerGroup 消费组
     * @param currentCID    要分配队列的消费者id
     * @param mqAll         Topic 下的队列
     * @param cidAll        消费者集合
     * @return
     */
    public static List<Integer> allocate(String consumerGroup, String currentCID, List<Integer> mqAll,
                                       List<String> cidAll) {
        // 检验参数是否正确
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        // 分配后的消息队列
        List<Integer> result = new ArrayList<Integer>();
        if (!cidAll.contains(currentCID)) {
            return result;
        }

        // 平均分配队列

        // 当前 Consumer 在消费集群中是第几个 consumer
        // 这里就是为什么需要对传入的 cidAll 参数必须进行排序的原因。如果不排序，Consumer 在本地计算出来的 index 无法一致，影响计算结果。
        int index = cidAll.indexOf(currentCID);


        // 余数，即多少消息队列无法平均分配
        int mod = mqAll.size() % cidAll.size();

        // 队列总数 <= 消费者总数时，分配当前消费则1个队列
        // 不能均分 && 当前消费者序号（从 0 开始） < 余下的队列数(mod)，分配当前消费者 mqAll /cidAll + 1 个队列
        // 不能均分 && 当前消费者序号（从 0 开始） >= 余下的队列数(mod)，分配当前消费者 mqAll/cidAll 个队列
        int averageSize =
                mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                        + 1 : mqAll.size() / cidAll.size());

        // 有余数的情况下，[0, mod) 平分余数，即每consumer多分配一个节点；第index开始，跳过前mod余数
        // Consumer 分配消息队列开始位置
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;

        // 分配队列数量。之所以要Math.min()的原因是：mqAll.size() <= cidAll.size()，是为了让部分consumer分配不到消息队列
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }

        // 分配消息队列结果
        return result;
    }
}
