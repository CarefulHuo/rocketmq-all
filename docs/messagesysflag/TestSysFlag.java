package carefulhuo.cf.test.rocketmq.messagesysflag;

public class TestSysFlag {
    public static void main(String[] args) {
        int sysFlag = 0;
        // 消息压缩标识
        System.out.println(MessageSysFlag.COMPRESSED_FLAG);
        sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
        System.out.println("sysFlag：" + sysFlag);

        // 事务消息标识
        System.out.println(MessageSysFlag.TRANSACTION_PREPARED_TYPE);
        sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
        System.out.println("sysFlag：" + sysFlag);

        System.out.println(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
        System.out.println(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
    }
}
