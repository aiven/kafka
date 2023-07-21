package kafka.log.remote;

public interface RemoteLogTaskManager {
    void pauseTasks(String topicName);
    void pauseTasks(String topicName, int partition);
    void pauseTasks();
    void resumeTasks(String topicName);
    void resumeTasks(String topicName, int partition);
    void resumeTasks();
}
