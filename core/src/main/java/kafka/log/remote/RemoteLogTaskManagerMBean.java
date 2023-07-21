package kafka.log.remote;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteLogTaskManagerMBean implements RemoteLogTaskManager {
    static final Logger LOG = LoggerFactory.getLogger(RemoteLogTaskManagerMBean.class);

    RemoteLogManager remoteLogManager;

    void init(RemoteLogManager remoteLogManager) {
        this.remoteLogManager = remoteLogManager;
    }

    public void pauseTasks(String topicName) {
        LOG.info("Pausing RLM tasks for topic: [{}]", topicName);
        if (remoteLogManager != null) {
            remoteLogManager.leaderOrFollowerTasks
                    .entrySet()
                    .stream()
                    .filter(e -> e.getKey().topicPartition().topic().equals(topicName))
                    .forEach(e -> {
                        LOG.info("Pausing RLM task: [{}]", e.getKey());
                        e.getValue().pause();
                    });
        }
    }

    public void pauseTasks(String topicName, int partition) {
        LOG.info("Pausing RLM tasks for topic partition: [{}:{}]", topicName, partition);
        if (remoteLogManager != null) {
            remoteLogManager.leaderOrFollowerTasks
                    .entrySet()
                    .stream()
                    .filter(e -> e.getKey().topicPartition().topic().equals(topicName) &&
                            e.getKey().topicPartition().partition() == partition)
                    .forEach(e -> {
                        LOG.info("Pausing RLM task: [{}]", e.getKey());
                        e.getValue().pause();
                    });
        }
    }

    public void pauseTasks() {
        LOG.info("Pausing all RLM tasks");
        if (remoteLogManager != null) {
            remoteLogManager.leaderOrFollowerTasks
                    .forEach((topicIdPartition, rlmTaskWithFuture) -> {
                        LOG.info("Pausing RLM task: [{}]", topicIdPartition);
                        rlmTaskWithFuture.pause();
                    });
        }
    }

    public void resumeTasks(String topicName) {
        LOG.info("Pausing RLM tasks for topic: [{}]", topicName);
        if (remoteLogManager != null) {
            remoteLogManager.leaderOrFollowerTasks
                    .entrySet()
                    .stream()
                    .filter(e -> e.getKey().topicPartition().topic().equals(topicName))
                    .forEach(e -> {
                        LOG.info("Resuming RLM task: [{}]", e.getKey());
                        e.getValue().pause();
                    });
        }
    }

    public void resumeTasks(String topicName, int partition) {
        LOG.info("Resuming RLM tasks for topic partition: [{}:{}]", topicName, partition);
        if (remoteLogManager != null) {
            remoteLogManager.leaderOrFollowerTasks
                    .entrySet()
                    .stream()
                    .filter(e -> e.getKey().topicPartition().topic().equals(topicName) &&
                            e.getKey().topicPartition().partition() == partition)
                    .forEach(e -> {
                        LOG.info("Resuming RLM task: [{}]", e.getKey());
                        e.getValue().resume();
                    });
        }
    }

    public void resumeTasks() {
        LOG.info("Resuming all RLM tasks");
        if (remoteLogManager != null) {
            remoteLogManager.leaderOrFollowerTasks
                    .forEach((topicIdPartition, rlmTaskWithFuture) -> {
                        LOG.info("Resuming RLM task: [{}]", topicIdPartition);
                        rlmTaskWithFuture.resume();
                    });
        }
    }
}
