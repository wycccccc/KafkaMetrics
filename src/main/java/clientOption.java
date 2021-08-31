import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class clientOption {
    public static void createTopic () {
        String brokerList = "kafka01:9092";
        String topic = "topic-create";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);
        //ensure partitions number and replication factor
        NewTopic newTopic = new NewTopic(topic, 3, (short) 0);
        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
        try{
            result.all().get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        client.close();
    }

    public static void describeTopicConfig () throws ExecutionException, InterruptedException {
        String brokerList = "kafka01:9092";
        String topic = "topic-create";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        DescribeConfigsResult result = client.describeConfigs(Collections.singleton(resource));
        Config config = result.all().get().get(resource);
        System.out.println(config);
        client.close();
    }
    public static void alterTopicConfig () throws ExecutionException, InterruptedException {
        String brokerList = "kafka01:9092";
        String topic = "topic-create";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        //alterConfig
        ConfigEntry entry = new ConfigEntry("cleanup.policy", "delete");
        AlterConfigOp alterConfigOp = new AlterConfigOp(entry, AlterConfigOp.OpType.SET);
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
        configs.put(resource, Collections.singleton(alterConfigOp));
        AlterConfigsResult result = client.incrementalAlterConfigs(configs);
        result.all().get();

    }


    public static void alterTopicPartition () throws ExecutionException, InterruptedException {
        String brokerList = "kafka01:9092";
        String topic = "topic-create";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        AdminClient client = AdminClient.create(props);
        //alterTopicPartition
        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put(topic, newPartitions);
        CreatePartitionsResult result = client.createPartitions(newPartitionsMap);
        result.all().get();
        client.close();
    }

}
