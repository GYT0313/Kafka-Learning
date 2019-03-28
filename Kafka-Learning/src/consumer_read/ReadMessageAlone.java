package consumer_read;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


/**
 * @FileName: ReadMessageAlone.java
 * @Package: consumer_read
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2019年3月27日 下午6:11:02
 */

public class ReadMessageAlone {
	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) {
		// Properties 对象
		Properties props = new Properties();
		props.put("bootstrap.servers", "slave1:9092,slave2:9092,slave3:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		// consumer 对象
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		// 自己给自己分配分区
		List<PartitionInfo> partitionInfos = null;
		// 向主题请求所有可用的分区, 如果指定特定的分区跳过
		partitionInfos = consumer.partitionsFor("CustomerCountry");
		// 知道那些可用的分区后，调用assign()
		List<TopicPartition> partitions = new ArrayList<>();
		if (partitionInfos != null) {
			for (PartitionInfo partition : partitionInfos) {
				partitions.add(new TopicPartition(partition.topic(), partition.partition()));
			}
			consumer.assign(partitions);
			// 设置分区开头读取, 0表示立立即返回，无需等待
			consumer.seekToBeginning(consumer.poll(0).partitions());
			try {
				while (true) {
					// 0.1s 的轮询等待
					ConsumerRecords<String, String> records = consumer.poll(1000);
					System.out.println(records.count());
					for (ConsumerRecord<String, String> record : records) {
						// 输出到控制台
						System.out.printf("topic = %s, partition = %s, offset = %sd, key = %s, value = %s\n", 
								record.topic(), record.partition(), record.offset(), record.key(), record.value());
					}
					Thread.sleep(500);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				consumer.close();
			}
		}
	}
}





