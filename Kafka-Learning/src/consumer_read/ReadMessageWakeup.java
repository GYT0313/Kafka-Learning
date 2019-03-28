package consumer_read;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

/**
 * @FileName: ReadMessageWakeup.java
 * @Package: consumer_read
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2019年3月27日 下午6:11:02
 */

public class ReadMessageWakeup {
	// 属性
	public static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
	public static KafkaConsumer<String, String> consumer;
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		// Properties 对象
		Properties props = new Properties();
		props.put("bootstrap.servers", "slave1:9092,slave2:9092,slave3:9092");
		props.put("group.id", "CountryCounter");	// 消费者群组
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		// consumer 对象
		consumer = new KafkaConsumer<>(props);
		
		// 该类对象
		ReadMessageWakeup rebalanceListener = new ReadMessageWakeup();
		// 订阅主题, 并设置再均衡监听器
		consumer.subscribe(Collections.singletonList("CustomerCountry"), 
				rebalanceListener.new HandleRebalance());	// 支持订阅多个主题，也支持正则

		try {
			// 设置分区开头读取, 0表示立立即返回，无需等待
			//consumer.seekToBeginning(consumer.poll(0).partitions());
			int count = 0;
			while (true) {
				// 0.1s 的轮询等待
				ConsumerRecords<String, String> records = consumer.poll(100);
				System.out.println(records.count());
				for (ConsumerRecord<String, String> record : records) {
					// 输出到控制台
					System.out.printf("topic = %s, partition = %s, offset = %sd, key = %s, value = %s\n", 
							record.topic(), record.partition(), record.offset(), record.key(), record.value());
					count++;
					// 指定提交特定的偏移量
					currentOffsets.put(new TopicPartition(record.topic(), record.partition()), 
							new OffsetAndMetadata(record.offset()+1, "no metadata"));
				}
				consumer.commitAsync(currentOffsets, null);
				Thread.sleep(500);
				// 读取3000条消息后退出
				if (count == 3000) {
					consumer.wakeup();
				}
			}
		} catch (WakeupException e) {
			// do nothing
			System.out.println("exit...");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
	
	// API
	private class HandleRebalance implements ConsumerRebalanceListener {

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			System.out.println("Before consumer...");
		}

		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			System.out.println("Lost partitions in rebalance. Committing current "
					+ "offsets: " + currentOffsets);
			// 同步提交偏移量
			consumer.commitSync(currentOffsets);
		}
	
	}
	
}





