package consumer_read;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @FileName: ReadMessageSimple.java
 * @Package: consumer_read
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2019年3月27日 下午6:11:02
 */

public class ReadMessageSimple {
	public static void main(String[] args) {
		// Properties 对象
		Properties props = new Properties();
		props.put("bootstrap.servers", "slave1:9092,slave2:9092,slave3:9092");
		props.put("group.id", "CountryCounter");	// 消费者群组
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		// consumer 对象
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		// 订阅主题
		consumer.subscribe(Collections.singleton("CustomerCountry"));	// 支持订阅多个主题，也支持正则
		
		try {
			while (true) {
				// 0.1s 的轮询等待
				@SuppressWarnings("deprecation")
				ConsumerRecords<String, String> records = consumer.poll(1000);
				System.out.println(records.count());
				for (ConsumerRecord<String, String> record : records) {
					// 输出到控制台
					System.out.printf("topic = %s, partition = %s, offset = %sd, key = %s, value = %s\n", 
							record.topic(), record.partition(), record.offset(), record.key(), record.value());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}





