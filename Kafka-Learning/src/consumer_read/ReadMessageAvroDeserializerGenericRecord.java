package consumer_read;

import java.util.Collections;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @FileName: ReadMessageAvroDeserializerGenericRecord.java
 * @Package: consumer_read
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2019年3月27日 下午6:11:02
 */

public class ReadMessageAvroDeserializerGenericRecord {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		// Properties 对象
		Properties props = new Properties();
		props.put("bootstrap.servers", "slave1:9092,slave2:9092,slave3:9092");
		props.put("group.id", "AvroTest");	// 消费者群组
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		props.put("schema.registry.url", "http://master:8081");
		
		// consumer 对象
		KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
		
		// 订阅主题
		consumer.subscribe(Collections.singletonList("AvroTest"));	// 支持订阅多个主题，也支持正则
		
		try {
			// 设置分区开头读取, 0表示立立即返回，无需等待
			consumer.seekToBeginning(consumer.poll(0).partitions());
			while (true) {
				// 0.1s 的轮询等待
				ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
				System.out.println(records.count());
				for (ConsumerRecord<String, GenericRecord> record : records) {
					// 输出到控制台
					System.out.printf("name = %sd, email = %s\n", 
							record.value().get("name"), record.value().get("email"));
				}
				// 同步提交偏移量
				consumer.commitSync();
				Thread.sleep(500);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}





