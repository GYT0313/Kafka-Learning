package producer_write;


import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * @FileName: SendMessageSimple.java
 * @Package: producer_write
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2019年3月26日 上午9:03:32
 */

public class SendMessageSimple {
	public static void main(String[] args) {
		// 创建生产者
		Properties kafkaProps = new Properties();
		// 指定broker
		kafkaProps.put("bootstrap.servers", "slave1:9092,slave2:9092,slave3:9092");
		kafkaProps.put("group.id", "CountryCounter");	// 消费者群组
		// 设置序列化
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// 实例化出producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
		
		// 创建ProducerRecord对象
		ProducerRecord<String, String>  record = new ProducerRecord<String, String>("CustomerCountry",
				"Hello", "Kafka");
		try {
			// 发送消息
			producer.send(record);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}
}





