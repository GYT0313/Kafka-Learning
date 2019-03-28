package producer_write;


import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * @FileName: SendMessageCallback.java
 * @Package: producer_write
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2019年3月26日 上午9:03:32
 */

public class SendMessageCallback {
	public static void main(String[] args) {
		// 创建生产者
		Properties kafkaProps = new Properties();
		// 指定broker
		kafkaProps.put("bootstrap.servers", "slave1:9092,slave2:9092");
		// 设置序列化
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// 实例化出producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
		
		// 创建ProducerRecord对象
		ProducerRecord<String, String>  record = new ProducerRecord<String, String>("CustomerCountry",
				"Hello", "Kafka-Callback");
		try {
			// 发送消息
			SendMessageCallback sMCallback = new SendMessageCallback();
			producer.send(record, sMCallback.new DemoProducerCallback());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}
	private class DemoProducerCallback implements Callback {

		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception e) {
			// TODO Auto-generated method stub
			if (e != null) {
				// 如果消息发送失败，打印异常
				e.printStackTrace();
			} else {
				System.out.println("成功发送消息！");
			}
		}

	}
}





