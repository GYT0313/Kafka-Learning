package producer_write;


import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


/**
 * @FileName: SendMessageAvro.java
 * @Package: producer_write
 * @Author: Gu Yongtao
 * @Description: [文件描述]
 *
 * @Date: 2019年3月26日 上午9:03:32
 */

public class SendMessageAvro {
	public static void main(String[] args) {
		// 创建生产者
		Properties kafkaProps = new Properties();
		// 指定broker
		kafkaProps.put("bootstrap.servers", "slave1:9092,slave2:9092");
		// 设置序列化
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		kafkaProps.put("schema.registry.url", "http://master:8081");
		
		// 因为没有使用Avro生成对象，所以需要提供Avro Schema
		String schemaStr = "{\"namespace\": \"customerManagement.avro\", \"type\": \"record\", " +
				 "\"name\": \"Customer\", " + 
				 "\"fields\": [{\"name\": \"id\", \"type\": \"int\"}, " + 
				 "{\"name\": \"name\",  \"type\": \"string\"}, " +
				 "{\"name\": \"email\", \"type\": \"string\", \"default\": \"null\"}]}";
		
		@SuppressWarnings("resource")
		Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(kafkaProps);
		
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(schemaStr);
		
		// 实例化SendMessageAvro
		SendMessageAvro sMCallback = new SendMessageAvro();
		// 发送多条消息
		for (int nCustomers = 0; nCustomers < 5000; nCustomers++) {
			String name = "exampleCustomer" + nCustomers;
			String email = "example" + nCustomers + "@example.com";
			
			// ProducerRecord的值就是一个GenericRecord对象，它包含了shcema和数据
			GenericRecord customer = new GenericData.Record(schema);
			customer.put("id", nCustomers);
			customer.put("name", name);
			customer.put("email", email);
			
			// 创建ProducerRecord对象
			ProducerRecord<String, GenericRecord> data = new ProducerRecord<String, 
					GenericRecord>("AvroTest", name, customer);
			
			try {
				// 发送消息
				producer.send(data, sMCallback.new DemoProducerCallback(nCustomers));
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private class DemoProducerCallback implements Callback {
		Integer id = null;
		public DemoProducerCallback(Integer id) {
			this.id = id;
		}

		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception e) {
			// TODO Auto-generated method stub
			if (e != null) {
				// 如果消息发送失败，打印异常
				e.printStackTrace();
			} else {
				System.out.println("Success send! Message ID: " + id);
			}
		}

	}
}





