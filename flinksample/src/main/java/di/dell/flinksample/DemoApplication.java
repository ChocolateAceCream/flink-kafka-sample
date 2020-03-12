package di.dell.flinksample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Properties;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;



@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(DemoApplication.class, args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "PLAINTEXT://192.168.1.15:9092");
		properties.setProperty("group.id", "nb_con_1");

		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("flinkInput", new SimpleStringSchema(), properties);
		consumer.setStartFromEarliest();
        DataStream<String> stream = env
				.addSource(consumer);
		stream
		//filter function, filter out all number strings. 
		.filter(name -> {
			if (name.matches("[0-9]+")) {
				// System.out.println(name);
				return true;
			}
			return false;
			
		//turn datastream into a sliding window with width=5 and sliding frequency =3
		}).countWindowAll(5,3)

		//reduce function
		// .reduce(new ReduceFunction<String>(){
		// 	@Override
		// 	public String reduce(String value1, String value2) throws Exception {
		// 		// TODO Auto-generated method stub
		// 		return String.valueOf(Integer.parseInt(value1)+Integer.parseInt(value2));
		// 	}
		// })
		.print();


        //stream.map();

		// FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("flinkOutput",new SimpleStringSchema(),properties);
		// producer.setWriteTimestampToKafka(true);

		// //now all stream data were send to the kafka flinkOutput topic
		// stream.addSink(producer);
		
        env.execute();
	}

}
