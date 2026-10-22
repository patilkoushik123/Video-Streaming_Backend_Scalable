package com.netflix.videoservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

	//published when video is uploaded to s3
	//encoding service will consume this
	@Bean
	public NewTopic videoUploadedTopic() {
		return TopicBuilder.name("video.uploaded")
				.partitions(3)
				.replicas(1)
				.build();        // .partitions(3)  creates the topic with three partitions.

//Think of partitions as separate logs into which Kafka distributes messages.
		
		/*Why three partitions?
				- Multiple consumers in the same consumer group can process messages in parallel.
				- Message throughput can increase.
				- Kafka can scale better as video uploads grow*/
		
		
		// concept of replicas
		/* 1. Imagine you have one video
Your Content Service uploads a video and publishes an event to Kafka:

video.uploaded

Suppose Kafka stores that event in Partition 0.

What happens if the Kafka broker storing that partition crashes? If there's no other copy, Kafka may be unable to serve that partition until it's recovered.

That's why we use replicas — copies of the data stored in Kafka partitions.

The other copies are of the same partition, but stored on different Kafka brokers. They are not stored in different partitions

 */
	}
	
	//published when encoding is complete
	//s3 will consume or streaming service will consume
	@Bean
	public NewTopic videoEncodedTopic() {
		  return TopicBuilder.name("video.encoded")
				  .partitions(3)
				  .replicas(1)
				  .build();
				  
	}
}
