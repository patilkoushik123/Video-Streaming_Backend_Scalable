package com.netflix.encodingservice.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.netflix.encodingservice.service.event.VideoUploadedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class VideoEventConsumer {
	
	private final EncodingService encodingService;
	/*
	 * Listens to video.uploaded kafka topic
	 * Triggered when video service uploads a raw video to s3
	 * flow:
	 *   Video service -> s3 upload -> kafka(video.uploaded)
	 *                              -> This Consumer
	 *                              -> EncodingService -> FFmpeg -> s3
	 *                              -> Kafka (video.encoded)
	 * 
	 */
	
	@KafkaListener(
			topics="video.uploaded",
			groupId="encoding-service-group"
			// concurrency = 3
			// only mentioning partition is not enough,to make parellel processing concurrency also requires.
			
			)
	public void consumeVideoUploadedEvent(VideoUploadedEvent event) {
		log.info("Consumed VideoUploadedEvent for movie:{} file :{}",
				event.getMovieId(),event.getOriginalFileName());
		try {
			encodingService.encodeVideo(event);
		}
		catch(Exception e) {
			log.error("Failed to process encoding for movie: {}-{}",
					event.getMovieId(),e.getMessage());
		}
	}

}
