package com.netflix.contentservice.service;

import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.netflix.contentservice.model.VideoStatus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class VideoUploadedEncodedEventConsumer {
private final ContentService contentService;
	
	@KafkaListener(
			topics="video.uploaded"
			)
	
	public void consumerVideoUploadedEvent(@Payload Map<String,Object> payload) {
		String movieId = payload.get("movieId").toString();
		String videoKey = payload.get("videoKey").toString();
		log.info("Video uploaded for movie: {} key : {}",movieId,videoKey);
		contentService.updateVideoKey(movieId, videoKey);
		
	}
	
	
	@KafkaListener(
		topics = "video.encoded"	
			)
	public void consumeVideoEncodedEvent(@Payload Map<String,Object> payload) {
		String movieId = payload.get("movieId").toString();
		String hlsUrl = payload.get("hlsUrl").toString();   // hlsurl is path of video in s3,here we wont use hlsurl to get video or to get presignhes url we use key and buketname to get the video details to stream..
		boolean success = (Boolean) payload.get("success");
		if(success) {
			contentService.updateHlsUrl(movieId, hlsUrl);
		}else {
			String errorMessage = payload.get("errorMessage").toString();
			contentService.updateVideoStatus(movieId, VideoStatus.FAILED);
		}
	}
}



