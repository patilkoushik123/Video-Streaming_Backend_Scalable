package com.netflix.videoservice.service;

import java.io.IOException;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.netflix.videoservice.event.VideoUploadedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;




@Service
@Slf4j
@RequiredArgsConstructor
public class VideoService {
	
	private final S3Client s3Client;
	
	private final KafkaTemplate<String,VideoUploadedEvent> kafkaTemplate;
	
	@Value("${aws.s3.bucket-name}")
	private String bucketName;
	
	private static final String VIDEO_UPLOADED_TOPIC="video.uploaded";

	public String uploadMovie(String movieId, MultipartFile file) throws IOException{
		// TODO Auto-generated method stub
	     log.info("Starting video upload for movie : {} file: {}",movieId,file.getOriginalFilename());
	     
	     
	     String videoKey = "raw/"+movieId+"/"+ UUID.randomUUID()+"_"+ file.getOriginalFilename();
	     
	     PutObjectRequest putObjectRequest = PutObjectRequest.builder()
	    		 .bucket(bucketName)
	    		 .key(videoKey)
	    		 .contentType(file.getContentType())
	             .contentLength(file.getSize())
	             .build();
	             
	             s3Client.putObject(putObjectRequest,RequestBody.fromInputStream(file.getInputStream(),file.getSize()));
	       log.info("video is uploaded to s3 succesfully ,key {}",videoKey); 
	             
	             //publish event to kafka
	             //encoding service will consume this and start FFmpeg processing.
	       
	       VideoUploadedEvent event = new VideoUploadedEvent(
	    	        bucketName,
	    	        videoKey,
	    	        movieId,
	    	        file.getOriginalFilename(),
	    	        file.getSize()
	    	);
	       
	       kafkaTemplate.send(VIDEO_UPLOADED_TOPIC,movieId,event);
	    		 log.info("VideoUploadedEvent published for movie: {}",movieId);
	    		 
	    		 return videoKey;
	    		   
	}

}
