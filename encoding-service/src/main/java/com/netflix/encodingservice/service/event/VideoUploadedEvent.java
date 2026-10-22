package com.netflix.encodingservice.service.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

//consumed from kafka topic: video.uploaded.
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VideoUploadedEvent {
	private String bucketName;
	private String videoKey;
	private String movieId;
	private String originalFileName;
	private long fileSizeBytes;
}
