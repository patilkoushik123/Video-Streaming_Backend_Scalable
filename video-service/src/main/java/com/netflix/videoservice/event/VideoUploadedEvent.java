package com.netflix.videoservice.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

//video from s3 to kafka
//encoding service consume thia to start FFmpeg streaming.



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
