package com.netflix.encodingservice.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.netflix.encodingservice.service.event.VideoEncodedEvent;
import com.netflix.encodingservice.service.event.VideoUploadedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;



@Service
@Slf4j
@RequiredArgsConstructor
public class EncodingService {

	
	private final S3Client s3Client;
	
	private final KafkaTemplate<String,VideoEncodedEvent> kafkaTemplate;
	
	@Value("${aws.s3.bucket-name}")
	private String bucketName;
	
	@Value("${ffmpeg.path}")
    private String ffmpegPath;
	
	@Value("${encoding.base-path}")
	private String basePath;
	
	private static final String VIDEO_ENCODED_TOPIC ="video.encoded";
	
	// video qualities to encode.
	
	// format : resolution,bitrate,height
	
	// bitrate=how much data is processed per second
	 // higher bitrate = better for high visual quality
	// lower bitrate = less visual quality
	// k=kbps.
	// if internet is faster then our video player can easily downloads the 5000k (5m)bits per seconds to draw the picture and deliver.
	// if net is ,the 5m bits unable to download,so if there is no option other than 5m bitrate,then video will start buffering,to avoid that buffering we have another versions,based on net range,these versions will change,and may be get low quality picture than buffering.
	private static final List<int[]> VIDEO_QUALITIES = Arrays.asList(
		    new int[]{1920, 5000, 1080},  //1080p - 5000k bitrate
		    new int[]{1280, 2800, 720},  //720p-2800k bitrate
		    new int[]{854, 1200, 480},   //480p-1200k bitrate
		    new int[]{640, 800, 360}     //360p-800k bitrate
		);
	
	/*
	 video services uploaded movie to s3,
	 steps:
	 1.download raw video from s3.
	 2.encode to multiple qualities using FFmpeg.
	 3.generate HLS playlist(.m3u8) for each quality.
	 4.create master playlist.
	 5. upload all encoded files back to s3.
	 6.publis videoEncodedEvent to kafka.
	 
	 */
	
	
	  public void encodeVideo(VideoUploadedEvent event) {
		  log.info("Starting encoding platform for movie: {}",event.getMovieId());
		  
		  //create a unique path for a movie
		  String jobPath = basePath + "/" + event.getMovieId();
		  
		  try {
			  
			  // create temp directories
			  Files.createDirectories(Paths.get(jobPath));
			  Files.createDirectories(Paths.get(jobPath+"/encoded"));
			  
			  // step 1: Download raw video from s3
			  String localVideoPath = jobPath + "/raw_video.mp4";
			  downloadFromS3(event.getVideoKey(),localVideoPath);
			  log.info("Raw video Downloaded to: {}",localVideoPath);
			  //step2 and 3 = encode video in multiple videos and generate hsl
			  for(int [] qualities : VIDEO_QUALITIES) {
				  int width = qualities[0];
				  int bitrate = qualities[1];
				  int height = qualities[2];
				  
				  String qualityDir = jobPath + "/encoded/" + height + "p";
				  Files.createDirectories(Paths.get(qualityDir));
				  encodeToHLS(localVideoPath,qualityDir,width,height,bitrate);
				  log.info("Encoded {}p successfully", height);			  }
			  
			  //step 4 = generate master playlist
			  String masterPlaylistPath = jobPath + "/encoded/master.m3u8";
			  generateMasterPlaylist(masterPlaylistPath);
			  log.info("Master Playlist Generated");
			  
			  // step 5 = upload all resources file to s3
			  String encodedPrefix = "encoded/" + event.getMovieId() +"/";
			  uploadEncodedFilesToS3(jobPath + "/encoded",encodedPrefix);
			  log.info("All encoded files uploaded to s3");
			  
			  //step 6:publish videoEncodedEvent
			  String masterPlaylistKey = encodedPrefix+"master.m3u8";
			  String hlsUrl = "https://" + bucketName +".s3.amazonaws.com/"+masterPlaylistKey;
			  
			  VideoEncodedEvent encodedEvent = new VideoEncodedEvent(
					  event.getMovieId(),
					  hlsUrl,
					  masterPlaylistKey,
					  true,
					  null
					  
	
					  
					  );
			  
			  kafkaTemplate.send(VIDEO_ENCODED_TOPIC,event.getMovieId(),encodedEvent);
			  
			  log.info("videoEncodedEvent published for movie: {}",event.getMovieId());
					  
		  }
		  catch(Exception e){
			  log.error("encoding failed for movie: {} - {}",event.getMovieId(),e.getMessage());
			  
			  VideoEncodedEvent failureEvent = new VideoEncodedEvent(
					  event.getMovieId(),
					  null,
					  null,
					  false,
					  e.getMessage()
					  
					  );
					  
				kafkaTemplate.send(VIDEO_ENCODED_TOPIC,event.getMovieId(),failureEvent);
			 
		  }
		  finally {
			  cleanupTempFiles(jobPath);
		  }
		  
	  }
	  
	  // download file from s3 to local path
	  
	private void downloadFromS3(String s3key,String localPath) {    //ok ffmpeg needs to store the video in a file format thats why we need somehow the path to store the video from s3,and here java helps to convert localpath to file by using paths.get
		GetObjectRequest getObjectRequest = GetObjectRequest.builder()
				.bucket(bucketName)
				.key(s3key)    
				.build();
		
	     s3Client.getObject(getObjectRequest,Paths.get(localPath));
	}
	
	// encode video to hls using ffmpeg.
	/* ffmpeg command created:
	           - multilple .ts playlist files(10 seconds each) 
	            - A .m3u8 playlist file for this quality */
	
	private void encodeToHLS(String inputPath,String outputDir,int width,int height,int bitrate) 
	 throws IOException,InterruptedException{                                  // collects raw video file from c drive and perform encoding in the ffmpge format
		String playlistPath = outputDir + "/playlist.m3u8";  // 1080p/playlist.m3u8 etc..
		String segmentPattern = outputDir + "/segment_%03d.ts"; //                 //t is a free, open-source command-line tool and multimedia framework used to decode, encode, transcode, mux, demux, stream, filter, and play digital audio and video files.
		//segmentPattern tells FFmpeg exactly how to name and format the individual 10-second video chunk files it generates during HLS encoding.
		// FFmpeg Command for HLS encoding
		
		List<String> command = Arrays.asList(
				ffmpegPath,
				"-i", inputPath,                          // Input file
				"-vf", "scale=" + width + ":" + height,   // Scale to resolution
				"-c:v", "libx264",                        // video codec
				"-b:v", bitrate + "k",                    // video bitrate
				"-c:a", "aac",                            // Audio coded
				"-b:a", "128k",                           // Audio bitrate
				"-hls_time", "10",                        // 10 second segments
				"-hls_list_size", "0",                    // Keep all segments
				"-hls_segment_filename", segmentPattern,  // segment naming
				"-f", "hls",                              // output format HLS
				playlistPath                              // output playlist
				);
		
		//actually java unable to do encoding of raw video so we use ffmpeg..which is standrd command line tool,but to run in java ,,and performing like operating system processes we use ProcessBuilder in java
		
		ProcessBuilder processBuilder = new ProcessBuilder(command);
		processBuilder.redirectErrorStream(true);
		processBuilder.inheritIO();
		Process process = processBuilder.start();
		int exitCode = process.waitFor();
		if(exitCode !=0) {
			throw new RuntimeException("FFmpeg encoding failed with exit code:"+ exitCode);
		}
		
		
	}     
	
	// Generate master HLS playlist that references all quality playlists
	// This is the file the video player download first.
	
	private void generateMasterPlaylist(String masterPlaylistPath) throws IOException{
		StringBuilder master = new StringBuilder();
		master.append("#EXTM3U\n");
		master.append("EXT-X-VERSION:3\n\n");
		
		// Add each quality to master playlist
		
		int[][] qualities = { {1920,5000,1080},{1280,2800,720},
				    {854,1200,480},{640,800,360}};
		for(int [] q: qualities) {
			int width = q[0];
			int bitrate = q[1];
			int height=q[2];
			master.append("#EXT-X-STREAM-INF:BANDWIDTH=")
			       .append(bitrate*1000)
			       .append(", RESOLUTION=").append(width).append("x").append(height)
			       .append(",CODECS=\"avc1.42e01e,mp4a.40.2\"\n");
			master.append(height).append("p/playlist.m3u8\n\n");
			
		}
		
		Files.writeString(Paths.get(masterPlaylistPath),master.toString());
		}
	//uploading all encoded files to s3.
	private void uploadEncodedFilesToS3(String localDir, String s3Prefix){
	    File directory = new File(localDir);
	    uploadDirectoryToS3(directory, localDir, s3Prefix);
	}
	
	private void uploadDirectoryToS3(File dir, String baseDir, String s3Prefix) {
	    for (File file : dir.listFiles()) {
	        if (file.isDirectory()) {
	            uploadDirectoryToS3(file, baseDir, s3Prefix);
	        } else {
	            String relativePath = file.getAbsolutePath()
	                    .substring(baseDir.length() + 1)
	                    .replace("\\", "/");

	            String s3Key = s3Prefix + relativePath;

	            String contentType = file.getName().endsWith(".m3u8")
	                    ? "application/x-mpegURL"
	                    : "video/MP2T";

	            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
	                    .bucket(bucketName)
	                    .key(s3Key)
	                    .contentType(contentType)
	                    .build();

	            s3Client.putObject(putObjectRequest, RequestBody.fromFile(file));
	            log.debug("Uploaded: {}", s3Key);
	        }
	    }
	}
	
	
	// clean up files after encoding
	private void cleanupTempFiles(String jobPath) {
	    try {
	        Path dirPath = Paths.get(jobPath);
	        if (Files.exists(dirPath)) {
	            Files.walk(dirPath)
	                    .sorted(java.util.Comparator.reverseOrder())
	                    .map(Path::toFile)
	                    .forEach(File::delete);

	            log.info("Temp files cleaned up for job: {}", jobPath);
	        }
	    } catch (IOException e) {
	        log.warn("Failed to cleanup temp files: {}", e.getMessage());
	    }
	}
	
	
	
	
	
}
