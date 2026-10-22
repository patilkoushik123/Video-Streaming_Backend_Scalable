package com.netflix.streamingservice.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.netflix.streamingservice.dto.StreamingResponse;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;


@Service
@RequiredArgsConstructor
@Slf4j
public class StreamingService {
	private final S3Client s3Client;
	
	private final S3Presigner s3Presigner;
	private final RedisTemplate<String,String> redisTemplate;
	
	@Value("${aws.s3.bucket-name}")
	private String bucketName;
	
	@Value("${aws.s3.presigned-url-expiry}")
	private long presignedUrlExpiry;
	
	private final static String STREAMING_URL_CACHE_PREFIX = "streaming:url:";
	/*
	 * get streaming url for a movie
	 * 
	 * FLOW:
	 * 1.check redis cache for existing presigned url
	 * 2.if cached - return immediately
	 * 3.if not cached - generate new presigned url from s3
	 * 4.cache  the url in redis
	 * 5.return streaming url
	 * 
	 * *
 * Why presigned URL?
 * - S3 bucket is private locker room - videos are not publicly accessible
 * - Presigned URL gives temporary access (X minutes)
 * - Prevent unauthorized video downloads
 
	 */
	

	public StreamingResponse getStreamingUrl(String movieId, String playlistKey) {
		// TODO Auto-generated method stub
		log.info("Getting streaming URL for movie: {}",movieId);
		
		String cacheKey = STREAMING_URL_CACHE_PREFIX + movieId;  // key will be for to check url bases redis is == streaming:url:+ movieId
		
		String cachedUrl = redisTemplate.opsForValue().get(cacheKey);  // based on key will get value..in value which contains url also..if cachedUrl is not null then we will get that url
		                                             
		if(cachedUrl!= null) {              //after encoding we should get url ryt,so we will check in this redis wheather the url exist or not.
			log.info("Returning cached streaming url for movie:{}",movieId);
			return new StreamingResponse(movieId,cachedUrl,"1080p,720p,480p,360",presignedUrlExpiry);
		
		}
		
		// generate presigned url from s3.
		log.info("Generating new presigned URL for movie: {}",movieId);
		String presignedUrl = generatePresignedUrl(playlistKey);
		
		// cache in redis for 55  minutes
		// 5 minutes less that actual expiry of url to avoid edge cases
	   redisTemplate.opsForValue().set(
			   cacheKey,
			   presignedUrl,     //here all the url and expiry and movieId,everything will get stored in redis.
			   55,               // here 55 min for to store the url,the reason behind 55 is,,total expiry time for presignged url is 60 which we kept,so when the video is 2 hours long,and if we keep both redis time limit and presignged url limit is 60,once 60 min is complete the url vanish,and couldnt access the video ,by keeping 55 min for redis what will happen means once 55 min of url time complete,still 5 min left for actual presigned url,so next follow up url will provided to redis and it will be next 55 min again.so like that whole movie can watch
			   TimeUnit.MINUTES
			   );/*
			   If I keep 55 minutes in Redis and 60 minutes in AWS S3, then at the end of 55 minutes, the video chunks are already downloaded into RAM buffer, so the video will run smoothly. Meanwhile, a fresh new URL will be fetched in the background.

If we keep 60 minutes in Redis and 60 minutes in AWS S3, at the end of 60 minutes, the player will be unable to download extra chunks because the pre-signed URL expires at that exact same time (resulting in a 403 Forbidden error). So the video will stop or freeze.

That is why we keep 55 minutes as a safety buffer!
			   */
	   
	   /*
	    Also, when 55 minutes are done, the frontend video player automatically triggers a background API request (GET /play?movieId=...) to get a fresh URL. The user can watch freely without clicking anything, pressing refresh, or facing any interruption. The player silently handles the whole request in the background, updates the media link in RAM buffer, and keeps the streaming completely seamless!
	    
	    // Example of built-in automatic retry logic in hls.js on the frontend
hls.on(Hls.Events.ERROR, function (event, data) {
  if (data.response && data.response.code === 403) {
    // Silent background fetch to your Spring Boot API
    fetch('/api/v1/streaming/play?movieId=101')
      .then(res => res.json())
      .then(data => {
         // Update the master playlist URL seamlessly in memory
         hls.loadSource(data.streamingUrl);
         hls.startLoad();
      });
  }
});

Standard HLS video player libraries (like hls.js, Video.js, or Android's ExoPlayer) have built-in retry hooks and event listeners specifically designed for security tokens and signed URLs.
	    */
	   
	   
	   // EVEN THE FRONTEND CODE HAS THE RETRY HOOK,SO AFTER 55 can trigger the endpoint so that will get new url,and user can waqtch the video without any intervention.
	   log.info("Streaming URL generated and cached for movie: {}",movieId);
		return new StreamingResponse(
				movieId,presignedUrl,
				"1080p,720p,480p,360",
				presignedUrlExpiry
				);

	}
	
	public String getSignedPlaylist(String movieId,String playlistPath) {
		// get base path for this playlist
		String basePath = playlistPath.substring(0,playlistPath.lastIndexOf('/')+1);
		// read m3u8 content from s3
		String m3u8Content = readFromS3(playlistPath);
		//Rewrite each line that is a segment or playlist reference
		String signedContent = rewriteM3u8SignedUrls(m3u8Content,basePath);
		
		return signedContent;
	}
	
	private String rewriteM3u8SignedUrls(String m3u8Content,String basePath) {
		StringBuilder rewritten = new StringBuilder();
		for(String line: m3u8Content.split("\n")) {
			String trimmed = line.trim();
			//skip empty lines and comments
			if(trimmed.isEmpty() || trimmed.startsWith("#")) {
				rewritten.append(line).append("\n");
				continue;
			}
			// this is a segment or playlist reference
			// build full s3 key and sign it
			String fullKey = basePath+ trimmed;
			String signedUrl = generatePresignedUrl(fullKey);
			rewritten.append(signedUrl).append("\n");
		}
		return rewritten.toString();
	}
	
	/*
	 * read file content from s3
	 */
	
	private String readFromS3(String s3Key) {
		GetObjectRequest request = GetObjectRequest.builder()
				.bucket(bucketName)
				.key(s3Key)
				.build();
		
		ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request);
		return new BufferedReader( new InputStreamReader(response))
				.lines()
				.collect(Collectors.joining("\n"));
	}  
	
	/*
	 * ============================================================
	 * getSignedPlylist() - SIGNED PLAYLIST FLOW
	 * ============================================================
	 *
	 * Purpose:
	 * Read the master.m3u8 file from private S3 and replace the
	 * playlist/file references inside it with temporary presigned URLs.
	 *
	 *
	 * Example S3 structure:
	 *
	 * movies/101/
	 * ├── master.m3u8
	 * ├── 1080p/
	 * │   ├── playlist.m3u8
	 * │   ├── segment_001.ts
	 * │   └── segment_002.ts
	 * └── 720p/
	 *     ├── playlist.m3u8
	 *     ├── segment_001.ts
	 *     └── segment_002.ts
	 *
	 *
	 * Example call:
	 *
	 * getSignedPlylist(
	 *     "101",
	 *     "movies/101/master.m3u8"
	 * );
	 *
	 * movieId      = 101
	 * playlistPath = movies/101/master.m3u8
	 *
	 *
	 * ============================================================
	 * 1. FIND BASE PATH
	 * ============================================================
	 *
	 * Code:
	 *
	 * String basePath =
	 *     playlistPath.substring(
	 *         0,
	 *         playlistPath.lastIndexOf('/') + 1
	 *     );
	 *
	 *
	 * playlistPath:
	 *
	 * movies/101/master.m3u8
	 *
	 * basePath:
	 *
	 * movies/101/
	 *
	 *
	 * Why?
	 *
	 * The master playlist contains relative paths such as:
	 *
	 * 1080p/playlist.m3u8
	 * 720p/playlist.m3u8
	 *
	 * We need to convert them into complete S3 keys:
	 *
	 * movies/101/ + 1080p/playlist.m3u8
	 *
	 * =
	 *
	 * movies/101/1080p/playlist.m3u8
	 *
	 *
	 * ============================================================
	 * 2. READ MASTER PLAYLIST FROM S3
	 * ============================================================
	 *
	 * Code:
	 *
	 * String m3u8Content = readFromS3(playlistPath);
	 *
	 *
	 * This calls:
	 *
	 * readFromS3("movies/101/master.m3u8");
	 *
	 *
	 * readFromS3() creates a GetObjectRequest:
	 *
	 * GetObjectRequest request = GetObjectRequest.builder()
	 *         .bucket(bucketName)
	 *         .key(s3Key)
	 *         .build();
	 *
	 *
	 * Meaning:
	 *
	 * "Get the master.m3u8 file from my private S3 bucket."
	 *
	 *
	 * S3 returns the content of master.m3u8.
	 *
	 *
	 * Example content:
	 *
	 * #EXTM3U
	 * #EXT-X-STREAM-INF:BANDWIDTH=5000000
	 * 1080p/playlist.m3u8
	 * #EXT-X-STREAM-INF:BANDWIDTH=2800000
	 * 720p/playlist.m3u8
	 *
	 *
	 * readFromS3() converts this S3 response into a Java String.
	 *
	 *
	 * ============================================================
	 * 3. REWRITE THE PLAYLIST WITH SIGNED URLs
	 * ============================================================
	 *
	 * Code:
	 *
	 * String signedContent =
	 *     rewriteM3u8SignedUrls(m3u8Content, basePath);
	 *
	 *
	 * We pass:
	 *
	 * m3u8Content = content of master.m3u8
	 *
	 * basePath = movies/101/
	 *
	 *
	 * rewriteM3u8SignedUrls() now processes the playlist
	 * line by line.
	 *
	 *
	 * ============================================================
	 * 4. PROCESS EACH LINE
	 * ============================================================
	 *
	 * Code:
	 *
	 * for(String line : m3u8Content.split("\n")) {
	 *
	 *
	 * The playlist is divided into individual lines.
	 *
	 *
	 * Example:
	 *
	 * Line 1 -> #EXTM3U
	 *
	 * Line 2 -> #EXT-X-STREAM-INF:BANDWIDTH=5000000
	 *
	 * Line 3 -> 1080p/playlist.m3u8
	 *
	 * Line 4 -> #EXT-X-STREAM-INF:BANDWIDTH=2800000
	 *
	 * Line 5 -> 720p/playlist.m3u8
	 *
	 *
	 * ============================================================
	 * 5. IGNORE HLS COMMENTS / INSTRUCTIONS
	 * ============================================================
	 *
	 * Code:
	 *
	 * String trimmed = line.trim();
	 *
	 * if(trimmed.isEmpty() || trimmed.startsWith("#")) {
	 *     rewritten.append(line).append("\n");
	 *     continue;
	 * }
	 *
	 *
	 * Example:
	 *
	 * #EXTM3U
	 *
	 * starts with "#".
	 *
	 * Therefore it is kept unchanged.
	 *
	 *
	 * Same for:
	 *
	 * #EXT-X-STREAM-INF:BANDWIDTH=5000000
	 *
	 *
	 * These are HLS instructions/metadata.
	 * They are NOT S3 files, so we don't generate signed URLs for them.
	 *
	 *
	 * ============================================================
	 * 6. FIND ACTUAL PLAYLIST / SEGMENT REFERENCE
	 * ============================================================
	 *
	 * Example line:
	 *
	 * 1080p/playlist.m3u8
	 *
	 *
	 * This does NOT start with "#".
	 *
	 * Therefore the code treats it as a file reference.
	 *
	 *
	 * Code:
	 *
	 * String fullKey = basePath + trimmed;
	 *
	 *
	 * Values:
	 *
	 * basePath = movies/101/
	 *
	 * trimmed  = 1080p/playlist.m3u8
	 *
	 *
	 * Result:
	 *
	 * fullKey = movies/101/1080p/playlist.m3u8
	 *
	 *
	 * Now we have the complete S3 key.
	 *
	 *
	 * ============================================================
	 * 7. GENERATE PRESIGNED URL
	 * ============================================================
	 *
	 * Code:
	 *
	 * String signedUrl = generatePresignedUrl(fullKey);
	 *
	 *
	 * This calls:
	 *
	 * generatePresignedUrl(
	 *     "movies/101/1080p/playlist.m3u8"
	 * );
	 *
	 *
	 * The generatePresignedUrl() method creates a temporary
	 * signed URL for this private S3 object.
	 *
	 *
	 * Example:
	 *
	 * https://s3.../movies/101/1080p/playlist.m3u8
	 * ?X-Amz-Algorithm=...
	 * &X-Amz-Signature=ABC...
	 *
	 *
	 * Then:
	 *
	 * rewritten.append(signedUrl).append("\n");
	 *
	 *
	 * replaces:
	 *
	 * 1080p/playlist.m3u8
	 *
	 * with:
	 *
	 * https://s3.../movies/101/1080p/playlist.m3u8?...signature
	 *
	 *
	 * ============================================================
	 * 8. SAME PROCESS FOR 720p
	 * ============================================================
	 *
	 * Original line:
	 *
	 * 720p/playlist.m3u8
	 *
	 *
	 * Build full S3 key:
	 *
	 * movies/101/720p/playlist.m3u8
	 *
	 *
	 * Generate presigned URL:
	 *
	 * generatePresignedUrl(
	 *     "movies/101/720p/playlist.m3u8"
	 * );
	 *
	 *
	 * Result:
	 *
	 * https://s3.../movies/101/720p/playlist.m3u8?...signature
	 *
	 *
	 * ============================================================
	 * 9. FINAL SIGNED PLAYLIST
	 * ============================================================
	 *
	 * Original master.m3u8:
	 *
	 * #EXTM3U
	 *
	 * #EXT-X-STREAM-INF:BANDWIDTH=5000000
	 * 1080p/playlist.m3u8
	 *
	 * #EXT-X-STREAM-INF:BANDWIDTH=2800000
	 * 720p/playlist.m3u8
	 *
	 *
	 * After rewriting:
	 *
	 * #EXTM3U
	 *
	 * #EXT-X-STREAM-INF:BANDWIDTH=5000000
	 * https://s3.../movies/101/1080p/playlist.m3u8?signature=ABC
	 *
	 * #EXT-X-STREAM-INF:BANDWIDTH=2800000
	 * https://s3.../movies/101/720p/playlist.m3u8?signature=XYZ
	 *
	 *
	 * ============================================================
	 * 10. OVERALL FLOW
	 * ============================================================
	 *
	 * getSignedPlylist()
	 *          ↓
	 * Get basePath
	 *          ↓
	 * readFromS3()
	 *          ↓
	 * Read master.m3u8
	 *          ↓
	 * rewriteM3u8SignedUrls()
	 *          ↓
	 * Process each line
	 *          ↓
	 * Ignore # HLS instructions
	 *          ↓
	 * Find playlist/segment reference
	 *          ↓
	 * Build complete S3 key
	 *          ↓
	 * generatePresignedUrl()
	 *          ↓
	 * Replace reference with signed URL
	 *          ↓
	 * Return signed playlist
	 *
	 *
	 * SIMPLE MEMORY:
	 *
	 * Read playlist
	 *      ↓
	 * Find file references
	 *      ↓
	 * Build S3 key
	 *      ↓
	 * Generate signed URL
	 *      ↓
	 * Replace file reference
	 *      ↓
	 * Return signed playlist
	 *
	 */
	
	
	/*
	 * generate a presinged url for s3 object
	 * url expired after configures time.
	 */

	private String generatePresignedUrl(String key) {
		GetObjectRequest getObjectRequest = GetObjectRequest.builder()
				.bucket(bucketName)
				.key(key)    //here key is exact path of each video in s3
				.build();
		
		GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
				.signatureDuration(Duration.ofMinutes(presignedUrlExpiry))
						.getObjectRequest(getObjectRequest)
						.build();
		
	 return s3Presigner.presignGetObject(presignRequest)
			 .url()
			 .toString();
	}
	
	// this invalidate method called when existing movie is updated or encoded.
	
	public void InvalidateCache(String movieId) {
		String cacheKey = STREAMING_URL_CACHE_PREFIX + movieId;
		redisTemplate.delete(cacheKey);
		log.info("Streaming URL cache invalidated for movie:{}",movieId);
	}
	

}
