package com.netflix.streamingservice.controller;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.netflix.streamingservice.dto.StreamingResponse;
import com.netflix.streamingservice.service.StreamingService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/v1/stream")
@CrossOrigin(origins = "*") // <--- ADD THIS
@Slf4j
@RequiredArgsConstructor
public class StreamingController {
   private final StreamingService streamingService;
   private final RedisTemplate<String,String> redisTemplate;
   private static final String MASTER_PLAYLIST_KEY_PREFIX="streaming:playlist:";
   /*
    * get streaming url for a movie
    * return presigned HLS master playlist URL
    * GET /api/v1/stream/{movieId}
    
    */
   @GetMapping("/{movieId}")
   public ResponseEntity<StreamingResponse> getStreamingUrl(@PathVariable String movieId){
	   log.info("streaming request for movie: {}",movieId);
	   // get master playlist key from redis
	   String playlistKey = redisTemplate.opsForValue().get(MASTER_PLAYLIST_KEY_PREFIX+movieId);  // playlistkey is value of movieid (cache key) ,the value is exact path of file of each video in s3,we will use this playlistkey  in the process of getting presigned url.
	   
	   if(playlistKey == null) {            // here helps to check wheather the video is encoded or not,if not encoded(playlistkey is null then not encoded)
		   return ResponseEntity.notFound().build();
	   }                                              // this redis is mapped in videEncodedeventconsumer.java
	  StreamingResponse response= streamingService.getStreamingUrl(movieId,playlistKey);
	  return ResponseEntity.ok(response);
   }
   
   @GetMapping("/{movieId}/playlist")
   public ResponseEntity<String> getSignedPlayList(@PathVariable String movieId,@RequestParam String path){
	   String signedPlaylist = streamingService.getSignedPlaylist(movieId,path);
	   return ResponseEntity.ok()
			   .header("Content-Type", "application/x-mpegURL")
			   .body(signedPlaylist);
   }
   
}
