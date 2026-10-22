package com.netflix.streamingservice.service;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.netflix.streamingservice.event.VideoEncodedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class VideoEncodedEventConsumer {
private final RedisTemplate<String,String> redisTemplate;
private static final String MASTER_PLAYLIST_KEY_PREFER = "streaming:playlist:";
/*
 * listens to video.encoded kafka topic.
 * stores master playlist key in redis when encoding is complete.
 * this allows streaming service to quickly fina the playlist key by movieid.
 */
@KafkaListener(
		topics = "video.encoded",
		groupId = "streaming-service-group"
		)
public void consumeVideoEncodedEvent(VideoEncodedEvent event) {
	log.info("Consumed VideoEncodedEvent for movie: {} success{}",event.getMovieId(),event.isSuccess());
	if(event.isSuccess()) {
		//store master playlist key in redis.
		String cacheKey = MASTER_PLAYLIST_KEY_PREFER+ event.getMovieId();
		redisTemplate.opsForValue().set(cacheKey,event.getMasterPlaylistKey());   //after encoding ,kafka send encoding event to streaming,..if that event is succeded,then key and value is stored in redis
		                                                                          // key is MASTER_PLAYLIST_KEY_PREFER+ event.getMovieId(), value is getmasterplaylist..
		log.info("Master playlist key stored in redis for movie: {}",event.getMovieId());
	}else {
		log.error("Encoding failed for movie:{}-{}",event.getMovieId(),event.getErrorMessage());
	}
}
}


//============================================================================
//REDIS STREAMING WORKFLOW SUMMARY & CORE LOGIC NOTES
//============================================================================

//1. KAFKA EVENT CONSUMER (BACKGROUND WORKFLOW)
//----------------------------------------------------------------------------
//- When video encoding completes, Kafka publishes 'VideoEncodedEvent'.
//- VideoEncodedEventConsumer listens to the 'video.encoded' topic.
//- On success, it writes the master playlist S3 object path to Redis:
//Key:   "streaming:playlist:<movieId>" (or MASTER_PLAYLIST_KEY_PREFIX + movieId)
//Value: "encoded/<movieId>/master.m3u8"
//TTL:   Permanent (No expiry set by default).

//2. CHECKING FOR MOVIE AVAILABILITY IN REDIS (CONTROLLER LEVEL)
//----------------------------------------------------------------------------
//- redisTemplate.opsForValue().get(key) takes the KEY and returns its VALUE.
//- If the key exists: Returns the string path (e.g., "encoded/101/master.m3u8").
//- If key doesn't exist: Returns null.
//- Checking 'if (playlistKey == null)' verifies whether the key exists in Redis:
//  * null  -> Movie is still encoding, failed, or missing -> Return 404 Not Found.
//  * !null -> Movie is encoded and ready to stream -> Proceed to generate URL.

//3. CACHING PRE-SIGNED URLS (SERVICE LEVEL)
//----------------------------------------------------------------------------
//- Service checks a second Redis key: "streaming:url:<movieId>".
//- CACHE HIT (cachedUrl != null):
//  * Instantly returns stored pre-signed S3 URL string from Redis RAM (~2-5ms).
//  * Skips AWS S3 SDK calls completely.
//  * Wraps data in 'new StreamingResponse(...)' and returns HTTP 200 OK to Postman/Client.
//- CACHE MISS (cachedUrl == null):
//  * Generates a new AWS S3 pre-signed URL (~50-150ms).
//  * Caches the new URL in Redis with a 55-minute TTL (safety buffer against AWS 60m expiry).
//  * Returns 'new StreamingResponse(...)' to Controller -> Client.


