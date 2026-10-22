package com.netflix.videoservice.controller;

import java.io.IOException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.netflix.videoservice.service.VideoService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/api/v1/videos")
@RequiredArgsConstructor
public class VideoController {
   private final VideoService videoService;
   
  // upload video file for a movie
   // accepts multipart file upload
   // post /api/v1/videos/upload/{movieId}
   
   @PostMapping("/upload/{movieId}")
  public ResponseEntity<String> uploadVideo(@PathVariable String movieId,@RequestParam("file") MultipartFile file)
   
   throws IOException{

  
  log.info("video upload request for movie:{} file size :{}MB",movieId,file.getSize()/(1024*1024));
  
  if(file.isEmpty()) {
	return ResponseEntity.badRequest().body("file is empty");  
  }
  
  String videoKey = videoService.uploadMovie(movieId,file);
  
  return ResponseEntity.ok(
		  "video uploaded successfully! key :"+videoKey+
		  "-Encoding started automatically via kafka");

}
  
}






