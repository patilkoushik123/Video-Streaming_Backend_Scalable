package com.netflix.contentservice.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.netflix.contentservice.dto.MovieRequest;
import com.netflix.contentservice.dto.MovieResponse;
import com.netflix.contentservice.model.Genre;
import com.netflix.contentservice.service.ContentService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/v1/movies")
@Slf4j
@RequiredArgsConstructor
public class ContentController {
private final ContentService contentService;

@PostMapping
public ResponseEntity<MovieResponse> addMovie(@Valid @RequestBody MovieRequest movieRequest){
	return ResponseEntity.status(HttpStatus.CREATED).body(contentService.addMovie(movieRequest));
}


@GetMapping
public ResponseEntity<List<MovieResponse>> getAllMovies(){
	return ResponseEntity.ok(contentService.getAllMovies());
}

@GetMapping("/genre/{genre}")
public ResponseEntity<List<MovieResponse>> getMoviesByGenre(@PathVariable Genre genre){
	return ResponseEntity.ok(contentService.getMoviesByGenre(genre));
}

@GetMapping("/{movieId}")
public ResponseEntity<MovieResponse> getMoviesByGenre(@PathVariable String movieId){
	return ResponseEntity.ok(contentService.getMoviesById(movieId));
}

public ResponseEntity<List<MovieResponse>>  searchMovies(@RequestParam String title){
	return ResponseEntity.ok(contentService.searchMovies(title));
}




}














