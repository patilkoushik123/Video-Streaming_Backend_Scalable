package com.netflix.contentservice.model;

import java.time.LocalDateTime;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Entity
@Table(name="movies")
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Movie {

	
	@GeneratedValue(strategy= GenerationType.UUID)
	@Id
	private String id;
	
	@Column(nullable=false)
	private String title;
	
	@Column(length=1000)
	private String description;
	
	@Enumerated(EnumType.STRING)
	private Genre genre;
	
	private String director;
	@Column(name = "movie_cast")
     private String cast;
	private int releaseYear;
	private double rating;
	private String thumbnailUrl;
	private int durationMinutes;
	
	//s3 key for the video file
	private String videoKey;
	
	
	// HLS master playlist URL for streaming;
	private String hlsUrl;
	
	
	//status of video processing..
	@Enumerated(EnumType.STRING)
	private VideoStatus videoStatus;
	
	@CreationTimestamp
	private LocalDateTime createdAt;
	
	@UpdateTimestamp
    private LocalDateTime updatedAt;
	
}



