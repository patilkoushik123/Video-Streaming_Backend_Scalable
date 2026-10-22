package com.netflix.contentservice.dto;

import java.time.LocalDateTime;

import com.netflix.contentservice.model.Genre;
import com.netflix.contentservice.model.Movie;
import com.netflix.contentservice.model.VideoStatus;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;




@NoArgsConstructor
@AllArgsConstructor
@Data
public class MovieRequest {

	
	@NotBlank(message =" Title is required")
	private String title;
	
	
	private String description;
	
	@NotNull(message = "Genre is required")
	private Genre genre;
	
	private String director;
	private String cast;
	private int releaseYear;
	private double rating;
	private String thumbnailUrl;
	private int durationMinutes;
	
	
}
