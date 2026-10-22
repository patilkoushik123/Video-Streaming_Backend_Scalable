package com.netflix.videoservice.config;



import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;



@Configuration
public class S3Config {

	@Value("${aws.access-key}")
	private String accessKey;
	
	@Value("${aws.secret-key}")
	private String secretKey;
	
	@Value("${aws.region}")
	private String region; 
	                                         //NOTE : we can upload raw video to s3 in two ways,1st way is through s3 client such as putObject,and another way is use presigner,backend provides temporary url and with that we an upload to s3,currently we are using 1st way.
	@Bean
	public S3Client s3Client() {
		return S3Client.builder()
				.region(Region.of(region))
				.credentialsProvider(StaticCredentialsProvider.create(     //fst way
						AwsBasicCredentials.create(accessKey,secretKey)))
				.build();
	}
	@Bean
	public S3Presigner s3Presigner() {
		return S3Presigner.builder()
				.region(Region.of(region))
				.credentialsProvider(StaticCredentialsProvider.create(
						AwsBasicCredentials.create(accessKey,secretKey)))    //2nd way..
				.build();
	}
	

}