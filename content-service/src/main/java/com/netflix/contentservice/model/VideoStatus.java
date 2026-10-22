package com.netflix.contentservice.model;

/*
 * tracks the video processing lifecycle.
 * flow:
 * pending -> uploaded -> encoding ->  encoded -> ready 
                                   -> failed
 */
public enum VideoStatus {
PENDING,  // MOVIE ADDED BUT NOT UPLOADED YET
UPLOADED,   // RAW VIDEO UPLOADED TO S3
ENCODING,  // FFmpeg  is encoding the video
ENCODED,  // encoding complete
READY,  //HLS playlist ready -- can be streamed
FAILED // encodinng failure
}
