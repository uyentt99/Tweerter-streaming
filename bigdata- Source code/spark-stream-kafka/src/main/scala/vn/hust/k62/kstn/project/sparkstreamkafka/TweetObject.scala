package vn.hust.k62.kstn.project.sparkstreamkafka

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonInclude, JsonProperty}

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
case class TweetObject(
                      @JsonProperty("id") val id : Long,
                      @JsonProperty("createdAt") val createdAt : Long,
                      @JsonProperty("text") val text: String,
                      @JsonProperty("source") var source : String,
                      @JsonProperty("lang") val lang : String,
                      var day : String ,
                      var hour : String
                      )
