use parser::request::{RequestMessage,RequestPayload};
use responses::response::{ResponseMessage,ResponsePayload};
use responses::metadata::{MetadataResponse,Broker,TopicMetadata,PartitionMetadata};
use responses::api_versions::{ApiVersionsResponse, ApiVersion };

//TODO: Use const for the API_KEY
/// api version =~ 0.9.0
pub const API_VERSION_0_9_0: [ApiVersion; 13] = [
  ApiVersion(0, 0, 0),
  ApiVersion(1, 0, 0),
  ApiVersion(2, 0, 0),
  ApiVersion(3, 0, 0),
  ApiVersion(8, 0, 2),
  ApiVersion(9, 0, 0),
  ApiVersion(10, 0, 0),
  ApiVersion(11, 0, 0),
  ApiVersion(12, 0, 0),
  ApiVersion(13, 0, 0),
  ApiVersion(14, 0, 0),
  ApiVersion(15, 0, 0),
  ApiVersion(16, 0, 0),
];

pub fn handle_request(req: RequestMessage) -> Result<ResponseMessage,u8> {
    match req.request_payload {
      RequestPayload::MetadataRequest(_) => {
        Ok(ResponseMessage {
          correlation_id: req.correlation_id,
          response_payload: ResponsePayload::MetadataResponse(MetadataResponse {
            brokers: vec![Broker {
              node_id: 1234,
              host: "localhost",
              port: 9092
            }],
            topics: vec![TopicMetadata {
              topic_error_code: 0,
              topic_name: "topic1",
              partitions: vec![PartitionMetadata {
                partition_error_code: 0,
                partition_id: 0,
                leader: 1234,
                replicas: vec![0],
                isr: vec![1]
              }]
            }]
          })
        })
      }
      RequestPayload::ProduceRequest(x) => {
        Ok(ResponseMessage {
            correlation_id: req.correlation_id,
            response_payload: ResponsePayload::ProduceResponse(vec![( "topic1", vec![(0, 0, 1337)])])
        })
      },
      RequestPayload::ApiVersionsRequest => {
        Ok(ResponseMessage {
          correlation_id: req.correlation_id,
          response_payload: ResponsePayload::ApiVersionsResponse(
            ApiVersionsResponse {
              error_code: 0,
              api_versions: &API_VERSION_0_9_0,
            }
          ),
        })
      },
      _ => Err(0)
    }
}
