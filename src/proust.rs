use parser::request::{RequestMessage,RequestPayload};
use responses::response::{ResponseMessage,ResponsePayload};
use responses::metadata::{MetadataResponse,Broker,TopicMetadata,PartitionMetadata};


pub fn handle_request(req: RequestMessage) -> Result<ResponseMessage,u8> {
    println!("Got request: {:?}", req);
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
      }
      _ => Err(0)
    }
}
