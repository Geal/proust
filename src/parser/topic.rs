use nom::{IResult, be_i32, be_i16};

use parser::{
  primitive::*,
  errors::*,
};

#[derive(PartialEq, Debug)]
pub struct CreateTopicsRequest<'a> {
  topics: Vec<CreateTopic<'a>>,
  timeout: i32,
}

#[derive(PartialEq, Debug)]
pub struct CreateTopic<'a> {
  topic: KafkaString<'a>,
  num_partitions: i32,
  replication_factor: i16,
  replica_assignment: Vec<ReplicaAssignment>,
  config_entries: Vec<ConfigEntries<'a>>,
}


#[derive(PartialEq, Debug)]
struct ReplicaAssignment {
  partition: i32,
  replicas: Vec<i32>,
}


#[derive(PartialEq, Debug)]
struct ConfigEntries<'a> {
  name: KafkaString<'a>,
  value: KafkaString<'a>,
}


named!(pub create_topics<CreateTopicsRequest>,
  do_parse!(
    topics: apply!(kafka_array, create_topic) >>
    timeout: be_i32 >>
    (
      CreateTopicsRequest {
        topics,
        timeout,
      }
    )
  )
);


named!(create_topic<CreateTopic>,
  do_parse!(
    topic: kafka_string >>
    num_partitions: be_i32 >>
    replication_factor: be_i16 >>
    replica_assignment: apply!(kafka_array, replica_assignment) >>
    config_entries: apply!(kafka_array, config_entries) >>
    (
      CreateTopic {
        topic,
        num_partitions,
        replication_factor,
        replica_assignment,
        config_entries,
      }
    )
  )
);


named!(parse_ints<Vec<i32>>, many0!(be_i32));

named!(replica_assignment<ReplicaAssignment>,
  do_parse!(
    partition: be_i32 >>
    replicas: parse_ints >>
    (
      ReplicaAssignment {
        partition,
        replicas,
      }
    )
  )
);


named!(config_entries<ConfigEntries>,
  do_parse!(
    name: kafka_string >>
    value: kafka_string >>
    (
      ConfigEntries {
        name,
        value,
      }
    )
  )
);

#[cfg(test)]
mod tests {

}