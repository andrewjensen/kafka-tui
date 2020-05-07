use byteorder::{BigEndian, ReadBytesExt};
use futures::StreamExt;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use std::collections::HashMap;
use std::io::Cursor;
use std::str;
use std::time::Duration;

const DEFAULT_TIMEOUT_MS: u64 = 60_000;
const MOCK_BROKERS: &str = "localhost:9092";
const CONSUMER_GROUP_NAME: &str = "example_consumer_group_id_2";
const TOPIC_NAME: &str = "__consumer_offsets";

#[derive(Debug)]
pub struct ClusterConsumerOffsetState {
    pub topics: HashMap<String, TopicState>,
}

impl ClusterConsumerOffsetState {
    fn new() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }

    fn apply_message(&mut self, message: &ConsumerOffsetMessage) {
        let topic = self
            .topics
            .entry(message.topic.clone())
            .or_insert(TopicState::new());
        topic.apply_message(message);
    }
}

#[derive(Debug, Clone)]
pub struct TopicState {
    pub consumer_group_states: HashMap<String, OffsetMap>,
}

impl TopicState {
    fn new() -> Self {
        Self {
            consumer_group_states: HashMap::new(),
        }
    }

    fn apply_message(&mut self, message: &ConsumerOffsetMessage) {
        let cg_state = self
            .consumer_group_states
            .entry(message.consumer_group.clone())
            .or_insert(OffsetMap::new());
        cg_state.set(message.partition_id, message.offset);
    }
}

#[derive(Debug, Clone)]
pub struct OffsetMap {
    pub partition_count: Option<usize>,
    pub partition_offsets: HashMap<i32, i64>,
}

impl OffsetMap {
    pub fn new() -> Self {
        Self {
            partition_count: None,
            partition_offsets: HashMap::new(),
        }
    }

    pub fn create_with_count(partition_count: usize) -> Self {
        Self {
            partition_count: Some(partition_count),
            partition_offsets: HashMap::new(),
        }
    }

    pub fn get(&self, partition_id: i32) -> i64 {
        match self.partition_offsets.get(&partition_id) {
            Some(offset) => *offset,
            None => 0,
        }
    }

    pub fn set(&mut self, partition_id: i32, offset: i64) {
        self.partition_offsets.insert(partition_id, offset);
    }

    pub fn get_summed_offsets(&self) -> i64 {
        self.partition_offsets
            .iter()
            .fold(0, |sum, (_partition_id, partition_offset)| {
                partition_offset + sum
            })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ConsumerOffsetMessage {
    pub consumer_group: String,
    pub topic: String,
    pub partition_id: i32,
    pub offset: i64,
}

#[derive(Debug)]
pub enum DecodeError {
    UnsupportedPayloadVersion(i16),
}

pub async fn fetch_consumer_offset_state() -> ClusterConsumerOffsetState {
    let topic_partition_offsets: OffsetMap = fetch_consumer_offsets_topic_offsets();

    let mut consumed_partition_offsets: OffsetMap =
        OffsetMap::create_with_count(topic_partition_offsets.partition_count.unwrap());

    let mut cluster_state = ClusterConsumerOffsetState::new();

    let topics = [TOPIC_NAME];

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", CONSUMER_GROUP_NAME)
        .set("bootstrap.servers", MOCK_BROKERS)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Warning)
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topics)
        .expect("Can't subscribe to specified topics");

    let mut message_stream = consumer.start();

    while let Some(message) = message_stream.next().await {
        if let Err(e) = message {
            println!("Kafka error: {}", e);
            panic!("Kafka error!");
        }

        let m = message.unwrap();

        consumed_partition_offsets.set(m.partition(), m.offset());

        let key = m.key();
        let payload = m.payload();

        match (key, payload) {
            (Some(key), Some(payload)) => {
                let decoded_message = decode_consumer_offset_message(key, payload);

                if let Some(Ok(dm)) = decoded_message {
                    cluster_state.apply_message(&dm);
                }
            }
            _ => {}
        }

        if is_consumer_caught_up(&topic_partition_offsets, &consumed_partition_offsets) {
            break;
        }
    }

    consumer.stop();

    cluster_state
}

fn decode_consumer_offset_message(
    key: &[u8],
    payload: &[u8],
) -> Option<Result<ConsumerOffsetMessage, DecodeError>> {
    // Key message contents, for version 1:
    // See https://github.com/apache/kafka/blob/85e81d48c81a11f8e32a734703c2103c655a4cc9/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L1313
    //
    // version          short   2 bytes
    // group            string  (variable)
    // topic            string  (variable)
    // partition        int     4 bytes
    //
    // Payload message contents, for version 3:
    // See https://github.com/apache/kafka/blob/85e81d48c81a11f8e32a734703c2103c655a4cc9/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L1342
    //
    // version          short   2 bytes
    // offset           long    8 bytes
    // leaderEpoch      int     4 bytes
    // metadata         string  (variable)
    // commitTimestamp  long    8 bytes

    let key_buffer = key;

    let (key_version, key_buffer) = consume_i16(key_buffer);
    if key_version != 1 {
        // We aren't interested in this other key, so we will just return early.
        return None;
    }

    let (group, key_buffer) = consume_string(key_buffer);
    let (topic, key_buffer) = consume_string(key_buffer);
    let (partition_id, _key_buffer) = consume_i32(key_buffer);

    let payload_buffer = payload;

    let (payload_version, payload_buffer) = consume_i16(payload_buffer);
    if payload_version != 3 {
        let error = DecodeError::UnsupportedPayloadVersion(payload_version);
        return Some(Err(error));
    }

    let (offset, _payload_buffer) = consume_i64(payload_buffer);

    let decoded_message = ConsumerOffsetMessage {
        consumer_group: group,
        topic: topic,
        partition_id: partition_id,
        offset: offset,
    };

    Some(Ok(decoded_message))
}

fn fetch_consumer_offsets_topic_offsets() -> OffsetMap {
    let timeout = Duration::from_millis(DEFAULT_TIMEOUT_MS);

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", MOCK_BROKERS)
        .create()
        .expect("Consumer creation failed");

    let metadata = consumer
        .fetch_metadata(Some(TOPIC_NAME), timeout)
        .expect("Failed to fetch metadata");

    let topic = metadata.topics().iter().next().unwrap();

    let mut offset_map = OffsetMap::create_with_count(topic.partitions().len());

    topic.partitions().iter().for_each(|partition| {
        let (_low_watermark, high_watermark) = consumer
            .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
            .unwrap();

        offset_map.set(partition.id(), high_watermark);
    });

    offset_map
}

fn is_consumer_caught_up(
    topic_partition_offsets: &OffsetMap,
    consumed_partition_offsets: &OffsetMap,
) -> bool {
    let topic_partitions = topic_partition_offsets.partition_count.unwrap() as i32;

    for partition_id in 0..topic_partitions {
        let partition_offset = topic_partition_offsets.get(partition_id);
        if partition_offset == 0 {
            continue;
        }

        let cg_offset = consumed_partition_offsets.get(partition_id);
        if cg_offset < partition_offset - 1 {
            return false;
        }
    }

    true
}

fn consume_i16(buffer: &[u8]) -> (i16, &[u8]) {
    let mut cursor = Cursor::new(buffer);
    let result = cursor.read_i16::<BigEndian>().unwrap();

    let new_buffer = &buffer[2..];

    (result, new_buffer)
}

fn consume_i32(buffer: &[u8]) -> (i32, &[u8]) {
    let mut cursor = Cursor::new(buffer);
    let result = cursor.read_i32::<BigEndian>().unwrap();

    let new_buffer = &buffer[4..];

    (result, new_buffer)
}

fn consume_i64(buffer: &[u8]) -> (i64, &[u8]) {
    let mut cursor = Cursor::new(buffer);
    let result = cursor.read_i64::<BigEndian>().unwrap();

    let new_buffer = &buffer[8..];

    (result, new_buffer)
}

fn consume_string(buffer: &[u8]) -> (String, &[u8]) {
    let mut cursor = Cursor::new(buffer);
    let string_length = cursor.read_i16::<BigEndian>().unwrap();

    let string_buffer = &buffer[(2 as usize)..((string_length + 2) as usize)];
    let result = str::from_utf8(string_buffer).unwrap().to_string();

    let new_buffer = &buffer[(string_length + 2) as usize..];

    (result, new_buffer)
}

fn print_byte_stream(bytes: &[u8]) {
    println!("{:?}", bytes);
    for byte in bytes {
        if *byte >= 32 && *byte < 127 {
            print!("{}", *byte as char);
        } else {
            print!("#");
        }
    }
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_consumer_offset_message() {
        let key: &[u8] = &[
            0, 1, 0, 22, 109, 121, 46, 116, 101, 115, 116, 46, 99, 111, 110, 115, 117, 109, 101,
            114, 46, 103, 114, 111, 117, 112, 0, 13, 109, 121, 46, 116, 101, 115, 116, 46, 116,
            111, 112, 105, 99, 0, 0, 0, 3,
        ];
        let payload: &[u8] = &[
            0, 3, 0, 0, 0, 0, 0, 0, 0, 7, 255, 255, 255, 255, 0, 0, 0, 0, 1, 113, 230, 224, 188, 50,
        ];
        let result = decode_consumer_offset_message(key, payload);

        let message = result.unwrap().unwrap();

        assert_eq!(
            message,
            ConsumerOffsetMessage {
                consumer_group: "my.test.consumer.group".to_string(),
                topic: "my.test.topic".to_string(),
                partition_id: 3,
                offset: 7
            }
        );
    }
}
