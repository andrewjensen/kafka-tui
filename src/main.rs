use cursive::theme::Effect;
use cursive::views::{Dialog, DummyView, LinearLayout, ScrollView, SelectView, TextView};
use cursive::Cursive;

mod formatting;
mod kafka;

use formatting::format_padded;
use kafka::{
    fetch_cluster_metadata, fetch_consumer_offset_state, fetch_topic_details, OffsetMap,
    TopicDetails, TopicPartitionDetails, TopicState, TopicSummary,
};

const MOCK_BROKERS: &str = "localhost:9092";

// For summary view
const CHARS_TOPIC_NAME: usize = 40;
const CHARS_PARTITION_COUNT: usize = 11;
const CHARS_REPLICA_COUNT: usize = 9;
const CHARS_SUM_OFFSETS: usize = 9;

// For topic view
const CHARS_PARTITION_ID: usize = 10;
const CHARS_PARTITION_OFFSET: usize = 10;
const CHARS_CG_NAME: usize = 40;
const CHARS_CG_SUM_OFFSETS: usize = 9;

// For consumer group view
const CHARS_PARTITION_LAG: usize = 10;

#[tokio::main]
async fn main() {
    println!("Fetching cluster metadata...");
    let cluster_summary = fetch_cluster_metadata(MOCK_BROKERS);

    println!("Fetching consumer group states...");
    let cluster_cg_state = fetch_consumer_offset_state().await;

    let mut topics = cluster_summary.topics;
    topics.sort_by(|a, b| a.name.cmp(&b.name));

    let mut siv = cursive::Cursive::default();

    siv.add_global_callback('q', |s| s.quit());

    let summary_view = Dialog::around(
        LinearLayout::vertical()
            .child(TextView::new("Cluster").effect(Effect::Bold))
            .child(TextView::new(format_cluster_summary(
                topics.len(),
                cluster_summary.brokers.len(),
            )))
            .child(DummyView)
            .child(TextView::new(format_topic_list_headers()).effect(Effect::Bold))
            .child(ScrollView::new(
                SelectView::new()
                    .with_all(
                        topics
                            .iter()
                            .map(|topic| (format_topic_summary(topic), topic.name.clone())),
                    )
                    .on_submit(move |s, topic_name| {
                        let topic = fetch_topic_details(MOCK_BROKERS, topic_name);
                        let topic_cg_state: Option<TopicState> =
                            match cluster_cg_state.topics.get(&topic_name.to_string()) {
                                Some(state) => Some(state.clone()),
                                None => None,
                            };
                        on_select_topic(s, topic, topic_cg_state);
                    }),
            )),
    )
    .title("Kafka");

    siv.add_layer(summary_view);

    siv.run();
}

// Summary View helpers

fn on_select_topic(siv: &mut Cursive, topic: TopicDetails, topic_state: Option<TopicState>) {
    let cg_view = match topic_state {
        Some(topic_state) => LinearLayout::vertical()
            .child(TextView::new(format_consumer_group_list_headers()).effect(Effect::Bold))
            .child(ScrollView::new(
                SelectView::new()
                    .with_all(topic_state.consumer_group_states.iter().map(
                        |(cg_name, cg_offset_map)| {
                            (
                                format_consumer_group(cg_name, &cg_offset_map),
                                cg_name.clone(),
                            )
                        },
                    ))
                    .on_submit(move |s, cg_name| {
                        let offset_map: OffsetMap = topic_state
                            .consumer_group_states
                            .get(cg_name)
                            .unwrap()
                            .clone();
                        on_select_consumer_group(s, cg_name, offset_map);
                    }),
            )),

        None => LinearLayout::vertical()
            .child(TextView::new("Consumer Groups").effect(Effect::Bold))
            .child(TextView::new("(None)")),
    };

    let topic_view = Dialog::around(
        LinearLayout::vertical()
            .child(TextView::new("Topic").effect(Effect::Bold))
            .child(TextView::new(format_topic_details(&topic)))
            .child(DummyView)
            .child(TextView::new(format_partition_list_headers()).effect(Effect::Bold))
            .child(ScrollView::new(TextView::new(format_partition_list(
                &topic.partitions,
            ))))
            .child(DummyView)
            .child(cg_view),
    )
    .title(format!("Topic: {}", topic.name))
    .button("Back", |s| {
        s.pop_layer();
    });

    siv.add_layer(topic_view);
}

fn format_cluster_summary(topic_count: usize, broker_count: usize) -> String {
    format!("Topics: {}\nBrokers: {}", topic_count, broker_count)
}

fn format_topic_list_headers() -> String {
    let name_fmt = format_padded("Topic", CHARS_TOPIC_NAME);
    let partitions_fmt = format_padded("Partitions", CHARS_PARTITION_COUNT);
    let replicas_fmt = format_padded("Replicas", CHARS_REPLICA_COUNT);
    let offsets_fmt = format_padded("Offsets", CHARS_SUM_OFFSETS);

    format!(
        "{} {} {} {}",
        name_fmt, partitions_fmt, replicas_fmt, offsets_fmt
    )
}

fn format_topic_summary(topic: &TopicSummary) -> String {
    let name_fmt = format_padded(&topic.name, CHARS_TOPIC_NAME);
    let partitions_fmt = format_padded(&topic.partition_count.to_string(), CHARS_PARTITION_COUNT);
    let replicas_fmt = format_padded(&topic.replica_count.to_string(), CHARS_REPLICA_COUNT);
    let offsets_fmt = format_padded(&topic.offset_sum.to_string(), CHARS_SUM_OFFSETS);

    format!(
        "{} {} {} {}",
        name_fmt, partitions_fmt, replicas_fmt, offsets_fmt
    )
}

// Topic View helpers

// TODO: pass in topic offsets so we can display partition lag
fn on_select_consumer_group(siv: &mut Cursive, consumer_group_name: &str, offset_map: OffsetMap) {
    let consumer_group_view = Dialog::around(
        LinearLayout::vertical()
            .child(TextView::new("Consumer Group").effect(Effect::Bold))
            .child(DummyView)
            .child(
                TextView::new(format_consumer_group_partition_list_headers()).effect(Effect::Bold),
            )
            .child(ScrollView::new(TextView::new(
                format_consumer_group_partition_list(&offset_map),
            ))),
    )
    .title(format!("Consumer Group: {}", consumer_group_name))
    .button("Back", |s| {
        s.pop_layer();
    });

    siv.add_layer(consumer_group_view);
}

fn format_topic_details(topic: &TopicDetails) -> String {
    let replica_count = topic.replicas.len();
    let partition_count = topic.partitions.len();
    let sum_offsets: i64 = topic
        .partitions
        .iter()
        .fold(0, |sum, partition| partition.offset + sum);
    format!(
        "Replication: {}\nPartitions: {}\nSum of Offsets: {}",
        replica_count, partition_count, sum_offsets
    )
}

fn format_partition_list_headers() -> String {
    let id_fmt = format_padded("Partition", CHARS_PARTITION_ID);
    let offset_fmt = format_padded("Offset", CHARS_PARTITION_OFFSET);

    format!("{} {}", id_fmt, offset_fmt)
}

fn format_partition_list(partitions: &Vec<TopicPartitionDetails>) -> String {
    let result_lines: Vec<String> = partitions
        .iter()
        .map(|partition| {
            let id_fmt = format_padded(&partition.id.to_string(), CHARS_PARTITION_ID);
            let offset_fmt = format_padded(&partition.offset.to_string(), CHARS_PARTITION_OFFSET);

            format!("{} {}", id_fmt, offset_fmt)
        })
        .collect();

    result_lines.join("\n")
}

fn format_consumer_group_list_headers() -> String {
    let name_fmt = format_padded("Consumer Group", CHARS_CG_NAME);
    let offset_fmt = format_padded("Offsets", CHARS_CG_SUM_OFFSETS);

    format!("{} {}", name_fmt, offset_fmt)
}

fn format_consumer_group(cg_name: &str, offset_map: &OffsetMap) -> String {
    let sum_offsets = offset_map.get_summed_offsets();

    let name_fmt = format_padded(cg_name, CHARS_CG_NAME);
    let offset_fmt = format_padded(&sum_offsets.to_string(), CHARS_CG_SUM_OFFSETS);

    format!("{} {}", name_fmt, offset_fmt)
}

// Consumer Group View helpers

fn format_consumer_group_partition_list_headers() -> String {
    let partition_fmt = format_padded("Partition", CHARS_PARTITION_ID);
    let offset_fmt = format_padded("Offset", CHARS_PARTITION_OFFSET);
    let lag_fmt = format_padded("Lag", CHARS_PARTITION_LAG);

    format!("{} {} {}", partition_fmt, offset_fmt, lag_fmt)
}

fn format_consumer_group_partition_list(offset_map: &OffsetMap) -> String {
    // FIXME: replace this with the actual topic partition count
    let mock_partition_count = 50;

    let result_lines: Vec<String> = (0..mock_partition_count)
        .map(|partition_id| {
            let cg_offset = offset_map.get(partition_id);

            let partition_fmt = format_padded(&partition_id.to_string(), CHARS_PARTITION_ID);
            let offset_fmt = format_padded(&cg_offset.to_string(), CHARS_PARTITION_OFFSET);
            // TODO: show CG lag on each partition
            let lag_fmt = format_padded("TODO", CHARS_PARTITION_LAG);

            format!("{} {} {}", partition_fmt, offset_fmt, lag_fmt)
        })
        .collect();

    result_lines.join("\n")
}
