use cursive::theme::Effect;
use cursive::views::{Dialog, DummyView, LinearLayout, ScrollView, SelectView, TextView};
use cursive::Cursive;

use crate::MOCK_BROKERS;

use crate::formatting::format_padded;
use crate::kafka::{
    fetch_topic_details, ClusterConsumerOffsetState, ClusterSummary, TopicState, TopicSummary,
};
use crate::tui::render_topic_view;

const CHARS_TOPIC_NAME: usize = 40;
const CHARS_PARTITION_COUNT: usize = 11;
const CHARS_REPLICA_COUNT: usize = 9;
const CHARS_SUM_OFFSETS: usize = 9;

pub fn render_summary_view(
    siv: &mut Cursive,
    cluster_summary: ClusterSummary,
    cluster_cg_state: ClusterConsumerOffsetState,
) {
    let mut topics = cluster_summary.topics;
    topics.sort_by(|a, b| a.name.cmp(&b.name));

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
                        render_topic_view(s, topic, topic_cg_state);
                    }),
            )),
    )
    .title("Kafka");

    siv.add_layer(summary_view);
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
