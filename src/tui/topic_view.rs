use cursive::theme::Effect;
use cursive::views::{Dialog, DummyView, LinearLayout, ScrollView, SelectView, TextView};
use cursive::Cursive;

use crate::formatting::format_padded;
use crate::kafka::{OffsetMap, TopicDetails, TopicPartitionDetails, TopicState};
use crate::tui::render_consumer_group_view;

const CHARS_PARTITION_ID: usize = 10;
const CHARS_PARTITION_OFFSET: usize = 10;
const CHARS_CG_NAME: usize = 40;
const CHARS_CG_SUM_OFFSETS: usize = 9;

pub fn render_topic_view(siv: &mut Cursive, topic: TopicDetails, topic_state: Option<TopicState>) {
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
                        render_consumer_group_view(s, cg_name, offset_map);
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
