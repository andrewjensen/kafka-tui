use cursive::theme::Effect;
use cursive::views::{Dialog, DummyView, LinearLayout, ScrollView, SelectView, TextView};
use cursive::Cursive;
use std::sync::Arc;

use crate::formatting::format_padded;
use crate::kafka::{OffsetMap, TopicDetails};
use crate::tui::render_consumer_group_view;
use crate::Model;

const CHARS_PARTITION_ID: usize = 10;
const CHARS_PARTITION_OFFSET: usize = 10;
const CHARS_CG_NAME: usize = 40;
const CHARS_CG_SUM_OFFSETS: usize = 9;

pub fn render_topic_view(siv: &mut Cursive, topic_name: &str) {
    let model = siv.user_data::<Model>().unwrap();
    let model_ref = Arc::clone(&model);
    let model_inner = model_ref.lock().unwrap();

    let topic: &TopicDetails = model_inner.cluster_summary.topics.get(topic_name).unwrap();
    let topic_cg_state = model_inner
        .cluster_cg_state
        .topics
        .get(&topic_name.to_string());

    let cg_view = match topic_cg_state {
        Some(topic_state) => LinearLayout::vertical()
            .child(TextView::new(format_consumer_group_list_headers()).effect(Effect::Bold))
            .child(ScrollView::new(
                SelectView::new()
                    .with_all(topic_state.consumer_group_states.iter().map(
                        |(cg_name, cg_offset_map)| {
                            (
                                format_consumer_group(cg_name, &cg_offset_map),
                                (topic_name.to_string(), cg_name.clone()),
                            )
                        },
                    ))
                    .on_submit(on_select_consumer_group),
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

fn on_select_consumer_group(siv: &mut Cursive, (topic_name, cg_name): &(String, String)) {
    render_consumer_group_view(siv, topic_name, cg_name);
}

fn format_topic_details(topic: &TopicDetails) -> String {
    let replica_count = topic.replicas.len();
    let partition_count = topic.partitions.partition_count.unwrap();
    let sum_offsets: i64 = topic.partitions.get_summed_offsets();
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

fn format_partition_list(partitions: &OffsetMap) -> String {
    let partition_count = partitions.partition_count.unwrap() as i32;

    let result_lines: Vec<String> = (0..partition_count)
        .map(|partition_id| {
            let offset = partitions.get(partition_id);

            let id_fmt = format_padded(&partition_id.to_string(), CHARS_PARTITION_ID);
            let offset_fmt = format_padded(&offset.to_string(), CHARS_PARTITION_OFFSET);

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
