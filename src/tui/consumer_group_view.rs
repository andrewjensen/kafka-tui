use cursive::theme::Effect;
use cursive::views::{DummyView, LinearLayout, ResizedView, ScrollView, TextView};
use cursive::Cursive;
use std::sync::Arc;

use crate::kafka::OffsetMap;
use crate::text_utils::format_padded;
use crate::Model;

const CHARS_PARTITION_ID: usize = 10;
const CHARS_PARTITION_OFFSET: usize = 10;
const CHARS_PARTITION_LAG: usize = 10;

pub fn render_consumer_group_view(siv: &mut Cursive, topic_name: &str, cg_name: &str) {
    let model = siv.user_data::<Model>().unwrap();
    let model_ref = Arc::clone(&model);
    let model_inner = model_ref.lock().unwrap();

    let topic = model_inner.cluster_summary.topics.get(topic_name).unwrap();

    let topic_offsets = &topic.partitions;

    let topic_cg_state = model_inner
        .cluster_cg_state
        .topics
        .get(&topic_name.to_string())
        .unwrap();

    let cg_offsets = topic_cg_state.consumer_group_states.get(cg_name).unwrap();

    let consumer_group_view = ResizedView::with_full_screen(
        LinearLayout::vertical()
            .child(TextView::new(format!("Consumer Group: {}", cg_name)).effect(Effect::Bold))
            .child(DummyView)
            .child(
                TextView::new(format_consumer_group_partition_list_headers()).effect(Effect::Bold),
            )
            .child(ScrollView::new(TextView::new(
                format_consumer_group_partition_list(topic_offsets, cg_offsets),
            ))),
    );

    siv.add_fullscreen_layer(consumer_group_view);
}

fn format_consumer_group_partition_list_headers() -> String {
    let partition_fmt = format_padded("Partition", CHARS_PARTITION_ID);
    let offset_fmt = format_padded("Offset", CHARS_PARTITION_OFFSET);
    let lag_fmt = format_padded("Lag", CHARS_PARTITION_LAG);

    format!("{} {} {}", partition_fmt, offset_fmt, lag_fmt)
}

fn format_consumer_group_partition_list(
    topic_offsets: &OffsetMap,
    cg_offsets: &OffsetMap,
) -> String {
    let partition_count = topic_offsets.partition_count.unwrap() as i32;

    let result_lines: Vec<String> = (0..partition_count)
        .map(|partition_id| {
            let partition_offset = topic_offsets.get(partition_id);
            let cg_offset = cg_offsets.get(partition_id);
            let cg_lag = partition_offset - cg_offset;

            let partition_fmt = format_padded(&partition_id.to_string(), CHARS_PARTITION_ID);
            let offset_fmt = format_padded(&cg_offset.to_string(), CHARS_PARTITION_OFFSET);
            let lag_fmt = format_padded(&cg_lag.to_string(), CHARS_PARTITION_LAG);

            format!("{} {} {}", partition_fmt, offset_fmt, lag_fmt)
        })
        .collect();

    result_lines.join("\n")
}
