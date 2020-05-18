use cursive::Cursive;
use std::sync::{Arc, Mutex};

mod formatting;
mod kafka;
mod tui;

use kafka::{
    fetch_cluster_metadata, fetch_consumer_offset_state, ClusterConsumerOffsetState, ClusterSummary,
};
use tui::render_summary_view;

pub const MOCK_BROKERS: &str = "localhost:9092";

pub struct ModelData {
    cluster_summary: ClusterSummary,
    cluster_cg_state: ClusterConsumerOffsetState,
}

type Model = Arc<Mutex<ModelData>>;

#[tokio::main]
async fn main() {
    println!("Fetching cluster metadata...");
    let cluster_summary = fetch_cluster_metadata(MOCK_BROKERS);

    println!("Fetching consumer group states...");
    let cluster_cg_state = fetch_consumer_offset_state().await;

    let mut siv = Cursive::default();

    let model_data = ModelData {
        cluster_summary: cluster_summary,
        cluster_cg_state: cluster_cg_state,
    };
    let model = Arc::new(Mutex::new(model_data));

    siv.set_user_data(Arc::clone(&model));

    siv.add_global_callback('q', |s| {
        let remaining_layers = s.screen().len();
        if remaining_layers == 1 {
            s.quit();
        } else {
            s.pop_layer().unwrap();
        }
    });

    render_summary_view(&mut siv);

    siv.run();
}
