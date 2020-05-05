mod kafka;

use kafka::fetch_cluster_metadata;

const MOCK_BROKERS: &str = "localhost:9092";

fn main() {
    let cluster_summary = fetch_cluster_metadata(MOCK_BROKERS);
    println!("{:#?}", cluster_summary);
}
