use clap::{
    Arg,
    App,
};
use std::env;
use solistener::block_listener;

fn main() {
    let matches = App::new("Solistener")
        .version("0.1")
        .author("Diego Wilson <diego.wilson.solis@gmail.com>")
        .about("Listener for Solana transactions.")
        .arg(Arg::with_name("project")
            .long("project")
            .short("p")
            .default_value("datadragonio-stage")
            .value_name("PROJECT")
            .help("Name of the GCP project."))
        .arg(Arg::with_name("dataset")
            .long("dataset")
            .short("d")
            .default_value("solana_test")
            .value_name("DATASET")
            .help("Name of the dataset that transactions will be written to."))
        .arg(Arg::with_name("start_slot")
            .long("start-slot")
            .short("s")
            .value_name("SLOT")
            .help("Start processing blocks at this slot."))
        .arg(Arg::with_name("end_slot")
            .long("end-slot")
            .short("e")
            .value_name("SLOT")
            .help("Stop after processing the block at this slot."))
        .get_matches();

    env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .expect("Environment variable GOOGLE_APPLICATION_CREDENTIALS is required");

    let project_id = matches.value_of("project").unwrap();
    let dataset_id = matches.value_of("dataset").unwrap();

    let start_slot: Option<u64>;
    match matches.value_of("start_slot") {
        None => {
            start_slot = None;
        }
        Some(slot) => {
            let s: u64 = slot
                .parse()
                .expect("Start slot is not a valid number");
            start_slot = Some(s);
        }
    }

    let end_slot: Option<u64>;
    match matches.value_of("end_slot") {
        None => {
            end_slot = None;
        }
        Some(slot) => {
            let s: u64 = slot
                .parse()
                .expect("End slot is not a valid number");
            end_slot = Some(s);
        }
    }
    let mut processor = block_listener::Listener::new(
        project_id,
        dataset_id,
        start_slot,
        end_slot,
    );
    processor.listen();
}
