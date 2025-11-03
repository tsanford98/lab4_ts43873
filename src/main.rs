#[macro_use]
extern crate log;
extern crate stderrlog;
extern crate clap;
extern crate ctrlc;
extern crate ipc_channel;
use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::process::{Child,Command};
use ipc_channel::ipc::IpcSender as Sender;
use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcOneShotServer;
use ipc_channel::ipc::channel;

pub mod message;
pub mod oplog;
pub mod coordinator;
pub mod participant;
pub mod client;
pub mod checker;
pub mod tpcoptions;
use message::ProtocolMessage;

///
/// pub fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     child_opts: CLI options for child process
///
/// 1. Set up IPC
/// 2. Spawn a child process using the child CLI options
/// 3. Do any required communication to set up the parent / child communication channels
/// 4. Return the child process handle and the communication channels for the parent
///
/// HINT: You can change the signature of the function if necessary
///
fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let (server, server_name) = IpcOneShotServer::<(Sender<ProtocolMessage>, Sender<Sender<ProtocolMessage>>)>::new()
        .expect("Failed to create server");

    child_opts.ipc_path = server_name.clone();

    let child = Command::new(env::current_exe().unwrap())
        .args(child_opts.as_vec())
        .spawn()
        .expect("Failed to execute child process");

    let (_, (parent_to_child_tx, mailbox_to_child)) =
        server.accept().expect("Failed to accept child bootstrap payload");

    // let (tx, rx) = channel().unwrap();
    // TODO
    let (child_to_parent_tx, child_to_parent_rx) = channel::<ProtocolMessage>().unwrap();
    mailbox_to_child
        .send(child_to_parent_tx)
        .expect("Failed to send child_to_parent_tx to child");


    (child, parent_to_child_tx, child_to_parent_rx)
}

///
/// pub fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     opts: CLI options for this process
///
/// 1. Connect to the parent via IPC
/// 2. Do any required communication to set up the parent / child communication channels
/// 3. Return the communication channels for the child
///
/// HINT: You can change the signature of the function if necessasry
///
fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    // let (tx, rx) = channel().unwrap();

    // TODO
    let bootstrap: Sender<(Sender<ProtocolMessage>, Sender<Sender<ProtocolMessage>>)> =
        Sender::connect(opts.ipc_path.clone()).expect("Failed to connect to rendezvous");
    let (parent_to_child_tx, parent_to_child_rx) = channel::<ProtocolMessage>().unwrap();
    let (mailbox_tx, mailbox_rx) = channel::<Sender<ProtocolMessage>>().unwrap();
    bootstrap
        .send((parent_to_child_tx, mailbox_tx))
        .expect("Failed to send bootstrap payload to parent");
    let child_to_parent_tx = mailbox_rx
        .recv()
        .expect("Failed to receive child_to_parent_tx from parent");



    (child_to_parent_tx, parent_to_child_rx)
}

///
/// pub fn run(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Creates a new coordinator
/// 2. Spawns and connects to new clients processes and then registers them with
///    the coordinator
/// 3. Spawns and connects to new participant processes and then registers them
///    with the coordinator
/// 4. Starts the coordinator protocol
/// 5. Wait until the children finish execution
///
fn run(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let coord_log_path = format!("{}//{}", opts.log_path, "coordinator.log");

    // TODO
    let mut coordinator = coordinator::Coordinator::new(coord_log_path, &running);
    // let (server, server_name) = IpcOneShotServer::<(Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>::new().unwrap();
    // set run_opts.ipc_path to server_name
    let mut run_opts = opts.clone();
    // run_opts.ipc_path = server_name;
    let num_requests_per_client = run_opts.num_requests;
    let mut children = Vec::new();
    // num may need to be set to num request / num clients but im not sure at this moment
    for i in 0..opts.num_clients {
        let mut child_opts = tpcoptions::TPCOptions {
            mode: "client".to_string(),
            num: i,
            ipc_path: run_opts.ipc_path.clone(),
            num_requests: num_requests_per_client,
            ..opts.clone()
        };
        let (child, tx, rx) = spawn_child_and_connect(&mut child_opts);
        let client_id_str = format!("client_{}", i);
        coordinator.client_join(&client_id_str, tx, rx);
        children.push(child);
    }
    for i in 0..opts.num_participants {
        let mut child_opts = tpcoptions::TPCOptions {
            mode: "participant".to_string(),
            num: i,
            ipc_path: run_opts.ipc_path.clone(),
            ..opts.clone()
        };
        let (child, tx, rx) = spawn_child_and_connect(&mut child_opts);
        let participant_id_str = format!("participant_{}", i);
        coordinator.participant_join(&participant_id_str, tx, rx);
        children.push(child);
    }
    coordinator.protocol();

    // Wait for all child processes to finish
    for mut child in children {
        if let Err(e) = child.wait() {
            error!("Failed to wait for child process: {:?}", e);
        }
    }
}

///
/// pub fn run_client(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new client
/// 3. Starts the client protocol
///
fn run_client(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    // TODO
    let (tx, rx) = connect_to_coordinator(opts);
    let mut client = client::Client::new(
        format!("client_{}", opts.num),
        running,
        tx,
        rx,
    );
    // info!("Client {} starting protocol with {} requests", opts.num, opts.num_requests);
    client.protocol(opts.num_requests);
}

///
/// pub fn run_participant(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new participant
/// 3. Starts the participant protocol
///
fn run_participant(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let participant_id_str = format!("participant_{}", opts.num);
    let participant_log_path = format!("{}//{}.log", opts.log_path, participant_id_str);

    // TODO
    let (tx, rx) = connect_to_coordinator(opts);
    let mut participant = participant::Participant::new(
        participant_id_str,
        participant_log_path,
        running,
        opts.send_success_probability,
        opts.operation_success_probability,
        tx,
        rx,
    );
    participant.protocol();
}

fn main() {
    // Parse CLI arguments
    let opts = tpcoptions::TPCOptions::new();
    // Set-up logging and create OpLog path if necessary
    stderrlog::new()
            .module(module_path!())
            .quiet(false)
            .timestamp(stderrlog::Timestamp::Millisecond)
            .verbosity(opts.verbosity)
            .init()
            .unwrap();
    match fs::create_dir_all(opts.log_path.clone()) {
        Err(e) => error!("Failed to create log_path: \"{:?}\". Error \"{:?}\"", opts.log_path, e),
        _ => (),
    }

    // Set-up Ctrl-C / SIGINT handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let m = opts.mode.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        if m == "run" {
            print!("\n");
        }
    }).expect("Error setting signal handler!");

    // Execute main logic
    match opts.mode.as_ref() {
        "run" => run(&opts, running),
        "client" => run_client(&opts, running),
        "participant" => run_participant(&opts, running),
        "check" => checker::check_last_run(opts.num_clients, opts.num_requests, opts.num_participants, &opts.log_path),
        _ => panic!("Unknown mode"),
    }
}
