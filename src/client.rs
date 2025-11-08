//!
//! client.rs
//! Implementation of 2PC client
//!
extern crate ipc_channel;
extern crate log;
extern crate stderrlog;

use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;

use client::ipc_channel::ipc::IpcReceiver as Receiver;
use client::ipc_channel::ipc::TryRecvError;
use client::ipc_channel::ipc::IpcSender as Sender;

use message;
use message::{MessageType, ProtocolMessage};
use message::RequestStatus;

// Client state and primitives for communicating with the coordinator
#[derive(Debug)]
pub struct Client {
    pub id_str: String,
    pub running: Arc<AtomicBool>,
    pub num_requests: u32,
    pub tx: Sender<ProtocolMessage>,
    pub rx: Receiver<ProtocolMessage>,
    successful_ops: u64,
    failed_ops: u64,
    unknown_ops: u64,
}

///
/// Client Implementation
/// Required:
/// 1. new -- constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown
/// 3. pub fn protocol(&mut self, n_requests: i32) -- Implements client side protocol
///
impl Client {

    ///
    /// new()
    ///
    /// Constructs and returns a new client, ready to run the 2PC protocol
    /// with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->client and client->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor
    ///
    pub fn new(
        id_str: String,
        running: Arc<AtomicBool>,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) -> Client {
        Client {
            id_str,
            running,
            num_requests: 0,
            // TODO
            tx,
            rx,
            successful_ops: 0,
            failed_ops: 0,
            unknown_ops: 0,
        }
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        info!("{}::Waiting for exit signal", self.id_str.clone());

        // TODO
        // wait until running is false
        loop {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }

        info!("{}::Exiting", self.id_str.clone());
    }

    ///
    /// send_next_operation(&mut self)
    /// Send the next operation to the coordinator
    ///
    pub fn send_next_operation(&mut self) {

        // create a new request with a unique TXID.
        self.num_requests = self.num_requests + 1;
        // construct unique transaction id
        let txid = format!("{}_op_{}", self.id_str.clone(), self.num_requests);

        // create protocol message
        let pm = ProtocolMessage::generate(MessageType::ClientRequest,
                                                    txid.clone(),
                                                    self.id_str.clone(),
                                                    self.num_requests);
        info!("{}::Sending operation #{}", self.id_str.clone(), self.num_requests);
        // log the message being sent
        info!("{}::Sending Protocol Message: {:?}", self.id_str.clone(), pm);

        // TODO
        self.tx.send(pm).unwrap();


        info!("{}::Sent operation #{}", self.id_str.clone(), self.num_requests);
    }

    ///
    /// recv_result()
    /// Wait for the coordinator to respond with the result for the
    /// last issued request. Note that we assume the coordinator does
    /// not fail in this simulation
    ///
    pub fn recv_result(&mut self) {

        info!("{}::Receiving Coordinator Result", self.id_str.clone());

        // TODO
        match self.rx.recv() {
            Ok(pm) => {
                // log the message being received
                info!("{}::Client Received Message: {:?}", self.id_str, pm);
                match pm.mtype {
                    // update stats based on message type
                    MessageType::ClientResultCommit => self.successful_ops += 1,
                    MessageType::ClientResultAbort => self.failed_ops += 1,
                    MessageType::CoordinatorExit => {
                        // Exit signal from coordinator
                        self.running.store(false, Ordering::SeqCst);
                    },
                    // should not happen but keeping for completeness - might need to fix
                    _ => {
                        error!("{}::recv_result received unexpected message type: {:?}", self.id_str, pm.mtype);
                    },
                }
            }
            Err(e) => {
                // don't know if this is correct to put here or not? very unlikely to happen
                error!("{}::recv_result failed: {:?}", self.id_str, e);
                self.unknown_ops += 1;
            }
        }
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this client before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        println!(
            "{:16}:    C:{:6}    A:{:6}    U:{:6}",
            self.id_str, self.successful_ops, self.failed_ops, self.unknown_ops
        );
    }

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep issuing requests!
    /// HINT: if you've issued all your requests, wait for some kind of
    ///       exit signal before returning from the protocol method!
    ///
    pub fn protocol(&mut self, n_requests: u32) {

        // TODO
        for _ in 0..n_requests {
            if !self.running.load(Ordering::SeqCst) {
                break; // Exit early if the program is stopped
            }
            self.send_next_operation();
            self.recv_result();
        }
        self.wait_for_exit_signal();
        self.report_status();
    }
}
