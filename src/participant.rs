//!
//! participant.rs
//! Implementation of 2PC participant
//!
extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::thread;
use ipc_channel::ipc::IpcError;
use participant::rand::prelude::*;
use participant::ipc_channel::ipc::IpcReceiver as Receiver;
use participant::ipc_channel::ipc::TryRecvError;
use participant::ipc_channel::ipc::IpcSender as Sender;

use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

///
/// ParticipantState
/// enum for Participant 2PC state machine
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantState {
    Quiescent,
    ReceivedP1,
    VotedAbort,
    VotedCommit,
    AwaitingGlobalDecision,
}

///
/// Participant
/// Structure for maintaining per-participant state and communication/synchronization objects to/from coordinator
///
#[derive(Debug)]
pub struct Participant {
    id_str: String,
    state: ParticipantState,
    log: oplog::OpLog,
    running: Arc<AtomicBool>,
    send_success_prob: f64,
    operation_success_prob: f64,
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>,
    committed_ops: u64,
    aborted_ops: u64,
    unknown_ops: u64,
}

///
/// Participant
/// Implementation of participant for the 2PC protocol
/// Required:
/// 1. new -- Constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- Implements participant side protocol for 2PC
///
impl Participant {

    ///
    /// new()
    ///
    /// Return a new participant, ready to run the 2PC protocol with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course.
    ///
    pub fn new(
        id_str: String,
        log_path: String,
        r: Arc<AtomicBool>,
        send_success_prob: f64,
        operation_success_prob: f64,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) -> Participant {

        Participant {
            id_str,
            state: ParticipantState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r,
            send_success_prob,
            operation_success_prob,
            // TODO
            tx,
            rx,
            committed_ops: 0,
            aborted_ops: 0,
            unknown_ops: 0,
        }
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator. This can fail depending on
    /// the success probability. For testing purposes, make sure to not specify
    /// the -S flag so the default value of 1 is used for failproof sending.
    ///
    /// HINT: You will need to implement the actual sending
    ///
    pub fn send(&mut self, pm: ProtocolMessage) {
        let x: f64 = random();
        if x <= self.send_success_prob {
            // TODO: Send success
            // check if running before sending - if not running, do not send
            self.tx.send(pm).unwrap();
        } else {
            // TODO: Send fail
            let fail_msg = ProtocolMessage::generate(
                MessageType::SendFailure,
                pm.txid,
                self.id_str.clone(),
                0,
            );
            // I would think we would increment unknown_ops here, but the assignment is unclear, leaving it out for now
            // incrementing here also would not be in line with how we increment committed/aborted ops upon receiving global decision
            // so not sure where to go from there
            // self.unknown_ops += 1;
            self.tx.send(fail_msg).unwrap();
        }
    }

    ///
    /// perform_operation
    /// Perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the
    /// command-line option success_probability.
    ///
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic.
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than
    ///       bool if it's more convenient for your design).
    ///
    // Honor operation_success_prob and return the outcome.
    pub fn perform_operation(&mut self, request_option: &Option<ProtocolMessage>) -> bool {
        let tx_id = match request_option {
            Some(req) => req.txid.clone(),
            None => "unknown_tx".to_string(),
        };
        info!("{}::Performing operation for tx_id {}", self.id_str, tx_id);
        let x: f64 = random();
        let ok = x <= self.operation_success_prob;
        ok
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        println!(
            "{:16}:    C:{:6}    A:{:6}    U:{:6}",
            self.id_str, self.committed_ops, self.aborted_ops, self.unknown_ops
        );
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        info!("{}::Waiting for exit signal", self.id_str.clone());

        // TODO
        while self.running.load(Ordering::SeqCst) {
            // sleep to avoid tight loop
            thread::sleep(Duration::from_millis(1));
        }

        info!("{}::Exiting", self.id_str.clone());
    }

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        info!("{}::Beginning protocol", self.id_str);

        loop {
            match self.rx.try_recv() {
                Ok(msg) => {
                    match msg.mtype {
                        // coordinator has proposed a transaction
                        MessageType::CoordinatorPropose => {
                            info!("{}::Received Coordinator PROPOSE for txid={}", self.id_str, msg.txid);
                            self.state = ParticipantState::ReceivedP1;

                            // perform the operation
                            let op_ok = self.perform_operation(&Some(msg.clone()));
                            // decide vote based on operation outcome
                            let vote_type = if op_ok {
                                self.state = ParticipantState::VotedCommit;
                                MessageType::ParticipantVoteCommit
                            } else {
                                self.state = ParticipantState::VotedAbort;
                                MessageType::ParticipantVoteAbort
                            };

                            // append to log
                            self.log.append(
                                vote_type,
                                msg.txid.clone(),
                                self.id_str.clone(),
                                0,
                            );

                            // make vote ProtocolMessage
                            let vote = ProtocolMessage::generate(
                                vote_type,
                                msg.txid.clone(),
                                self.id_str.clone(),
                                0,
                            );

                            self.send(vote);
                            // set state to awaiting global decision
                            self.state = ParticipantState::AwaitingGlobalDecision;
                        }
                        // coordinator has sent global commit
                        MessageType::CoordinatorCommit => {
                            info!("{}::Received Coordinator COMMIT for txid={}", self.id_str, msg.txid);
                            self.state = ParticipantState::Quiescent;
                            // log the commit and increment committed ops
                            self.committed_ops += 1;
                            self.log.append(
                                MessageType::CoordinatorCommit,
                                msg.txid.clone(),
                                self.id_str.clone(),
                                0,
                            );
                        }
                        // coordinator has sent global abort
                        MessageType::CoordinatorAbort => {
                            info!("{}::Received Coordinator ABORT for txid={}", self.id_str, msg.txid);
                            self.state = ParticipantState::Quiescent;
                            // log the abort and increment aborted ops
                            self.aborted_ops += 1;
                            self.log.append(
                                MessageType::CoordinatorAbort,
                                msg.txid.clone(),
                                self.id_str.clone(),
                                0,
                            );
                        }
                        // coordinator is shutting down
                        MessageType::CoordinatorExit => {
                            info!("{}::Received EXIT signal from Coordinator", self.id_str);
                            self.running.store(false, Ordering::SeqCst);
                            break;
                        }
                        _ => {
                            println!("{}::Ignoring message {:?}", self.id_str, msg.mtype);
                        }
                    }
                }
                Err(TryRecvError::Empty) => {
                    // no message available so sleep to avoid tight loop
                    thread::sleep(Duration::from_millis(1));
                }
                Err(TryRecvError::IpcError(e)) => {
                    // coordinator side may still be starting up so sleep to avoid tight loop
                    error!("{}::IPC receive error: {:?}", self.id_str, e);
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
        self.wait_for_exit_signal();
        self.report_status();
    }
}
