//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate ipc_channel;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::TryRecvError;
use coordinator::ipc_channel::ipc::channel;

use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

/// CoordinatorState
/// States for 2PC state machine
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    ReceivedRequest,
    ProposalSent,
    ReceivedVotesAbort,
    ReceivedVotesCommit,
    SentGlobalDecision
}

/// Coordinator
/// Struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    running: Arc<AtomicBool>,
    log: oplog::OpLog,
    clients: HashMap<String, (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
    participants: HashMap<String, (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
    committed_ops: u64,
    aborted_ops: u64,
    unknown_ops: u64,
}

///
/// Coordinator
/// Implementation of coordinator functionality
/// Required:
/// 1. new -- Constructor
/// 2. protocol -- Implementation of coordinator side of protocol
/// 3. report_status -- Report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- What to do when a participant joins
/// 5. client_join -- What to do when a client joins
///
impl Coordinator {

    ///
    /// new()
    /// Initialize a new coordinator
    ///
    /// <params>
    ///     log_path: directory for log files --> create a new log there.
    ///     r: atomic bool --> still running?
    ///
    pub fn new(
        log_path: String,
        r: &Arc<AtomicBool>) -> Coordinator {

        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r.clone(),
            // TODO
            clients: HashMap::new(),
            participants: HashMap::new(),
            committed_ops: 0,
            aborted_ops: 0,
            unknown_ops: 0,
        }
    }

    ///
    /// participant_join()
    /// Adds a new participant for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn participant_join(&mut self, name: &String, tx: Sender<ProtocolMessage>, rx: Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        self.participants.insert(name.clone(), (tx, rx));
        info!("Participant {} joined successfully.", name);
    }

    ///
    /// client_join()
    /// Adds a new client for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn client_join(&mut self, name: &String, tx: Sender<ProtocolMessage>, rx: Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        // insert to the clients hashmap when joining to coordinator
        self.clients.insert(name.clone(), (tx, rx));
        info!("Client {} joined successfully.", name);
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        println!(
            "coordinator     :    C:{:6}    A:{:6}    U:{:6}",
            self.committed_ops, self.aborted_ops, self.unknown_ops
        );
    }

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        while self.running.load(Ordering::SeqCst) {
            let mut made_progress = false;

            // Iterate all clients once per tick
            let client_keys: Vec<String> = self.clients.keys().cloned().collect();
            for client_key in client_keys {
                let handled = if let Some((to_client, from_client)) = self.clients.get_mut(&client_key) {
                    match from_client.try_recv() {
                        Ok(request) => {
                            info!("Coordinator received request from client: {}", client_key);
                            self.state = CoordinatorState::ReceivedRequest;

                            // Send proposal to all participants
                            for (participant_name, (p_tx, _p_rx)) in self.participants.iter() {
                                let proposal = ProtocolMessage::generate(
                                    MessageType::CoordinatorPropose,
                                    request.txid.clone(),
                                    client_key.clone(),
                                    0,
                                );
                                if let Err(e) = p_tx.send(proposal) {
                                    error!("Failed to send proposal to participant {}: {:?}", participant_name, e);
                                }
                            }
                            self.state = CoordinatorState::ProposalSent;

                            // collect votes with a deadline
                            let mut votes = Vec::new();
                            let mut pending: std::collections::HashSet<String> =
                                self.participants.keys().cloned().collect();
                            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(1000);

                            while !pending.is_empty() && std::time::Instant::now() < deadline {
                                for (pname, (_p_tx, p_rx)) in self.participants.iter_mut() {
                                    if !pending.contains(pname) {
                                        continue;
                                    }
                                    match p_rx.try_recv() {
                                        Ok(vote) => {
                                            if vote.mtype == MessageType::ParticipantVoteCommit
                                                || vote.mtype == MessageType::ParticipantVoteAbort
                                            {
                                                info!("Received vote from participant {}: {:?}", pname, vote.mtype);
                                                votes.push((pname.clone(), vote));
                                                pending.remove(pname);
                                            }
                                        }
                                        Err(TryRecvError::Empty) => {}
                                        Err(e) => {
                                            error!("Error receiving vote from {}: {:?}", pname, e);
                                            pending.remove(pname);
                                        }
                                    }
                                }
                                if !pending.is_empty() {
                                    std::thread::sleep(std::time::Duration::from_millis(10));
                                }
                            }

                            if !pending.is_empty() {
                                error!("Timeout waiting for votes from participants: {:?}", pending);
                                self.state = CoordinatorState::ReceivedVotesAbort;
                            }

                            // decision phase
                            let mut global_decision = MessageType::CoordinatorCommit;
                            for (_pname, vote) in &votes {
                                if vote.mtype != MessageType::ParticipantVoteCommit {
                                    global_decision = MessageType::CoordinatorAbort;
                                    self.state = CoordinatorState::ReceivedVotesAbort;
                                    break;
                                }
                            }
                            if global_decision == MessageType::CoordinatorCommit && pending.is_empty() {
                                self.state = CoordinatorState::ReceivedVotesCommit;
                            } else {
                                global_decision = MessageType::CoordinatorAbort;
                            }

                            match global_decision {
                                MessageType::CoordinatorCommit => self.committed_ops += 1,
                                MessageType::CoordinatorAbort => self.aborted_ops += 1,
                                _ => self.unknown_ops += 1,
                            }

                            self.log.append(
                                global_decision,
                                request.txid.clone(),
                                "coordinator".to_string(),
                                0,
                            );

                            // Notify participants
                            for (participant_name, (p_tx, _p_rx)) in self.participants.iter() {
                                let decision = ProtocolMessage::generate(
                                    global_decision,
                                    request.txid.clone(),
                                    client_key.clone(),
                                    0,
                                );
                                if let Err(e) = p_tx.send(decision) {
                                    error!("Failed to send decision to participant {}: {:?}", participant_name, e);
                                }
                            }
                            self.state = CoordinatorState::SentGlobalDecision;

                            // Notify client with result
                            let client_result = match global_decision {
                                MessageType::CoordinatorCommit => MessageType::ClientResultCommit,
                                _ => MessageType::ClientResultAbort,
                            };
                            let result_msg = ProtocolMessage::generate(
                                client_result,
                                request.txid.clone(),
                                "coordinator".to_string(),
                                0,
                            );
                            if let Err(e) = to_client.send(result_msg) {
                                error!("Failed to send result to client {}: {:?}", client_key, e);
                            }
                            true
                        }
                        Err(TryRecvError::Empty) => false,
                        Err(e) => {
                            error!("Error receiving client request: {:?}", e);
                            false
                        }
                    }
                } else {
                    false
                };
                if handled { made_progress = true; }
            }

            if !made_progress {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }

        self.report_status();
    }
}
