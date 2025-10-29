//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate ipc_channel;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

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
    participants: HashMap<String, Sender<ProtocolMessage>>,
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

        }
    }

    ///
    /// participant_join()
    /// Adds a new participant for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn participant_join(&mut self, name: &String, tx: Sender<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        self.participants.insert(name.clone(), tx);
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
        let successful_ops: u64 = 0;
        let failed_ops: u64 = 0;
        let unknown_ops: u64 = 0;

        println!("coordinator     :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", successful_ops, failed_ops, unknown_ops);
    }

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {

        // TODO
        while self.running.load(Ordering::SeqCst) {
            // Wait for a client request
            if let Some((client_name, (tx, rx))) = self.clients.iter_mut().next() {
                match rx.try_recv() {
                    Ok(request) => {
                        info!("Coordinator received request from client: {}", client_name);
                        self.state = CoordinatorState::ReceivedRequest;
                        // Proposal Phase
                        for (participant_name, participant_tx) in &self.participants {
                            let proposal = ProtocolMessage::generate(
                                MessageType::CoordinatorPropose,
                                request.txid.clone(),
                                client_name.clone(),
                                0,
                            );
                            if let Err(e) = participant_tx.send(proposal) {
                                error!("Failed to send proposal to participant {}: {:?}", participant_name, e);
                            }
                        }
                        self.state = CoordinatorState::ProposalSent;

                        let mut votes = Vec::new();
                        for (participant_name, participant_tx) in &self.participants {
                            match rx.try_recv() {
                                Ok(vote) => {
                                    if vote.mtype == MessageType::ParticipantVoteCommit
                                        || vote.mtype == MessageType::ParticipantVoteAbort
                                    {
                                        info!("Received vote from participant {}: {:?}", participant_name, vote);
                                        votes.push(vote);
                                    }
                                }
                                Err(TryRecvError::Empty) => {
                                    error!("Timeout waiting for vote from participant {}", participant_name);
                                    self.state = CoordinatorState::ReceivedVotesAbort;
                                    break;
                                }
                                Err(e) => {
                                    error!("Error receiving vote: {:?}", e);
                                    self.state = CoordinatorState::ReceivedVotesAbort;
                                    break;
                                }
                            }
                        }
                        // global decision made from votes
                        let mut global_decision = MessageType::CoordinatorCommit;
                        for vote in &votes {
                            // If any vote is abort, global decision is abort
                            if vote.mtype != MessageType::ParticipantVoteCommit {
                                global_decision = MessageType::CoordinatorAbort;
                                self.state = CoordinatorState::ReceivedVotesAbort;
                                break;
                            }
                        }
                        // Update state based on global decision -
                        if global_decision == MessageType::CoordinatorCommit {
                            self.state = CoordinatorState::ReceivedVotesCommit;
                        }

                        for (participant_name, participant_tx) in &self.participants {
                            let decision = ProtocolMessage::generate(
                                global_decision,
                                request.txid.clone(),
                                client_name.clone(),
                                0,
                            );
                            if let Err(e) = participant_tx.send(decision) {
                                error!("Failed to send decision to participant {}: {:?}", participant_name, e);
                            }
                        }
                        self.state = CoordinatorState::SentGlobalDecision;
                    }
                    Err(TryRecvError::Empty) => {
                        // No request received, continue
                        thread::sleep(Duration::from_millis(100));
                    }
                    Err(e) => {
                        error!("Error receiving client request: {:?}", e);
                    }
                }
            }
        }

        self.report_status();
    }
}
