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
            let mut progressed = false;

            // iterate all clients
            let client_keys: Vec<String> = self.clients.keys().cloned().collect();
            for client_key in client_keys {
                // check if there is a request from this client
                let handled = if let Some((to_client, from_client)) = self.clients.get_mut(&client_key) {
                    match from_client.try_recv() {
                        Ok(request) => {
                            info!("Coordinator received request from client: {}", client_key);
                            self.state = CoordinatorState::ReceivedRequest;

                            // send proposal to all participants
                            for (participant_name, (p_tx, _p_rx)) in self.participants.iter() {
                                let proposal = ProtocolMessage::generate(
                                    MessageType::CoordinatorPropose,
                                    request.txid.clone(),
                                    client_key.clone(),
                                    0,
                                );
                                p_tx.send(proposal).expect("Failed to send proposal to participant");
                            }
                            self.state = CoordinatorState::ProposalSent;

                            // collect votes with a deadline
                            let mut votes = Vec::new();
                            // set of participants we are still waiting for votes from
                            let mut pending: HashSet<String> = self.participants.keys().cloned().collect();

                            // wait for votes until all received
                            while !pending.is_empty() {
                                // print how many participants we are waiting for
                                for (pname, (_p_tx, p_rx)) in self.participants.iter_mut() {
                                    // skip if already received vote from this participant - probably not needed but keeps it safe
                                    if !pending.contains(pname) {
                                        continue;
                                    }
                                    match p_rx.try_recv() {
                                        Ok(vote) => {
                                            // only consider commit/abort votes or send failure messages
                                            if vote.mtype == MessageType::ParticipantVoteCommit
                                                || vote.mtype == MessageType::ParticipantVoteAbort || vote.mtype == MessageType::SendFailure
                                            {
                                                info!("Received vote from participant {}: {:?}", pname, vote.mtype);
                                                // store the vote
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
                                    thread::sleep(Duration::from_millis(10));
                                }
                            }

                            // log any participants that did not respond in time
                            if !pending.is_empty() {
                                error!("Timeout waiting for votes from participants: {:?}", pending);
                                self.state = CoordinatorState::ReceivedVotesAbort;
                            }

                            // decision phase
                            let mut global_decision = MessageType::CoordinatorCommit;
                            for (_pname, vote) in &votes {
                                // if any participant voted to abort, abort
                                if vote.mtype == MessageType::ParticipantVoteAbort || vote.mtype == MessageType::SendFailure {
                                    global_decision = MessageType::CoordinatorAbort;
                                    break;
                                }
                            }
                            // if there are still pending participants, abort
                            if global_decision == MessageType::CoordinatorCommit && pending.is_empty() {
                                self.state = CoordinatorState::ReceivedVotesCommit;
                                // all participants voted to commit so commit
                            } else {
                                global_decision = MessageType::CoordinatorAbort;
                            }

                            match global_decision {
                                MessageType::CoordinatorCommit => self.committed_ops += 1,
                                MessageType::CoordinatorAbort => self.aborted_ops += 1,
                                // doest seem to be a case where unknown_ops would increase - leaving this for completeness
                                _ => self.unknown_ops += 1,
                            }

                            // log the global decision
                            self.log.append(
                                global_decision,
                                request.txid.clone(),
                                "coordinator".to_string(),
                                0,
                            );

                            // iterate all participants to send global decision to them
                            for (_participant_name, (p_tx, _p_rx)) in self.participants.iter() {
                                // generate the decision message
                                let decision = ProtocolMessage::generate(
                                    global_decision,
                                    request.txid.clone(),
                                    client_key.clone(),
                                    0,
                                );
                                // send decision to participant
                                p_tx.send(decision).expect("Failed to send global decision to participant");
                            }
                            // change state to SentGlobalDecision
                            self.state = CoordinatorState::SentGlobalDecision;

                            // notify client with result
                            let client_result = match global_decision {
                                MessageType::CoordinatorCommit => MessageType::ClientResultCommit,
                               _ => MessageType::ClientResultAbort,
                            };
                            // make result message
                            let result_msg = ProtocolMessage::generate(
                                client_result,
                                request.txid.clone(),
                                "coordinator".to_string(),
                                0,
                            );
                            // send result to client
                            to_client.send(result_msg).expect("Failed to send global decision to participant");
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
                if handled {
                    progressed = true;
                }
            }

            if !progressed {
                thread::sleep(Duration::from_millis(20));
            }
        }
        self.report_status();
    }
}
