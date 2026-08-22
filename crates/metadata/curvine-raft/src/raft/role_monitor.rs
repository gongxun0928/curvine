// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use curvine_core_error::CommonResult;
use curvine_runtime::sync::{StateCtl, StateListener, StateMonitor};
use num_enum::{FromPrimitive, IntoPrimitive};
use raft::{SoftState, StateRole};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Read-only handle onto the leadership epoch counter (see
/// `RoleMonitor::epoch_ctl`). Cloneable and cheap to poll.
#[derive(Clone)]
pub struct EpochCtl {
    epoch: Arc<AtomicU64>,
}

impl EpochCtl {
    fn new(epoch: Arc<AtomicU64>) -> Self {
        Self { epoch }
    }

    /// A private epoch counter that never advances on its own; used by
    /// standalone/test masters that have no raft role transitions.
    pub fn private() -> Self {
        Self::new(Arc::new(AtomicU64::new(0)))
    }

    /// Current leadership epoch. Every actual role transition (including
    /// leader -> follower -> leader cycles that end in the same state)
    /// advances it by one.
    pub fn value(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// Advance this epoch counter by one. Intended for standalone/test
    /// masters whose epochs are not raft-driven; raft-owned handles
    /// advance via `RoleMonitor::advance_role` and must not be bumped
    /// manually. Cache services only ever read the epoch, so a manual
    /// advance can only cause extra segment burn, never aliasing.
    pub fn advance(&self) {
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }
}

// raft node status.
#[repr(i8)]
#[derive(PartialEq, PartialOrd, Debug, Clone, Copy, IntoPrimitive, FromPrimitive)]
pub enum RoleState {
    // The raft node has not joined the cluster yet.
    #[num_enum(default)]
    Init = 0,
    Leader = 1,
    Follower = 2,
    Exit = 3,
}

// Asynchronous Task Status Monitor.
pub struct RoleMonitor(StateMonitor, Arc<AtomicU64>);

impl RoleMonitor {
    pub fn new() -> Self {
        Self(
            StateMonitor::new(RoleState::Init.into()),
            Arc::new(AtomicU64::new(0)),
        )
    }

    // Node role conversion.
    pub fn advance_role(&self, ss: &SoftState) {
        if ss.raft_state == StateRole::Leader || ss.raft_state == StateRole::Follower {
            let now = if ss.raft_state == StateRole::Leader {
                RoleState::Leader
            } else {
                RoleState::Follower
            };

            let cur: RoleState = self.0.state();
            if cur == RoleState::Init {
                self.0.advance_state(now, true);
            } else {
                self.0.advance_state(now, false);
            }
            // Every actual transition advances the leadership epoch so
            // leader-scoped volatile state (e.g. cache id segments) can
            // detect lost-and-regained leadership without observing the
            // intermediate states, including cycles that end in the same
            // role (Leader -> Follower -> Leader).
            if cur != now {
                self.1.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    /// Read-only handle onto the leadership epoch counter. The epoch
    /// advances on every role transition, so a stored epoch equal to the
    /// current one proves no leadership change happened in between.
    pub fn epoch_ctl(&self) -> EpochCtl {
        EpochCtl::new(self.1.clone())
    }

    pub fn is_leader(&self) -> bool {
        self.state() == RoleState::Leader
    }

    pub fn advance_exit(&self) {
        self.0.advance_state(RoleState::Exit, true);
        self.1.fetch_add(1, Ordering::SeqCst);
    }

    pub fn new_listener(&self) -> RoleStateListener {
        RoleStateListener(self.0.new_listener())
    }

    pub fn read_ctl(&self) -> StateCtl {
        self.0.read_ctl()
    }

    pub fn state(&self) -> RoleState {
        self.0.state()
    }

    pub fn is_running(&self) -> bool {
        self.state() != RoleState::Exit
    }
}

pub struct RoleStateListener(StateListener);

impl RoleStateListener {
    pub async fn wait_leader(&mut self) -> CommonResult<()> {
        self.0.wait_state(RoleState::Leader).await
    }

    pub async fn wait_follower(&mut self) -> CommonResult<()> {
        self.0.wait_state(RoleState::Follower).await
    }

    // Wait for the node to become a leader or follower.
    pub async fn wait_role(&mut self) -> CommonResult<()> {
        loop {
            let cur = RoleState::from(self.0.next_state().await?);
            if cur == RoleState::Leader || cur == RoleState::Follower {
                return Ok(());
            } else {
                continue;
            }
        }
    }
}

impl Default for RoleMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn soft_state(role: StateRole) -> SoftState {
        SoftState {
            leader_id: 1,
            raft_state: role,
        }
    }

    #[test]
    fn epoch_advances_on_every_role_transition() {
        let monitor = RoleMonitor::new();
        let epoch = monitor.epoch_ctl();
        assert_eq!(epoch.value(), 0);

        // Init -> Leader: one transition.
        monitor.advance_role(&soft_state(StateRole::Leader));
        assert_eq!(epoch.value(), 1);

        // Same role reported again: not a transition.
        monitor.advance_role(&soft_state(StateRole::Leader));
        assert_eq!(epoch.value(), 1);

        // Leader -> Follower -> Leader without any observer in between:
        // the epoch must still advance (twice), even though the final
        // role equals the stored one.
        monitor.advance_role(&soft_state(StateRole::Follower));
        monitor.advance_role(&soft_state(StateRole::Leader));
        assert_eq!(epoch.value(), 3);

        // Exit also invalidates leader-scoped state.
        monitor.advance_exit();
        assert_eq!(epoch.value(), 4);
    }

    #[test]
    fn epoch_handles_are_shared() {
        let monitor = RoleMonitor::new();
        let epoch = monitor.epoch_ctl();
        let epoch2 = monitor.epoch_ctl();
        monitor.advance_role(&soft_state(StateRole::Leader));
        assert_eq!(epoch.value(), 1);
        assert_eq!(epoch2.value(), 1);
    }
}
