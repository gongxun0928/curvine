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

use curvine_raft::raft::{EpochCtl, RoleState};
use curvine_runtime::sync::StateCtl;
use num_enum::{FromPrimitive, IntoPrimitive};

// master state controller
#[repr(i8)]
#[derive(PartialEq, PartialOrd, Debug, Clone, Copy, IntoPrimitive, FromPrimitive)]
pub enum MasterState {
    // Active master node, only this state can provide a metadata lake.
    Active = 1,
    // Slave node, only copy logs.
    #[num_enum(default)]
    Standby = 2,

    // The node is in safe mode.
    SafeMode = 3,

    // The node has exited.
    Exit = 4,
}

#[derive(Clone)]
pub struct MasterMonitor {
    pub(crate) journal_ctl: StateCtl,
    pub fs_ctl: StateCtl,
    /// Leadership epoch: advances on every raft role transition. Master
    /// services with leader-scoped volatile state (e.g. the cache object
    /// id segment) compare a stored epoch against this to detect
    /// lost-and-regained leadership without observing the intermediate
    /// states. Standalone/test masters share a private epoch counter so
    /// the handle is always live.
    pub(crate) journal_epoch: EpochCtl,
}

impl MasterMonitor {
    pub fn new(journal_ctl: StateCtl, fs_ctl: StateCtl) -> Self {
        Self::with_epoch(journal_ctl, fs_ctl, EpochCtl::private())
    }

    pub fn with_epoch(journal_ctl: StateCtl, fs_ctl: StateCtl, journal_epoch: EpochCtl) -> Self {
        Self {
            journal_ctl,
            fs_ctl,
            journal_epoch,
        }
    }

    /// Current leadership epoch (see `journal_epoch`).
    pub fn journal_epoch(&self) -> u64 {
        self.journal_epoch.value()
    }

    // Determine whether the current node is an active node.
    // The journal is at the active node of the leader, which is the master.
    pub fn is_active(&self) -> bool {
        let cur: RoleState = self.journal_ctl.state();
        cur == RoleState::Leader
    }

    pub fn journal_state(&self) -> MasterState {
        let s: RoleState = self.journal_ctl.state();
        match s {
            RoleState::Leader => MasterState::Active,
            RoleState::Follower => MasterState::Standby,
            _ => MasterState::Exit,
        }
    }

    pub fn is_stop(&self) -> bool {
        let cur: RoleState = self.journal_ctl.state();
        cur == RoleState::Exit
    }
}
