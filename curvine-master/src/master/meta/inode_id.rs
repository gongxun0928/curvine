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

use crate::master::meta::block_id::BlockIdCodec;
use crate::master::meta::inode::ROOT_INODE_ID;
use curvine_core_error::{err_box, CommonResult};
use curvine_runtime::sync::AtomicLong;

pub struct InodeId(AtomicLong);

impl InodeId {
    pub const ID_BYTES: i64 = 40;
    pub const SEQ_BYTES: i64 = 24;
    pub const ID_MASK: i64 = (1i64 << Self::ID_BYTES) - 1;
    pub const SEQ_MASK: i64 = (1i64 << Self::SEQ_BYTES) - 1;
}

impl Default for InodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl InodeId {
    pub fn new() -> Self {
        Self(AtomicLong::new(ROOT_INODE_ID))
    }

    pub fn next(&self) -> CommonResult<i64> {
        let id = self.0.next();
        if id > BlockIdCodec::FS_INODE_MAX {
            err_box!(
                "inode id exceeds maximum value {}",
                BlockIdCodec::FS_INODE_MAX
            )
        } else {
            Ok(id)
        }
    }

    pub fn create_block_id(id: i64, seq: i64) -> CommonResult<i64> {
        BlockIdCodec::encode_block_id(id, seq)
    }

    pub fn get_id(id: i64) -> i64 {
        BlockIdCodec::get_owner(id)
    }

    pub fn get_seq(id: i64) -> i64 {
        BlockIdCodec::get_seq(id)
    }

    pub fn is_root(id: i64) -> bool {
        Self::get_id(id) == ROOT_INODE_ID
    }

    pub fn current(&self) -> i64 {
        self.0.get()
    }

    pub fn reset(&self, new_value: i64) -> CommonResult<()> {
        if new_value < ROOT_INODE_ID {
            return err_box!("inode id watermark below root: {}", new_value);
        }
        if new_value > BlockIdCodec::FS_INODE_MAX {
            return err_box!(
                "inode id watermark above fs domain end {}: {}",
                BlockIdCodec::FS_INODE_MAX,
                new_value
            );
        }
        loop {
            let c = self.current();
            if new_value < c {
                return err_box!(
                    "Cannot reset to less than the current value: {}, \
                where newValue {}",
                    c,
                    new_value
                );
            }
            if self.0.compare_and_set(c, new_value) {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::master::meta::InodeId;
    use curvine_core_error::CommonResult;
    use curvine_runtime::common::Utils;

    #[test]
    fn test_id_create() -> CommonResult<()> {
        let inode_id = InodeId::new();

        let file_id = inode_id.next()?;
        let seq_id = Utils::rand_id() as i64 & InodeId::SEQ_MASK;

        let block_id = InodeId::create_block_id(file_id, seq_id)?;

        let file_id_parse = InodeId::get_id(block_id);
        let seq_id_parse = InodeId::get_seq(block_id);

        assert_eq!(file_id, file_id_parse);
        assert_eq!(seq_id, seq_id_parse);

        Ok(())
    }
}
