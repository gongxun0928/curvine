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

use crate::master::meta::inode::InodeView::{self, Dir, FileEntry};
use crate::master::meta::inode::{InodeDir, InodeFile, InodePtr, PATH_SEPARATOR};
use crate::master::meta::store::InodeStore;
use curvine_core_error::{err_box, try_option, CommonResult};
use curvine_model::ListOptions;
use glob::Pattern;
use std::fmt;

/// One tree entry copied while the `FsDir` read lock is held.
#[derive(Debug)]
pub(crate) struct GlobTreeEntry {
    pub(crate) path: String,
    pub(crate) name: String,
    pub(crate) id: i64,
    pub(crate) is_dir: bool,
}

#[derive(Debug)]
pub(crate) struct GlobTreePage {
    pub(crate) entries: Vec<GlobTreeEntry>,
    /// Number of child names examined, including names that did not match.
    pub(crate) scanned: usize,
    /// Last scanned child name when another page may exist.
    pub(crate) next_after: Option<String>,
}

pub struct InodePath {
    path: String,
    name: String,
    pub components: Vec<String>,
    pub inodes: Vec<InodePtr>,
}

impl InodePath {
    pub fn resolve<T: AsRef<str>>(
        root: InodePtr,
        path: T,
        store: &InodeStore,
    ) -> CommonResult<Self> {
        let components = InodeView::path_components(path.as_ref())?;
        let name = try_option!(
            components.last(),
            "Path {} has no components",
            path.as_ref()
        );

        if name.is_empty() {
            return err_box!("Path {} is invalid", path.as_ref());
        }

        let mut inodes: Vec<InodePtr> = Vec::with_capacity(components.len());
        let mut cur_inode = root;
        let mut index = 0;

        while index < components.len() {
            //make sure resolved_inode is not a FileEntry
            //if it is a FileEntry, load the complete file data from store
            let resolved_inode = match cur_inode.as_ref() {
                FileEntry(f) => {
                    // If it is a FileEntry, load the complete object from store
                    match store.get_inode(f.id(), Some(f.name()))? {
                        Some(full_inode) => InodePtr::from_owned(full_inode),
                        None => return err_box!("Failed to load inode {} from store", f.id()),
                    }
                }
                _ => cur_inode.clone(),
            };

            inodes.push(resolved_inode);

            if index == components.len() - 1 {
                break;
            }

            index += 1;
            let child_name: &str = components[index].as_str();
            match cur_inode.as_mut() {
                Dir(d) => {
                    if let Some(child) = d.get_child_ptr(child_name) {
                        cur_inode = child;
                    } else {
                        // The directory has not been created, so there is no need to search again.
                        break;
                    }
                }

                _ => break,
            }
        }

        let inode_path = Self {
            path: path.as_ref().to_string(),
            name: name.to_string(),
            components,
            inodes,
        };

        Ok(inode_path)
    }

    /// True when every component exists in the in-memory tree.
    ///
    /// The last hop only checks that the child pointer is present. A `FileEntry`
    /// is enough, even when the store has no inode body for that id: the tree
    /// is the authority for existence. This does not call `store.get_inode`.
    pub fn exists_in_tree(root: InodePtr, path: &str) -> CommonResult<bool> {
        let components = InodeView::path_components(path)?;
        let name = try_option!(components.last(), "Path {} has no components", path);
        if name.is_empty() {
            return err_box!("Path {} is invalid", path);
        }
        if components.len() == 1 {
            return Ok(true);
        }

        let mut cur = root;
        for (index, component) in components.iter().enumerate().skip(1) {
            let child = match cur.as_ref() {
                Dir(dir) => dir.get_child(component).map(InodePtr::from_ref),
                _ => return Ok(false),
            };
            match child {
                Some(next) => {
                    if index + 1 == components.len() {
                        return Ok(true);
                    }
                    cur = next;
                }
                None => return Ok(false),
            }
        }
        Ok(false)
    }

    fn join_child_path(parent_path: &str, child_name: &str) -> String {
        if parent_path.is_empty() || parent_path == PATH_SEPARATOR {
            format!("{PATH_SEPARATOR}{child_name}")
        } else {
            format!("{parent_path}{PATH_SEPARATOR}{child_name}")
        }
    }

    fn resolve_tree_node(root: InodePtr, path: &str) -> CommonResult<Option<InodePtr>> {
        let components = InodeView::path_components(path)?;
        let name = try_option!(components.last(), "Path {} has no components", path);
        if name.is_empty() {
            return err_box!("Path {} is invalid", path);
        }
        if components.len() == 1 {
            return Ok(Some(root));
        }

        let mut cur = root;
        for component in components.iter().skip(1) {
            let next = match cur.as_ref() {
                Dir(dir) => dir.get_child(component).map(InodePtr::from_ref),
                _ => return Ok(None),
            };
            match next {
                Some(next) => cur = next,
                None => return Ok(None),
            }
        }
        Ok(Some(cur))
    }

    fn glob_tree_entry(parent_path: &str, inode: &InodeView) -> GlobTreeEntry {
        GlobTreeEntry {
            path: Self::join_child_path(parent_path, inode.name()),
            name: inode.name().to_string(),
            id: inode.id(),
            is_dir: inode.is_dir(),
        }
    }

    /// Owned literal child. Safe to keep after the `FsDir` read guard drops.
    pub(crate) fn glob_literal_child(
        root: InodePtr,
        parent_path: &str,
        child_name: &str,
    ) -> CommonResult<Option<GlobTreeEntry>> {
        let Some(parent) = Self::resolve_tree_node(root, parent_path)? else {
            return Ok(None);
        };
        let child = match parent.as_ref() {
            Dir(dir) => dir.get_child(child_name),
            _ => None,
        };
        Ok(child.map(|inode| Self::glob_tree_entry(parent_path, inode)))
    }

    /// At most `limit` child names. `next_after` is the last scanned name, not the last match.
    pub(crate) fn glob_children_page(
        root: InodePtr,
        parent_path: &str,
        pattern: &Pattern,
        start_after: Option<String>,
        limit: usize,
    ) -> CommonResult<GlobTreePage> {
        if limit == 0 {
            return err_box!("glob page limit must be greater than zero");
        }
        let Some(parent) = Self::resolve_tree_node(root, parent_path)? else {
            return Ok(GlobTreePage {
                entries: Vec::new(),
                scanned: 0,
                next_after: None,
            });
        };
        let dir = match parent.as_ref() {
            Dir(dir) => dir,
            _ => {
                return Ok(GlobTreePage {
                    entries: Vec::new(),
                    scanned: 0,
                    next_after: None,
                })
            }
        };
        let scanned = dir.list_options(&ListOptions {
            limit: Some(limit),
            start_after,
        });
        let scanned_len = scanned.len();
        let next_after = (scanned_len == limit)
            .then(|| scanned.last().map(|inode| inode.name().to_string()))
            .flatten();
        let entries = scanned
            .into_iter()
            .filter(|inode| pattern.matches(inode.name()))
            .map(|inode| Self::glob_tree_entry(parent_path, inode))
            .collect();
        Ok(GlobTreePage {
            entries,
            scanned: scanned_len,
            next_after,
        })
    }

    pub fn is_root(&self) -> bool {
        self.components.len() <= 1
    }

    // If all inodes on the path already exist, then return true.
    pub fn is_full(&self) -> bool {
        self.components.len() == self.inodes.len()
    }

    // Get the path name.
    pub fn name(&self) -> &str {
        &self.name
    }

    // Get the full full path.
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn child_path(&self, child: impl AsRef<str>) -> String {
        if self.is_root() {
            format!("/{}", child.as_ref())
        } else {
            format!("{}{}{}", self.path, PATH_SEPARATOR, child.as_ref())
        }
    }

    pub fn get_components(&self) -> &Vec<String> {
        &self.components
    }

    pub fn get_path(&self, index: usize) -> String {
        if index > self.components.len() {
            return "".to_string();
        }

        self.components[..index].join(PATH_SEPARATOR)
    }

    // Get the previous directory name.
    pub fn get_parent_path(&self) -> String {
        self.get_path(self.components.len() - 1)
    }

    // Get the parent path that already exists on the path, not target path.
    pub fn get_valid_parent_path(&self) -> String {
        self.get_path(self.existing_len())
    }

    pub fn get_component(&self, pos: usize) -> CommonResult<&'_ str> {
        match self.components.get(pos) {
            None => err_box!("Path does not exist"),
            Some(v) => Ok(v),
        }
    }

    pub fn get_inodes(&self) -> &Vec<InodePtr> {
        &self.inodes
    }

    // Get the last node that already exists on the path
    pub fn get_last_inode(&self) -> Option<InodePtr> {
        self.get_inode(-1)
    }

    // Convert the last node to InodeDir
    pub fn clone_last_dir(&self) -> CommonResult<InodeDir> {
        if let Some(v) = self.get_inode((self.inodes.len() - 1) as i32) {
            Ok(v.as_dir_ref()?.clone())
        } else {
            err_box!("status error: {}", self.path)
        }
    }

    // Convert the last node to InodeDir
    pub fn clone_last_file(&self) -> CommonResult<InodeFile> {
        if let Some(v) = self.get_last_inode() {
            Ok(v.as_file_ref()?.clone())
        } else {
            err_box!("status error")
        }
    }

    /// Get the inode that already exists in the path
    /// If it is a positive number, it indicates the start position; if it is a negative number, it indicates the start from the end.
    pub fn get_inode(&self, pos: i32) -> Option<InodePtr> {
        let pos = if pos < 0 {
            (self.components.len() as i32 + pos) as usize
        } else {
            pos as usize
        };

        if pos < self.inodes.len() {
            Some(self.inodes[pos].clone())
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.components.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn existing_len(&self) -> usize {
        self.inodes.len()
    }

    pub fn append(&mut self, inode: InodePtr) -> CommonResult<()> {
        if self.components.len() == self.inodes.len() {
            return err_box!(
                "Path {} is The path is complete, appending nodes is not allowed",
                self.path
            );
        }

        match self.get_component(self.inodes.len()) {
            Ok(n) if n == inode.name() => (),
            _ => return err_box!("data status  {:?}", self),
        }

        self.inodes.push(inode);
        Ok(())
    }

    // Determine whether it is an empty directory.
    pub fn is_empty_dir(&self) -> bool {
        match self.get_last_inode() {
            Some(v) => v.child_len() == 0,

            _ => true,
        }
    }

    // Return the last inode only if the path was fully resolved.
    pub fn task_last(mut self) -> Option<InodePtr> {
        if self.inodes.len() == self.components.len() {
            self.inodes.pop()
        } else {
            None
        }
    }
}

impl fmt::Debug for InodePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InodePath")
            .field("path", &self.path)
            .field("name", &self.name)
            .field("components", &self.components)
            .field("inodes", &self.inodes)
            .field("store", &"<InodeStore>")
            .finish()
    }
}
