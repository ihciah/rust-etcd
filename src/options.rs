use std::borrow::Cow;
use url::form_urlencoded::Serializer;

use bytes::Bytes;

use crate::Error;

/// Possible conditions for "compare and delete" and "compare and swap" operations.
#[derive(Debug)]
pub struct ComparisonConditions<'a> {
    /// The etcd modified index the key must have before the operation is performed.
    pub modified_index: Option<u64>,
    /// The value the key must have before the operation is performed.
    pub value: Option<&'a str>,
}

impl<'a> ComparisonConditions<'a> {
    /// Returns a boolean indicating whether or not both conditions are unset.
    pub fn is_empty(&self) -> bool {
        self.modified_index.is_none() && self.value.is_none()
    }
}

/// Controls the various different ways a delete operation can be performed.
#[derive(Debug, Default)]
pub struct DeleteOptions<'a> {
    /// Conditions used for "compare and delete" operations.
    pub conditions: Option<ComparisonConditions<'a>>,
    /// Whether or not the key to be deleted is a directory.
    pub dir: Option<bool>,
    /// Whether or not keys within a directory should be deleted recursively.
    pub recursive: Option<bool>,
}

/// Controls the various different ways a get operation can be performed.
#[derive(Debug, Default)]
pub struct GetOptions {
    /// Whether or not to use read linearization to avoid stale data.
    pub strong_consistency: bool,
    /// Whether or not keys within a directory should be included in the response.
    pub recursive: bool,
    /// Whether or not directory contents will be sorted within the response.
    pub sort: Option<bool>,
    /// Whether or not to wait for a change.
    pub wait: bool,
    /// The etcd index to use as a lower bound when watching a key.
    pub wait_index: Option<u64>,
}

/// Controls the various different ways a create, update, or set operation can be performed.
#[derive(Debug, Default)]
pub struct SetOptions<'a> {
    /// Conditions used for "compare and swap" operations.
    pub conditions: Option<ComparisonConditions<'a>>,
    /// Whether or not to use the "create in order" API.
    pub create_in_order: bool,
    /// Whether or not the key being operated on is or should be a directory.
    pub dir: Option<bool>,
    /// Whether or not the key being operated on must already exist.
    pub prev_exist: Option<bool>,
    /// Time to live in seconds.
    pub ttl: Option<u64>,
    /// New value for the key.
    pub value: Option<&'a str>,
    /// Whether we should refresh the key, instead of setting it.alloc
    pub refresh: bool,
}

impl<'a> SetOptions<'a> {
    /// Converts this `SetOptions` into option pairs that can be used to create a request body from these bytes.
    ///
    /// JAKE-TODO: Can we just make this return a `Bytes` for the body!
    pub(crate) fn into_request_body(self) -> Result<Bytes, Error> {
        let mut option_pairs = vec![];

        if let Some(value) = self.value {
            option_pairs.push(("value", Cow::Borrowed(value)));
        }

        if let Some(ref ttl) = self.ttl {
            option_pairs.push(("ttl", Cow::Owned(ttl.to_string())));
        }

        if let Some(dir) = self.dir {
            option_pairs.push(("dir", Cow::Borrowed(bool_to_str(dir))));
        }

        let prev_exist = match self.prev_exist {
            Some(prev_exist) => prev_exist,
            None => false,
        };

        // If we are calling refresh, we should also ensure we are setting prevExist.
        if prev_exist || self.refresh {
            let prev_exist = prev_exist || self.refresh;
            option_pairs.push(("prevExist", Cow::Borrowed(bool_to_str(prev_exist))));
        }

        if self.refresh {
            option_pairs.push(("refresh", Cow::Borrowed(bool_to_str(true))));
        }

        if let Some(conditions) = self.conditions {
            if conditions.is_empty() {
                return Err(Error::InvalidConditions);
            }

            if let Some(modified_index) = conditions.modified_index {
                option_pairs.push(("prevIndex", Cow::Owned(modified_index.to_string())));
            }

            if let Some(value) = conditions.value {
                option_pairs.push(("prevValue", Cow::Borrowed(value)));
            }
        }

        let mut serializer = Serializer::new(String::new());
        serializer.extend_pairs(option_pairs);
        Ok(serializer.finish().into())
    }
}

#[inline(always)]
const fn bool_to_str(b: bool) -> &'static str {
    if b {
        "true"
    } else {
        "false"
    }
}
