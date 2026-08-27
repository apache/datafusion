// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! FFI-safe representations of the plain-data types in the
//! [`object_store::ObjectStore`] API.
//!
//! # Extensions are not carried across the boundary
//!
//! [`GetOptions`], [`PutOptions`], [`PutMultipartOptions`], [`CopyOptions`],
//! and [`RenameOptions`] each carry an `extensions: ::http::Extensions` field.
//! `Extensions` is a heterogeneous `TypeId`-keyed map of arbitrary values;
//! `TypeId` is not stable across separately compiled libraries, so the contents
//! cannot be interpreted on the far side even in principle. Extensions are
//! therefore dropped in both directions. The `object_store` crate documents
//! that its own backends ignore extensions entirely, so this only affects
//! third-party stores that use them for out-of-band context such as tracing
//! spans.

use chrono::{DateTime, Utc};
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, CopyMode, CopyOptions, GetOptions, GetRange,
    ListResult, ObjectMeta, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, RenameOptions, RenameTargetMode, TagSet, UpdateVersion,
};
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;

use crate::util::FFI_Option;

/// An FFI-safe [`DateTime<Utc>`].
///
/// Sent as a split timestamp rather than a nanosecond count so that timestamps
/// outside the roughly 1677-2262 range representable in `i64` nanoseconds
/// survive the round trip.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FFI_Timestamp {
    pub secs: i64,
    pub nanos: u32,
}

impl From<DateTime<Utc>> for FFI_Timestamp {
    fn from(value: DateTime<Utc>) -> Self {
        Self {
            secs: value.timestamp(),
            nanos: value.timestamp_subsec_nanos(),
        }
    }
}

impl From<FFI_Timestamp> for DateTime<Utc> {
    fn from(value: FFI_Timestamp) -> Self {
        DateTime::from_timestamp(value.secs, value.nanos).unwrap_or_default()
    }
}

fn opt_string(value: &Option<String>) -> FFI_Option<SString> {
    match value {
        Some(v) => FFI_Option::Some(v.as_str().into()),
        None => FFI_Option::None,
    }
}

fn from_opt_string(value: FFI_Option<SString>) -> Option<String> {
    value.into_option().map(|v| v.to_string())
}

/// An FFI-safe [`ObjectMeta`].
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_ObjectMeta {
    pub location: SString,
    pub last_modified: FFI_Timestamp,
    pub size: u64,
    pub e_tag: FFI_Option<SString>,
    pub version: FFI_Option<SString>,
}

impl From<&ObjectMeta> for FFI_ObjectMeta {
    fn from(meta: &ObjectMeta) -> Self {
        Self {
            location: meta.location.as_ref().into(),
            last_modified: meta.last_modified.into(),
            size: meta.size,
            e_tag: opt_string(&meta.e_tag),
            version: opt_string(&meta.version),
        }
    }
}

impl From<ObjectMeta> for FFI_ObjectMeta {
    fn from(meta: ObjectMeta) -> Self {
        Self::from(&meta)
    }
}

impl From<FFI_ObjectMeta> for ObjectMeta {
    fn from(meta: FFI_ObjectMeta) -> Self {
        ObjectMeta {
            // `Path::from` normalizes, and `location` was produced by
            // `Path::as_ref` on an already-normalized path, so this round trips.
            location: Path::from(meta.location.to_string()),
            last_modified: meta.last_modified.into(),
            size: meta.size,
            e_tag: from_opt_string(meta.e_tag),
            version: from_opt_string(meta.version),
        }
    }
}

/// An FFI-safe [`GetRange`].
#[repr(C, u8)]
#[derive(Debug, Clone, Copy)]
pub enum FFI_GetRange {
    Bounded { start: u64, end: u64 },
    Offset(u64),
    Suffix(u64),
}

impl From<&GetRange> for FFI_GetRange {
    fn from(range: &GetRange) -> Self {
        match range {
            GetRange::Bounded(r) => FFI_GetRange::Bounded {
                start: r.start,
                end: r.end,
            },
            GetRange::Offset(o) => FFI_GetRange::Offset(*o),
            GetRange::Suffix(s) => FFI_GetRange::Suffix(*s),
        }
    }
}

impl From<FFI_GetRange> for GetRange {
    fn from(range: FFI_GetRange) -> Self {
        match range {
            FFI_GetRange::Bounded { start, end } => GetRange::Bounded(start..end),
            FFI_GetRange::Offset(o) => GetRange::Offset(o),
            FFI_GetRange::Suffix(s) => GetRange::Suffix(s),
        }
    }
}

/// An FFI-safe [`Attribute`].
///
/// [`Attribute::Metadata`] carries a user-defined key, which travels in the
/// accompanying string; the other variants ignore it.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_Attribute {
    pub kind: u8,
    pub metadata_key: SString,
    pub value: SString,
}

const ATTR_CONTENT_DISPOSITION: u8 = 0;
const ATTR_CONTENT_ENCODING: u8 = 1;
const ATTR_CONTENT_LANGUAGE: u8 = 2;
const ATTR_CONTENT_TYPE: u8 = 3;
const ATTR_CACHE_CONTROL: u8 = 4;
const ATTR_STORAGE_CLASS: u8 = 5;
const ATTR_METADATA: u8 = 6;

pub(crate) fn attributes_to_ffi(attributes: &Attributes) -> SVec<FFI_Attribute> {
    attributes
        .iter()
        .map(|(key, value)| {
            let (kind, metadata_key) = match key {
                Attribute::ContentDisposition => (ATTR_CONTENT_DISPOSITION, ""),
                Attribute::ContentEncoding => (ATTR_CONTENT_ENCODING, ""),
                Attribute::ContentLanguage => (ATTR_CONTENT_LANGUAGE, ""),
                Attribute::ContentType => (ATTR_CONTENT_TYPE, ""),
                Attribute::CacheControl => (ATTR_CACHE_CONTROL, ""),
                Attribute::StorageClass => (ATTR_STORAGE_CLASS, ""),
                Attribute::Metadata(k) => (ATTR_METADATA, k.as_ref()),
                // `Attribute` is `#[non_exhaustive]`; an unknown variant is
                // dropped rather than mistranslated.
                _ => (u8::MAX, ""),
            };
            FFI_Attribute {
                kind,
                metadata_key: metadata_key.into(),
                value: value.as_ref().into(),
            }
        })
        .filter(|attr| attr.kind != u8::MAX)
        .collect()
}

pub(crate) fn attributes_from_ffi(attributes: SVec<FFI_Attribute>) -> Attributes {
    attributes
        .into_iter()
        .filter_map(|attr| {
            let key = match attr.kind {
                ATTR_CONTENT_DISPOSITION => Attribute::ContentDisposition,
                ATTR_CONTENT_ENCODING => Attribute::ContentEncoding,
                ATTR_CONTENT_LANGUAGE => Attribute::ContentLanguage,
                ATTR_CONTENT_TYPE => Attribute::ContentType,
                ATTR_CACHE_CONTROL => Attribute::CacheControl,
                ATTR_STORAGE_CLASS => Attribute::StorageClass,
                ATTR_METADATA => {
                    Attribute::Metadata(attr.metadata_key.to_string().into())
                }
                _ => return None,
            };
            Some((key, AttributeValue::from(attr.value.to_string())))
        })
        .collect()
}

/// Encode a [`TagSet`] as its URL-encoded wire form.
///
/// [`TagSet`] exposes [`TagSet::encoded`] but can only be built with
/// [`TagSet::push`], so the encoded form is parsed back into pairs and re-pushed
/// on the far side. `push` applies the same encoding, so this round trips.
pub(crate) fn tags_to_ffi(tags: &TagSet) -> SString {
    tags.encoded().into()
}

pub(crate) fn tags_from_ffi(encoded: &SString) -> TagSet {
    let encoded = encoded.to_string();
    let mut tags = TagSet::default();
    for (key, value) in url::form_urlencoded::parse(encoded.as_bytes()) {
        tags.push(key.as_ref(), value.as_ref());
    }
    tags
}

/// An FFI-safe [`GetOptions`].
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_GetOptions {
    pub if_match: FFI_Option<SString>,
    pub if_none_match: FFI_Option<SString>,
    pub if_modified_since: FFI_Option<FFI_Timestamp>,
    pub if_unmodified_since: FFI_Option<FFI_Timestamp>,
    pub range: FFI_Option<FFI_GetRange>,
    pub version: FFI_Option<SString>,
    pub head: bool,
}

impl From<&GetOptions> for FFI_GetOptions {
    fn from(options: &GetOptions) -> Self {
        Self {
            if_match: opt_string(&options.if_match),
            if_none_match: opt_string(&options.if_none_match),
            if_modified_since: options.if_modified_since.map(FFI_Timestamp::from).into(),
            if_unmodified_since: options
                .if_unmodified_since
                .map(FFI_Timestamp::from)
                .into(),
            range: options.range.as_ref().map(FFI_GetRange::from).into(),
            version: opt_string(&options.version),
            head: options.head,
        }
    }
}

impl From<FFI_GetOptions> for GetOptions {
    fn from(options: FFI_GetOptions) -> Self {
        GetOptions {
            if_match: from_opt_string(options.if_match),
            if_none_match: from_opt_string(options.if_none_match),
            if_modified_since: options.if_modified_since.into_option().map(Into::into),
            if_unmodified_since: options
                .if_unmodified_since
                .into_option()
                .map(Into::into),
            range: options.range.into_option().map(Into::into),
            version: from_opt_string(options.version),
            head: options.head,
            extensions: Default::default(),
        }
    }
}

/// An FFI-safe [`PutMode`].
#[repr(C, u8)]
#[derive(Debug, Clone)]
pub enum FFI_PutMode {
    Overwrite,
    Create,
    Update {
        e_tag: FFI_Option<SString>,
        version: FFI_Option<SString>,
    },
}

impl From<&PutMode> for FFI_PutMode {
    fn from(mode: &PutMode) -> Self {
        match mode {
            PutMode::Overwrite => FFI_PutMode::Overwrite,
            PutMode::Create => FFI_PutMode::Create,
            PutMode::Update(v) => FFI_PutMode::Update {
                e_tag: opt_string(&v.e_tag),
                version: opt_string(&v.version),
            },
        }
    }
}

impl From<FFI_PutMode> for PutMode {
    fn from(mode: FFI_PutMode) -> Self {
        match mode {
            FFI_PutMode::Overwrite => PutMode::Overwrite,
            FFI_PutMode::Create => PutMode::Create,
            FFI_PutMode::Update { e_tag, version } => PutMode::Update(UpdateVersion {
                e_tag: from_opt_string(e_tag),
                version: from_opt_string(version),
            }),
        }
    }
}

/// An FFI-safe [`PutOptions`].
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_PutOptions {
    pub mode: FFI_PutMode,
    pub tags: SString,
    pub attributes: SVec<FFI_Attribute>,
}

impl From<&PutOptions> for FFI_PutOptions {
    fn from(options: &PutOptions) -> Self {
        Self {
            mode: (&options.mode).into(),
            tags: tags_to_ffi(&options.tags),
            attributes: attributes_to_ffi(&options.attributes),
        }
    }
}

impl From<FFI_PutOptions> for PutOptions {
    fn from(options: FFI_PutOptions) -> Self {
        PutOptions {
            mode: options.mode.into(),
            tags: tags_from_ffi(&options.tags),
            attributes: attributes_from_ffi(options.attributes),
            extensions: Default::default(),
        }
    }
}

/// An FFI-safe [`PutMultipartOptions`].
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_PutMultipartOptions {
    pub tags: SString,
    pub attributes: SVec<FFI_Attribute>,
}

impl From<&PutMultipartOptions> for FFI_PutMultipartOptions {
    fn from(options: &PutMultipartOptions) -> Self {
        Self {
            tags: tags_to_ffi(&options.tags),
            attributes: attributes_to_ffi(&options.attributes),
        }
    }
}

impl From<FFI_PutMultipartOptions> for PutMultipartOptions {
    fn from(options: FFI_PutMultipartOptions) -> Self {
        PutMultipartOptions {
            tags: tags_from_ffi(&options.tags),
            attributes: attributes_from_ffi(options.attributes),
            extensions: Default::default(),
        }
    }
}

/// An FFI-safe [`PutResult`].
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_PutResult {
    pub e_tag: FFI_Option<SString>,
    pub version: FFI_Option<SString>,
}

impl From<&PutResult> for FFI_PutResult {
    fn from(result: &PutResult) -> Self {
        Self {
            e_tag: opt_string(&result.e_tag),
            version: opt_string(&result.version),
        }
    }
}

impl From<FFI_PutResult> for PutResult {
    fn from(result: FFI_PutResult) -> Self {
        PutResult {
            e_tag: from_opt_string(result.e_tag),
            version: from_opt_string(result.version),
        }
    }
}

/// An FFI-safe [`CopyOptions`].
///
/// [`CopyMode::Create`] is what makes `copy_if_not_exists` atomic, so the mode
/// must survive the boundary intact.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FFI_CopyOptions {
    /// `true` for [`CopyMode::Create`], `false` for [`CopyMode::Overwrite`].
    pub create: bool,
}

impl From<&CopyOptions> for FFI_CopyOptions {
    fn from(options: &CopyOptions) -> Self {
        Self {
            create: matches!(options.mode, CopyMode::Create),
        }
    }
}

impl From<FFI_CopyOptions> for CopyOptions {
    fn from(options: FFI_CopyOptions) -> Self {
        CopyOptions {
            mode: if options.create {
                CopyMode::Create
            } else {
                CopyMode::Overwrite
            },
            extensions: Default::default(),
        }
    }
}

/// An FFI-safe [`RenameOptions`].
///
/// [`RenameTargetMode::Create`] is what makes `rename_if_not_exists` atomic,
/// which delta-style commit protocols rely on for concurrency control.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FFI_RenameOptions {
    /// `true` for [`RenameTargetMode::Create`], `false` for
    /// [`RenameTargetMode::Overwrite`].
    pub create: bool,
}

impl From<&RenameOptions> for FFI_RenameOptions {
    fn from(options: &RenameOptions) -> Self {
        Self {
            create: matches!(options.target_mode, RenameTargetMode::Create),
        }
    }
}

impl From<FFI_RenameOptions> for RenameOptions {
    fn from(options: FFI_RenameOptions) -> Self {
        RenameOptions {
            target_mode: if options.create {
                RenameTargetMode::Create
            } else {
                RenameTargetMode::Overwrite
            },
            extensions: Default::default(),
        }
    }
}

/// An FFI-safe [`ListResult`].
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_ListResult {
    pub common_prefixes: SVec<SString>,
    pub objects: SVec<FFI_ObjectMeta>,
}

impl From<&ListResult> for FFI_ListResult {
    fn from(result: &ListResult) -> Self {
        Self {
            common_prefixes: result
                .common_prefixes
                .iter()
                .map(|p| p.as_ref().into())
                .collect(),
            objects: result.objects.iter().map(FFI_ObjectMeta::from).collect(),
        }
    }
}

impl From<FFI_ListResult> for ListResult {
    fn from(result: FFI_ListResult) -> Self {
        ListResult {
            common_prefixes: result
                .common_prefixes
                .into_iter()
                .map(|p| Path::from(p.to_string()))
                .collect(),
            objects: result.objects.into_iter().map(ObjectMeta::from).collect(),
        }
    }
}

/// Convert a [`PutPayload`] into its FFI-safe wire form.
///
/// The payload is a sequence of [`bytes::Bytes`] blocks; each block crosses as
/// its own buffer so the block structure is preserved and no data is copied.
pub(crate) fn put_payload_to_ffi(payload: &PutPayload) -> SVec<super::FFI_Bytes> {
    payload
        .iter()
        .map(|b| super::FFI_Bytes::from(b.clone()))
        .collect()
}

pub(crate) fn put_payload_from_ffi(payload: SVec<super::FFI_Bytes>) -> PutPayload {
    payload
        .into_iter()
        .map(bytes::Bytes::from)
        .collect::<PutPayload>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_meta_round_trip() {
        let original = ObjectMeta {
            location: Path::from("a/b/c.parquet"),
            last_modified: DateTime::from_timestamp(1_700_000_000, 123_456_789).unwrap(),
            size: 4096,
            e_tag: Some("\"abc123\"".to_string()),
            version: Some("v7".to_string()),
        };

        let restored = ObjectMeta::from(FFI_ObjectMeta::from(&original));
        assert_eq!(restored, original);
    }

    #[test]
    fn object_meta_round_trip_without_optionals() {
        let original = ObjectMeta {
            location: Path::from("root.json"),
            last_modified: DateTime::from_timestamp(0, 0).unwrap(),
            size: 0,
            e_tag: None,
            version: None,
        };

        let restored = ObjectMeta::from(FFI_ObjectMeta::from(&original));
        assert_eq!(restored, original);
    }

    #[test]
    fn get_range_round_trip() {
        for original in [
            GetRange::Bounded(10..200),
            GetRange::Offset(42),
            GetRange::Suffix(8),
        ] {
            let restored = GetRange::from(FFI_GetRange::from(&original));
            assert_eq!(format!("{restored:?}"), format!("{original:?}"));
        }
    }

    #[test]
    fn get_options_round_trip() {
        let original = GetOptions {
            if_match: Some("\"e1\"".to_string()),
            if_none_match: Some("\"e2\"".to_string()),
            if_modified_since: Some(DateTime::from_timestamp(1_600_000_000, 0).unwrap()),
            if_unmodified_since: Some(
                DateTime::from_timestamp(1_600_000_001, 500).unwrap(),
            ),
            range: Some(GetRange::Bounded(5..25)),
            version: Some("v1".to_string()),
            head: true,
            extensions: Default::default(),
        };

        let restored = GetOptions::from(FFI_GetOptions::from(&original));
        assert_eq!(restored.if_match, original.if_match);
        assert_eq!(restored.if_none_match, original.if_none_match);
        assert_eq!(restored.if_modified_since, original.if_modified_since);
        assert_eq!(restored.if_unmodified_since, original.if_unmodified_since);
        assert_eq!(
            format!("{:?}", restored.range),
            format!("{:?}", original.range)
        );
        assert_eq!(restored.version, original.version);
        assert_eq!(restored.head, original.head);
    }

    #[test]
    fn put_mode_round_trip() {
        let cases = [
            PutMode::Overwrite,
            PutMode::Create,
            PutMode::Update(UpdateVersion {
                e_tag: Some("\"tag\"".to_string()),
                version: None,
            }),
        ];
        for original in cases {
            let restored = PutMode::from(FFI_PutMode::from(&original));
            assert_eq!(restored, original);
        }
    }

    #[test]
    fn attributes_round_trip() {
        let original: Attributes = [
            (Attribute::ContentType, "application/parquet"),
            (Attribute::CacheControl, "no-cache"),
            (Attribute::StorageClass, "GLACIER"),
            (Attribute::Metadata("custom".into()), "value"),
        ]
        .into_iter()
        .collect();

        let restored = attributes_from_ffi(attributes_to_ffi(&original));
        assert_eq!(restored, original);
    }

    #[test]
    fn tags_round_trip() {
        let mut original = TagSet::default();
        original.push("test/foo", "value sdlks");
        original.push("foo", " sdf _ /+./sd");

        let restored = tags_from_ffi(&tags_to_ffi(&original));
        assert_eq!(restored.encoded(), original.encoded());
    }

    #[test]
    fn empty_tags_round_trip() {
        let restored = tags_from_ffi(&tags_to_ffi(&TagSet::default()));
        assert_eq!(restored.encoded(), "");
    }

    #[test]
    fn copy_and_rename_options_round_trip() {
        for create in [true, false] {
            let original = CopyOptions {
                mode: if create {
                    CopyMode::Create
                } else {
                    CopyMode::Overwrite
                },
                extensions: Default::default(),
            };
            let restored = CopyOptions::from(FFI_CopyOptions::from(&original));
            assert_eq!(restored.mode, original.mode);

            let original = RenameOptions {
                target_mode: if create {
                    RenameTargetMode::Create
                } else {
                    RenameTargetMode::Overwrite
                },
                extensions: Default::default(),
            };
            let restored = RenameOptions::from(FFI_RenameOptions::from(&original));
            assert_eq!(restored.target_mode, original.target_mode);
        }
    }

    #[test]
    fn list_result_round_trip() {
        let original = ListResult {
            common_prefixes: vec![Path::from("a"), Path::from("b/c")],
            objects: vec![ObjectMeta {
                location: Path::from("a/1.parquet"),
                last_modified: DateTime::from_timestamp(10, 0).unwrap(),
                size: 12,
                e_tag: None,
                version: None,
            }],
        };

        let restored = ListResult::from(FFI_ListResult::from(&original));
        assert_eq!(restored.common_prefixes, original.common_prefixes);
        assert_eq!(restored.objects, original.objects);
    }

    #[test]
    fn put_payload_round_trip_preserves_blocks() {
        let original: PutPayload = vec![
            bytes::Bytes::from_static(b"first"),
            bytes::Bytes::from_static(b"second"),
        ]
        .into_iter()
        .collect();

        let restored = put_payload_from_ffi(put_payload_to_ffi(&original));
        assert_eq!(restored.content_length(), original.content_length());
        let restored_blocks: Vec<_> = restored.iter().cloned().collect();
        let original_blocks: Vec<_> = original.iter().cloned().collect();
        assert_eq!(restored_blocks, original_blocks);
    }

    #[test]
    fn timestamp_round_trip_beyond_nanosecond_range() {
        // Year 2500, outside the i64-nanosecond representable range.
        let original = DateTime::from_timestamp(16_725_225_600, 0).unwrap();
        let restored: DateTime<Utc> = FFI_Timestamp::from(original).into();
        assert_eq!(restored, original);
    }
}
