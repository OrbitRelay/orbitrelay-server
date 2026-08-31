//! Single-byte-range parsing for HTTP Asset delivery.

use orbitrelay_asset_runtime::AssetByteRange;
use thiserror::Error;

/// A resolved inclusive HTTP range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedRange {
    start: u64,
    end: u64,
}

impl ResolvedRange {
    /// Creates a resolved range after validating inclusive bounds.
    pub fn new(start: u64, end: u64) -> Result<Self, RangeParseError> {
        if start > end {
            return Err(RangeParseError::Unsatisfiable);
        }
        Ok(Self { start, end })
    }
    /// Returns the inclusive first byte.
    #[must_use]
    pub const fn start(self) -> u64 {
        self.start
    }
    /// Returns the inclusive last byte.
    #[must_use]
    pub const fn end(self) -> u64 {
        self.end
    }
    /// Returns the number of bytes.
    #[must_use]
    pub const fn length(self) -> u64 {
        self.end - self.start + 1
    }
    /// Converts to the Asset runtime's half-open range.
    pub fn asset_range(self) -> Result<AssetByteRange, RangeParseError> {
        AssetByteRange::new(self.start, self.length()).map_err(|_| RangeParseError::Unsatisfiable)
    }
}

/// Range parser failures mapped by HTTP to 400 or 416.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RangeParseError {
    /// The header syntax is malformed.
    #[error("malformed byte range")]
    Malformed,
    /// The range is syntactically valid but cannot be served.
    #[error("byte range is unsatisfiable")]
    Unsatisfiable,
    /// Multiple ranges are intentionally not implemented.
    #[error("multiple byte ranges are unsupported")]
    Multiple,
}

/// Parses one HTTP Range header against an immutable Asset length.
pub fn parse_range(value: &str, total: u64) -> Result<Option<ResolvedRange>, RangeParseError> {
    let Some(spec) = value.strip_prefix("bytes=") else {
        return Err(RangeParseError::Malformed);
    };
    if spec.contains(',') {
        return Err(RangeParseError::Multiple);
    }
    let (first, second) = spec.split_once('-').ok_or(RangeParseError::Malformed)?;
    if first.is_empty() {
        let suffix = second
            .parse::<u64>()
            .map_err(|_| RangeParseError::Malformed)?;
        if suffix == 0 || total == 0 {
            return Err(RangeParseError::Unsatisfiable);
        }
        let start = total.saturating_sub(suffix);
        return ResolvedRange::new(start, total - 1).map(Some);
    }
    let start = first
        .parse::<u64>()
        .map_err(|_| RangeParseError::Malformed)?;
    if total == 0 || start >= total {
        return Err(RangeParseError::Unsatisfiable);
    }
    if second.is_empty() {
        return ResolvedRange::new(start, total - 1).map(Some);
    }
    let end = second
        .parse::<u64>()
        .map_err(|_| RangeParseError::Malformed)?
        .min(total - 1);
    ResolvedRange::new(start, end).map(Some)
}

#[cfg(test)]
mod tests {
    use super::{parse_range, RangeParseError};

    #[test]
    fn parses_prefix_open_and_suffix_ranges() {
        assert_eq!(parse_range("bytes=0-3", 10).unwrap().unwrap().length(), 4);
        assert_eq!(parse_range("bytes=4-", 10).unwrap().unwrap().start(), 4);
        assert_eq!(parse_range("bytes=-4", 10).unwrap().unwrap().start(), 6);
        assert_eq!(parse_range("bytes=-99", 10).unwrap().unwrap().start(), 0);
    }

    #[test]
    fn distinguishes_malformed_unsatisfiable_and_multiple() {
        assert_eq!(
            parse_range("bytes=a-b", 10),
            Err(RangeParseError::Malformed)
        );
        assert_eq!(
            parse_range("bytes=10-", 10),
            Err(RangeParseError::Unsatisfiable)
        );
        assert_eq!(
            parse_range("bytes=0-1,2-3", 10),
            Err(RangeParseError::Multiple)
        );
    }
}
