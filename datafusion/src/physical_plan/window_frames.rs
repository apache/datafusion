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

//! Window frame
//!
//! The frame-spec determines which output rows are read by an aggregate window function. The frame-spec consists of four parts:
//! - A frame type - either ROWS, RANGE or GROUPS,
//! - A starting frame boundary,
//! - An ending frame boundary,
//! - An EXCLUDE clause.

use crate::error::{DataFusionError, Result};
use sqlparser::ast::{WindowFrame, WindowFrameBound, WindowFrameUnits};

const DEFAULT_WINDOW_FRAME: WindowFrame = WindowFrame {
    units: WindowFrameUnits::Range,
    start_bound: WindowFrameBound::Preceding(None),
    end_bound: Some(WindowFrameBound::CurrentRow),
};

fn get_bound_rank(bound: &WindowFrameBound) -> (u8, u64) {
    match bound {
        WindowFrameBound::Preceding(None) => (0, 0),
        WindowFrameBound::Following(None) => (4, 0),
        WindowFrameBound::Preceding(Some(0))
        | WindowFrameBound::CurrentRow
        | WindowFrameBound::Following(Some(0)) => (2, 0),
        WindowFrameBound::Preceding(Some(v)) => (1, u64::MAX - *v),
        WindowFrameBound::Following(Some(v)) => (3, *v),
    }
}

/// Validate a window frame if present, otherwise return the default window frame.
pub(crate) fn validate_window_frame(
    window_frame: &Option<WindowFrame>,
) -> Result<&WindowFrame> {
    let window_frame: &WindowFrame =
        window_frame.as_ref().unwrap_or(&DEFAULT_WINDOW_FRAME);
    let start_bound = &window_frame.start_bound;
    let end_bound = window_frame
        .end_bound
        .as_ref()
        .unwrap_or(&WindowFrameBound::CurrentRow);

    if let WindowFrameBound::Following(None) = *start_bound {
        Err(DataFusionError::Execution(
            "Invalid window frame: start bound cannot be unbounded following".to_owned(),
        ))
    } else if let WindowFrameBound::Preceding(None) = *end_bound {
        Err(DataFusionError::Execution(
            "Invalid window frame: end bound cannot be unbounded preceding".to_owned(),
        ))
    } else if get_bound_rank(start_bound) > get_bound_rank(end_bound) {
        Err(DataFusionError::Execution(format!(
            "Invalid window frame: start bound ({}) cannot be larger than end bound ({})",
            start_bound, end_bound
        )))
    } else {
        Ok(window_frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_window_frame() -> Result<()> {
        let default_value = validate_window_frame(&None)?;
        assert_eq!(default_value, &DEFAULT_WINDOW_FRAME);

        let window_frame = Some(WindowFrame {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::Following(None),
            end_bound: None,
        });
        let result = validate_window_frame(&window_frame);
        assert_eq!(
            result.err().unwrap().to_string(),
            "Execution error: Invalid window frame: start bound cannot be unbounded following".to_owned()
        );

        let window_frame = Some(WindowFrame {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::Preceding(None),
            end_bound: Some(WindowFrameBound::Preceding(None)),
        });
        let result = validate_window_frame(&window_frame);
        assert_eq!(
            result.err().unwrap().to_string(),
            "Execution error: Invalid window frame: end bound cannot be unbounded preceding".to_owned()
        );
        let window_frame = Some(WindowFrame {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::Preceding(Some(1)),
            end_bound: Some(WindowFrameBound::Preceding(Some(2))),
        });
        let result = validate_window_frame(&window_frame);
        assert_eq!(
            result.err().unwrap().to_string(),
            "Execution error: Invalid window frame: start bound (1 PRECEDING) cannot be larger than end bound (2 PRECEDING)".to_owned()
        );
        Ok(())
    }

    #[test]
    fn test_get_bound_rank_eq() {
        assert_eq!(
            get_bound_rank(&WindowFrameBound::CurrentRow),
            get_bound_rank(&WindowFrameBound::CurrentRow)
        );
        assert_eq!(
            get_bound_rank(&WindowFrameBound::Preceding(Some(0))),
            get_bound_rank(&WindowFrameBound::CurrentRow)
        );
        assert_eq!(
            get_bound_rank(&WindowFrameBound::CurrentRow),
            get_bound_rank(&WindowFrameBound::Following(Some(0)))
        );
        assert_eq!(
            get_bound_rank(&WindowFrameBound::Following(Some(2))),
            get_bound_rank(&WindowFrameBound::Following(Some(2)))
        );
        assert_eq!(
            get_bound_rank(&WindowFrameBound::Following(None)),
            get_bound_rank(&WindowFrameBound::Following(None))
        );
        assert_eq!(
            get_bound_rank(&WindowFrameBound::Preceding(Some(2))),
            get_bound_rank(&WindowFrameBound::Preceding(Some(2)))
        );
        assert_eq!(
            get_bound_rank(&WindowFrameBound::Preceding(None)),
            get_bound_rank(&WindowFrameBound::Preceding(None))
        );
    }

    #[test]
    fn test_get_bound_rank_cmp() {
        assert!(
            get_bound_rank(&WindowFrameBound::Preceding(Some(1)))
                < get_bound_rank(&WindowFrameBound::CurrentRow)
        );
        // ! yes this is correct!
        assert!(
            get_bound_rank(&WindowFrameBound::Preceding(Some(2)))
                < get_bound_rank(&WindowFrameBound::Preceding(Some(1)))
        );
        assert!(
            get_bound_rank(&WindowFrameBound::Preceding(Some(u64::MAX)))
                < get_bound_rank(&WindowFrameBound::Preceding(Some(u64::MAX - 1)))
        );
        assert!(
            get_bound_rank(&WindowFrameBound::Preceding(None))
                < get_bound_rank(&WindowFrameBound::Preceding(Some(1000000)))
        );
        assert!(
            get_bound_rank(&WindowFrameBound::Preceding(None))
                < get_bound_rank(&WindowFrameBound::Preceding(Some(u64::MAX)))
        );
        assert!(
            get_bound_rank(&WindowFrameBound::Preceding(None))
                < get_bound_rank(&WindowFrameBound::Following(Some(0)))
        );
        assert!(
            get_bound_rank(&WindowFrameBound::Preceding(Some(1)))
                < get_bound_rank(&WindowFrameBound::Following(Some(1)))
        );
        assert!(
            get_bound_rank(&WindowFrameBound::CurrentRow)
                < get_bound_rank(&WindowFrameBound::Following(Some(1)))
        );
        assert!(
            get_bound_rank(&WindowFrameBound::Following(Some(1)))
                < get_bound_rank(&WindowFrameBound::Following(Some(2)))
        );
        assert!(
            get_bound_rank(&WindowFrameBound::Following(Some(2)))
                < get_bound_rank(&WindowFrameBound::Following(None))
        );
        assert!(
            get_bound_rank(&WindowFrameBound::Following(Some(u64::MAX)))
                < get_bound_rank(&WindowFrameBound::Following(None))
        );
    }
}
