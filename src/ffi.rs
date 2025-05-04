//! C-compatible FFI layer for the Databento PMZ calculation
//! This interface is designed to be called from C# via P/Invoke

/// FFI module for C/C# interoperability.
/// 
/// This module provides a C-compatible interface for the Databento client library,
/// allowing it to be used from C, C#, or other languages that support C FFI.
/// 
/// The main functionality exposed is the PMZ (Pre-Market Zone) calculation
/// via the `pmz_calculate` function.

use crate::examples::es_futures_pmz;
use chrono::NaiveDate;
use std::{
    ffi::{c_char, CStr, CString},
    ptr,
};
use tokio::runtime::Runtime;

/// Error codes for PMZ calculation functions.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub enum PmzErrorCode {
    /// No error occurred
    Success = 0,
    /// Invalid API key
    InvalidApiKey = 1,
    /// Invalid date format
    InvalidDate = 2,
    /// API request failed
    ApiRequestFailed = 3,
    /// Data processing failed
    DataProcessingFailed = 4,
    /// Insufficient data for calculation
    InsufficientData = 5,
    /// Other error
    Other = 99,
}

/// C-compatible PMZ result struct
#[repr(C)]
#[derive(Debug)]
pub struct CPmzResult {
    /// Error code (0 = success)
    pub error_code: PmzErrorCode,
    /// Error message if error_code != 0, otherwise null
    pub error_message: *mut c_char,
    /// Date for which PMZ values were calculated (format: YYYY-MM-DD)
    pub date: *mut c_char,
    /// Pre-Market High value
    pub pmh: f64,
    /// Pre-Market Low value
    pub pml: f64,
    /// Previous day's Line in Sand (LIS) value
    pub prev_day_lis: f64,
    /// Indicates if market gapped up (1) or down (0)
    pub is_gap_up: i32,
    /// PMZ high value (buy zone)
    pub pmz_high: f64,
    /// PMZ low value (sell zone) 
    pub pmz_low: f64,
    /// Risk value (PMZ High - PMZ Low)
    pub risk: f64,
}

/// Frees memory allocated by `pmz_calculate`.
/// 
/// # Safety
/// 
/// This function must be called with a pointer returned by `pmz_calculate`.
/// Calling it with any other pointer is undefined behavior.
#[no_mangle]
pub unsafe extern "C" fn pmz_free_result(result: *mut CPmzResult) {
    if !result.is_null() {
        let result_ref = &mut *result;
        
        // Free error_message if it's not null
        if !result_ref.error_message.is_null() {
            let _ = CString::from_raw(result_ref.error_message);
        }
        
        // Free date if it's not null
        if !result_ref.date.is_null() {
            let _ = CString::from_raw(result_ref.date);
        }
        
        // Free the result struct itself
        drop(Box::from_raw(result));
    }
}

/// Calculates PMZ (Pre-Market Zone) values for E-mini S&P 500 futures.
/// 
/// # Parameters
/// 
/// * `api_key` - Databento API key (null-terminated C string)
/// * `date` - Optional date in YYYY-MM-DD format (null-terminated C string), or NULL for today
/// 
/// # Returns
/// 
/// A pointer to a heap-allocated `CPmzResult` struct. The caller must free this memory
/// by calling `pmz_free_result` when done.
/// 
/// # Safety
/// 
/// This function is unsafe because it interacts with C strings and memory that
/// crosses the FFI boundary.
#[no_mangle]
pub unsafe extern "C" fn pmz_calculate(
    api_key: *const c_char,
    date: *const c_char,
) -> *mut CPmzResult {
    // Check if API key is null
    if api_key.is_null() {
        return create_error_result(
            PmzErrorCode::InvalidApiKey,
            "API key cannot be null",
        );
    }

    // Try to convert API key to Rust string
    let api_key_cstr = match CStr::from_ptr(api_key).to_str() {
        Ok(s) => s,
        Err(_) => {
            return create_error_result(
                PmzErrorCode::InvalidApiKey,
                "API key contains invalid UTF-8",
            );
        }
    };

    // Parse the date if provided
    let parse_date = if date.is_null() {
        None
    } else {
        match CStr::from_ptr(date).to_str() {
            Ok(date_str) => match NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                Ok(d) => Some(d),
                Err(_) => {
                    return create_error_result(
                        PmzErrorCode::InvalidDate,
                        "Invalid date format, expected YYYY-MM-DD",
                    );
                }
            },
            Err(_) => {
                return create_error_result(
                    PmzErrorCode::InvalidDate,
                    "Date contains invalid UTF-8",
                );
            }
        }
    };

    // Create a tokio runtime for async execution
    let runtime = match Runtime::new() {
        Ok(rt) => rt,
        Err(_) => {
            return create_error_result(
                PmzErrorCode::Other,
                "Failed to create async runtime",
            );
        }
    };

    // Run the PMZ calculation
    let result = runtime.block_on(async {
        es_futures_pmz::calculate_pmz(api_key_cstr, parse_date, false).await
    });

    // Convert the result to a C-compatible struct
    match result {
        Ok(pmz_result) => {
            let date_cstring = match CString::new(pmz_result.date.to_string()) {
                Ok(cs) => cs,
                Err(_) => {
                    return create_error_result(
                        PmzErrorCode::Other,
                        "Failed to convert date to C string",
                    );
                }
            };

            let result = Box::new(CPmzResult {
                error_code: PmzErrorCode::Success,
                error_message: ptr::null_mut(),
                date: date_cstring.into_raw(),
                pmh: pmz_result.pmh,
                pml: pmz_result.pml,
                prev_day_lis: pmz_result.prev_day_lis,
                is_gap_up: if pmz_result.is_gap_up { 1 } else { 0 },
                pmz_high: pmz_result.pmz_high,
                pmz_low: pmz_result.pmz_low,
                risk: pmz_result.risk,
            });

            Box::into_raw(result)
        }
        Err(e) => {
            create_error_result(
                PmzErrorCode::DataProcessingFailed,
                &format!("PMZ calculation failed: {}", e),
            )
        }
    }
}

/// Creates an error result for returning from C API functions.
unsafe fn create_error_result(code: PmzErrorCode, message: &str) -> *mut CPmzResult {
    let error_message = match CString::new(message) {
        Ok(cs) => cs,
        Err(_) => CString::new("Error message contains null bytes").unwrap(),
    };

    let result = Box::new(CPmzResult {
        error_code: code,
        error_message: error_message.into_raw(),
        date: ptr::null_mut(),
        pmh: 0.0,
        pml: 0.0,
        prev_day_lis: 0.0,
        is_gap_up: 0,
        pmz_high: 0.0,
        pmz_low: 0.0,
        risk: 0.0,
    });

    Box::into_raw(result)
}