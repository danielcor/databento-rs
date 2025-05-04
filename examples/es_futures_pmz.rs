use anyhow::Result;
use databento::{
    dbn::{Encoding, OhlcvMsg, Schema, SType}, // Changed Ohlcv1M to OhlcvMsg, added SType
    historical::{
        metadata::ListFieldsParams,
        symbology::ResolveParams,
        timeseries::GetRangeParams, ClientBuilder,
        DateRange, DateTimeRange,
    },
};
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc, Datelike}; // Added DateTime, Timelike
use chrono_tz::{America::New_York, US::Eastern}; // Added Eastern
use std::{collections::HashMap, env}; // Added HashMap
// Import necessary types from the `time` crate
use time::{Date, OffsetDateTime};

// --- Candle Struct (adapted from ohlcv_candles.rs) ---
#[derive(Debug, Clone)] // Added Debug, Clone
struct Candle {
    timestamp: DateTime<chrono_tz::Tz>, // Use timezone-aware DateTime (Eastern)
    instrument_id: u32,
    symbol: String, // Assuming a single symbol for simplicity here
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: u64,
}

impl Candle {
    // Simplified constructor for this example, assuming symbol is known
    fn new(ohlcv: &OhlcvMsg, symbol: &str) -> Self {
        // Convert timestamp from nanos to a DateTime (UTC)
        let ts_nanos = ohlcv.hd.ts_event as i64;
        let seconds = ts_nanos / 1_000_000_000;
        let nanos = (ts_nanos % 1_000_000_000) as u32;
        let utc_timestamp = Utc.timestamp_opt(seconds, nanos).single().unwrap();

        // Convert UTC to Eastern Time
        let est_timestamp = utc_timestamp.with_timezone(&Eastern);

        // Convert fixed point prices (with 1e-9 scaling) to floating point
        let scaling_factor = 1e-9; // Use 1e-9 directly

        Candle {
            timestamp: est_timestamp,
            instrument_id: ohlcv.hd.instrument_id,
            symbol: symbol.to_string(), // Use the passed symbol
            open: ohlcv.open as f64 * scaling_factor,
            high: ohlcv.high as f64 * scaling_factor,
            low: ohlcv.low as f64 * scaling_factor,
            close: ohlcv.close as f64 * scaling_factor,
            volume: ohlcv.volume,
        }
    }

    // Format the timestamp to yyyy-mm-dd HH:MM (Eastern Time)
    fn format_timestamp(&self) -> String {
        self.timestamp.format("%Y-%m-%d %H:%M").to_string()
    }
}

// --- Aggregation Function (adapted from ohlcv_candles.rs) ---
// Takes a slice of 1-min candles and aggregates them into 5-min candles
fn aggregate_candles(candles: &[Candle], interval_minutes: u32) -> Vec<Candle> {
    let mut result = Vec::new();
    let mut candle_map: HashMap<String, Vec<&Candle>> = HashMap::new();

    // Group by interval_minutes intervals
    for candle in candles {
        let minute = candle.timestamp.minute();
        let normalized_minute = (minute / interval_minutes) * interval_minutes;

        let key = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}",
            candle.timestamp.year(),
            candle.timestamp.month(),
            candle.timestamp.day(),
            candle.timestamp.hour(),
            normalized_minute
        );

        candle_map.entry(key).or_default().push(candle);
    }

    // Aggregate each group
    for (timestamp_key, group) in candle_map {
        if group.is_empty() {
            continue;
        }

        // Parse the key back to a DateTime in Eastern Time
        let timestamp = match DateTime::parse_from_str(&format!("{}:00 +0000", timestamp_key), "%Y-%m-%d %H:%M:%S %z") {
             Ok(dt_utc) => dt_utc.with_timezone(&Eastern),
             Err(e) => {
                 eprintln!("Error parsing timestamp key '{}': {}", timestamp_key, e);
                 continue;
             }
         };

        let open = group.first().unwrap().open;
        let close = group.last().unwrap().close;
        let high = group.iter().map(|c| c.high).fold(f64::MIN, f64::max);
        let low = group.iter().map(|c| c.low).fold(f64::MAX, f64::min);
        let volume = group.iter().map(|c| c.volume).sum();

        result.push(Candle {
            timestamp,
            instrument_id: group.first().unwrap().instrument_id,
            symbol: group.first().unwrap().symbol.clone(),
            open,
            high,
            low,
            close,
            volume,
        });
    }

    result.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
    result
}

// Function to check if a given date is a weekend
fn is_weekend(date: &NaiveDate) -> bool {
    use chrono::Weekday::*;
    let weekday = date.weekday();
    weekday == Sat || weekday == Sun
}

// Function to get the previous trading day (skipping weekends)
fn get_previous_trading_day(date: NaiveDate) -> NaiveDate {
    let mut prev_day = date - Duration::days(1);
    while is_weekend(&prev_day) {
        prev_day -= Duration::days(1);
    }
    prev_day
}

#[tokio::main]
async fn main() -> Result<()> {
    // --- Configuration ---
    let api_key = env::var("DATABENTO_API_KEY")
        .map_err(|_| anyhow::anyhow!("DATABENTO_API_KEY environment variable not set"))?; // Use anyhow for generic error
    let dataset = "GLBX.MDP3"; // CME Globex MDP3
    let symbol = "ES.c.0"; // Continuous front-month ES contract
    let schema = Schema::Ohlcv1M; // Ensure schema is Ohlcv1M

    // --- Date and Time Setup ---
    let today_naive = Utc::now().date_naive(); // Use today's date in UTC
    // Ensure we are calculating for a weekday, if today is weekend, find previous friday
    let mut current_trading_day_naive = today_naive;
     while is_weekend(&current_trading_day_naive) {
        current_trading_day_naive = current_trading_day_naive - Duration::days(1);
    }
    let previous_trading_day_naive = get_previous_trading_day(current_trading_day_naive);

    // Define the time range in New York time
    let tz = New_York;
    let pmz_start_time = NaiveTime::from_hms_opt(7, 25, 0).unwrap(); // PMZ Start (inclusive)
    let pmz_end_time = NaiveTime::from_hms_opt(9, 25, 0).unwrap();   // PMZ End (exclusive)
    let market_open_time = NaiveTime::from_hms_opt(9, 30, 0).unwrap(); // Market Open (for gap calc)
    let market_open_end_time = NaiveTime::from_hms_opt(9, 35, 0).unwrap(); // End of Market Open candle
    let lis_time = NaiveTime::from_hms_opt(15, 55, 0).unwrap(); // LIS candle start (ends 16:00)
    let lis_end_time = NaiveTime::from_hms_opt(16, 0, 0).unwrap(); // LIS candle end

    // Define UTC query range: Previous day LIS time to Current day LIS time + buffer
    let query_start_dt_naive = NaiveDateTime::new(previous_trading_day_naive, NaiveTime::from_hms_opt(15, 50, 0).unwrap());
    let query_end_dt_naive = NaiveDateTime::new(current_trading_day_naive, NaiveTime::from_hms_opt(16, 5, 0).unwrap());

    let query_start_dt_utc = tz.from_local_datetime(&query_start_dt_naive).unwrap().with_timezone(&Utc);
    let query_end_dt_utc = tz.from_local_datetime(&query_end_dt_naive).unwrap().with_timezone(&Utc);

    // Convert query times for databento API
    let query_start_dt_offset = OffsetDateTime::from_unix_timestamp_nanos(query_start_dt_utc.timestamp_nanos_opt().unwrap_or(0).into())?;
    let query_end_dt_offset = OffsetDateTime::from_unix_timestamp_nanos(query_end_dt_utc.timestamp_nanos_opt().unwrap_or(0).into())?;

    println!(
        "Calculating PMZ for {} (Previous Trading Day: {})",
        current_trading_day_naive.format("%Y-%m-%d"),
        previous_trading_day_naive.format("%Y-%m-%d")
    );
    println!("Querying 1-min data from {} to {}", query_start_dt_utc, query_end_dt_utc);

    // --- Databento Client ---
    let mut client = ClientBuilder::new()
        .key(api_key)?
        .build()?; // Use .build() for the historical client

    // --- Fetch Data ---
    // Construct DateTimeRange using From trait
    let date_time_range = DateTimeRange::from((query_start_dt_offset, query_end_dt_offset));

    let params = GetRangeParams::builder()
        .dataset(dataset.to_string())
        .symbols(vec![symbol.to_string()]) // Use the specific symbol
        .schema(schema)
        .stype_in(SType::Continuous) // Specify SType for continuous contract
        .date_time_range(date_time_range) // Pass the constructed range
        .build();

    // Pass params by reference
    let mut data_decoder = client.timeseries().get_range(&params).await?;

    // --- Process 1-min Candles ---
    let mut all_one_min_candles: Vec<Candle> = Vec::new(); // Store ALL 1-min candles from query
    let mut record_count = 0;

    // Use decode_record() with OhlcvMsg
    while let Some(record) = data_decoder.decode_record::<OhlcvMsg>().await? {
         record_count += 1;
         // Create a 1-min Candle (constructor converts to EST)
         let candle = Candle::new(&record, &symbol);
         all_one_min_candles.push(candle);
    }

    println!("Retrieved {} one-minute records in query range.", record_count);

    // --- Calculate Previous Day LIS ---
    let prev_lis_start_est = tz.from_local_datetime(&NaiveDateTime::new(previous_trading_day_naive, lis_time)).unwrap();
    let prev_lis_end_est = tz.from_local_datetime(&NaiveDateTime::new(previous_trading_day_naive, lis_end_time)).unwrap();
    let prev_lis_one_min: Vec<Candle> = all_one_min_candles
        .iter()
        .filter(|c| c.timestamp >= prev_lis_start_est && c.timestamp < prev_lis_end_est)
        .cloned()
        .collect();
    let prev_lis_five_min = aggregate_candles(&prev_lis_one_min, 5);
    let prev_day_lis: Option<f64> = prev_lis_five_min.first().map(|c| c.close);

    // --- Calculate Current 9:30 AM Open ---
    let open_start_est = tz.from_local_datetime(&NaiveDateTime::new(current_trading_day_naive, market_open_time)).unwrap();
    let open_end_est = tz.from_local_datetime(&NaiveDateTime::new(current_trading_day_naive, market_open_end_time)).unwrap();
    let open_one_min: Vec<Candle> = all_one_min_candles
        .iter()
        .filter(|c| c.timestamp >= open_start_est && c.timestamp < open_end_est)
        .cloned()
        .collect();
    let open_five_min = aggregate_candles(&open_one_min, 5);
    let current_day_930_open: Option<f64> = open_five_min.first().map(|c| c.open);

    // --- Determine Gap Direction ---
    let gap_up: Option<bool> = match (current_day_930_open, prev_day_lis) {
        (Some(open), Some(lis)) => Some(open >= lis),
        _ => None, // Cannot determine gap if open or prev LIS is missing
    };
    let gap_direction_str = match gap_up {
        Some(true) => "Up",
        Some(false) => "Down",
        None => "Unknown",
    };

    // --- Filter & Aggregate PMZ Candles (Current Day 7:25 - 9:25 EST) ---
    let pmz_filter_start_est = tz.from_local_datetime(&NaiveDateTime::new(current_trading_day_naive, pmz_start_time)).unwrap();
    let pmz_filter_end_est = tz.from_local_datetime(&NaiveDateTime::new(current_trading_day_naive, pmz_end_time)).unwrap();
    let pmz_one_min_candles: Vec<Candle> = all_one_min_candles
        .iter()
        .filter(|c| c.timestamp >= pmz_filter_start_est && c.timestamp < pmz_filter_end_est)
        .cloned()
        .collect();
    println!("Found {} one-minute candles within PMZ ({} - {} EST).", pmz_one_min_candles.len(), pmz_start_time.format("%H:%M:%S"), pmz_end_time.format("%H:%M:%S"));
    let pmz_five_min_candles = aggregate_candles(&pmz_one_min_candles, 5);
    println!("Aggregated PMZ into {} five-minute candles.", pmz_five_min_candles.len());

    // --- Calculate PMH and PML ---
    let pmh: Option<f64> = pmz_five_min_candles.iter().map(|c| c.high).fold(None, |max_h, h| Some(max_h.map_or(h, |current_max| current_max.max(h))));
    let pml: Option<f64> = pmz_five_min_candles.iter().map(|c| c.low).fold(None, |min_l, l| Some(min_l.map_or(l, |current_min| current_min.min(l))));

    // --- Calculate Risk Range & PMZ Width ---
    let risk_range: Option<f64> = pmh.zip(pml).map(|(h, l)| h - l);
    let pmz_width: Option<f64> = risk_range.map(|r| r * 0.2);

    // --- Calculate PMZ High/Low based on Gap ---
    let (pmz_high, pmz_low) = match (gap_up, pmh, pml, risk_range) {
        (Some(true), Some(h), _, Some(r)) => (Some(h - r * 0.2), Some(h - r * 0.4)), // Gap Up
        (Some(false), _, Some(l), Some(r)) => (Some(l + r * 0.4), Some(l + r * 0.2)), // Gap Down
        _ => (None, None), // Cannot calculate if gap or PMH/PML/Risk is missing
    };

    // --- Calculate Current Day LIS ---
    let current_lis_start_est = tz.from_local_datetime(&NaiveDateTime::new(current_trading_day_naive, lis_time)).unwrap();
    let current_lis_end_est = tz.from_local_datetime(&NaiveDateTime::new(current_trading_day_naive, lis_end_time)).unwrap();
    let current_lis_one_min: Vec<Candle> = all_one_min_candles
        .iter()
        .filter(|c| c.timestamp >= current_lis_start_est && c.timestamp < current_lis_end_est)
        .cloned()
        .collect();
    let current_lis_five_min = aggregate_candles(&current_lis_one_min, 5);
    let current_day_lis: Option<f64> = current_lis_five_min.first().map(|c| c.close);

    // --- Calculate Upper/Lower Risk Levels ---
    let upper_risk: Option<f64> = pmz_high.zip(risk_range).map(|(h, r)| h + r);
    let lower_risk: Option<f64> = pmz_low.zip(risk_range).map(|(l, r)| l - r);

    // --- Output Results ---
    println!("-------------------------------------");
    println!("Date: {}", current_trading_day_naive.format("%Y-%m-%d"));

    // Print calculated values, handling Optionals
    println!("Prev Day LIS (Close @ {:?} EST): {}",
        NaiveDateTime::new(previous_trading_day_naive, lis_end_time).format("%Y-%m-%d %H:%M"),
        prev_day_lis.map_or_else(|| "N/A".to_string(), |v| format!("{:.2}", v)));
    println!("Curr Day Open (@ {:?} EST): {}",
        NaiveDateTime::new(current_trading_day_naive, market_open_time).format("%H:%M"),
        current_day_930_open.map_or_else(|| "N/A".to_string(), |v| format!("{:.2}", v)));
    println!("Gap Direction: {}", gap_direction_str);

    if let (Some(h), Some(l)) = (pmh, pml) {
         println!("PMH ({} - {} EST): {:.2}", pmz_start_time.format("%H:%M"), pmz_end_time.format("%H:%M"), h);
         println!("PML ({} - {} EST): {:.2}", pmz_start_time.format("%H:%M"), pmz_end_time.format("%H:%M"), l);
         if let Some(r) = risk_range {
             println!("Risk Range (PMH-PML): {:.2}", r);
         }
         if let Some(w) = pmz_width {
             println!("PMZ Width (Risk*0.2): {:.2}", w);
         }
         if let Some(ph) = pmz_high {
             println!("PMZ High: {:.2}", ph);
         }
         if let Some(pl) = pmz_low {
             println!("PMZ Low: {:.2}", pl);
         }
         if let Some(ur) = upper_risk {
             println!("Upper Risk (PMZ High + Risk Range): {:.2}", ur);
         }
         if let Some(lr) = lower_risk {
             println!("Lower Risk (PMZ Low - Risk Range): {:.2}", lr);
         }
    } else {
        println!("PMH/PML: Could not calculate (No data in PMZ range?)");
    }

    println!("Curr Day LIS (Close @ {:?} EST): {}",
        NaiveDateTime::new(current_trading_day_naive, lis_end_time).format("%H:%M"),
        current_day_lis.map_or_else(|| "N/A".to_string(), |v| format!("{:.2}", v)));

    // Optional: Print the 5-min PMZ candles
    println!("
Aggregated 5-Minute PMZ Candles ({} - {} EST):", pmz_start_time.format("%H:%M"), pmz_end_time.format("%H:%M"));
    println!("Timestamp (ET)       | Open     | High     | Low      | Close    | Volume");
    println!("--------------------|----------|----------|----------|----------|--------");
    for candle in &pmz_five_min_candles {
        println!("{} | {:8.2} | {:8.2} | {:8.2} | {:8.2} | {:7}",
                candle.format_timestamp(),
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume);
    }

    // Optional: Print the 5-min 9:30 candle if found
    if let Some(open_candle) = open_five_min.first() {
       println!("
Aggregated 5-Minute Market Open Candle ({} - {} EST):", market_open_time.format("%H:%M"), market_open_end_time.format("%H:%M"));
       println!("Timestamp (ET)       | Open     | High     | Low      | Close    | Volume");
       println!("--------------------|----------|----------|----------|----------|--------");
       println!("{} | {:8.2} | {:8.2} | {:8.2} | {:8.2} | {:7}",
               open_candle.format_timestamp(),
               open_candle.open,
               open_candle.high,
               open_candle.low,
               open_candle.close,
               open_candle.volume);
    }

    // Optional: Print the 5-min LIS candles (previous and current day)
    if let Some(prev_lis_candle) = prev_lis_five_min.first() {
        println!("
Aggregated 5-Minute Prev Day LIS Candle ({} - {} EST):", lis_time.format("%H:%M"), lis_end_time.format("%H:%M"));
        println!("Timestamp (ET)       | Open     | High     | Low      | Close    | Volume");
        println!("--------------------|----------|----------|----------|----------|--------");
        println!("{} | {:8.2} | {:8.2} | {:8.2} | {:8.2} | {:7}",
                prev_lis_candle.format_timestamp(),
                prev_lis_candle.open,
                prev_lis_candle.high,
                prev_lis_candle.low,
                prev_lis_candle.close,
                prev_lis_candle.volume);
    }
    if let Some(curr_lis_candle) = current_lis_five_min.first() {
        println!("
Aggregated 5-Minute Curr Day LIS Candle ({} - {} EST):", lis_time.format("%H:%M"), lis_end_time.format("%H:%M"));
        println!("Timestamp (ET)       | Open     | High     | Low      | Close    | Volume");
        println!("--------------------|----------|----------|----------|----------|--------");
        println!("{} | {:8.2} | {:8.2} | {:8.2} | {:8.2} | {:7}",
                curr_lis_candle.format_timestamp(),
                curr_lis_candle.open,
                curr_lis_candle.high,
                curr_lis_candle.low,
                curr_lis_candle.close,
                curr_lis_candle.volume);
    }

    // Metadata fetching if PMH/PML failed
    if pmh.is_none() || pml.is_none() {
        println!("Attempting to fetch metadata for dataset {}...", dataset);

        // Correct metadata calls: Pass dataset directly if no Params struct exists
        match client.metadata().list_schemas(dataset).await { // Pass dataset directly
            Ok(schemas) => println!("Available schemas: {:?}", schemas),
            Err(e) => eprintln!("Failed to fetch schemas: {}", e),
        }

        // ListFieldsParams builder only takes encoding and schema
        let fields_params = ListFieldsParams::builder()
            .encoding(Encoding::Dbn) // Added encoding (assuming DBN)
            .schema(schema) // Changed to required schema, not Option<Schema>
            .build();
        match client.metadata().list_fields(&fields_params).await {
            Ok(fields) => println!("Fields for schema {:?}: {:?}", schema, fields),
            Err(e) => eprintln!("Failed to fetch fields for schema {:?}: {}", schema, e),
        }

        // Pass dataset directly for list_unit_prices
        match client.metadata().list_unit_prices(dataset).await { // Pass dataset directly
            Ok(prices) => println!("Unit prices for dataset {}: {:?}", dataset, prices),
            Err(e) => eprintln!("Failed to fetch unit prices: {}", e),
        }

        // Convert chrono::NaiveDate to time::Date
        // Convert month() u32 to u8 before TryFrom
        let month_u8 = target_date_naive.month() as u8;
        let time_month = time::Month::try_from(month_u8)?;

        let target_date_time = Date::from_calendar_date(
            target_date_naive.year(),
            time_month, // Use the converted time::Month
            target_date_naive.day() as u8,
        )?;

        // Construct DateRange using From trait
        let date_range = DateRange::from((target_date_time, target_date_time));

        let resolve_params = ResolveParams::builder()
            .dataset(dataset.to_string())
            .symbols(vec![symbol.to_string()])
            .date_range(date_range) // Pass the constructed range
            .build();
        match client.symbology().resolve(&resolve_params).await {
            Ok(resolution) => println!("Symbology resolution for {}: {:?}", symbol, resolution),
            Err(e) => eprintln!("Failed to resolve symbology for {}: {}", symbol, e),
        }
    }
    println!("-------------------------------------");

    Ok(())
}