//! Example to retrieve 5-minute historical candles for ES futures over the last 5 days.
use std::{collections::HashMap, error::Error};

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use chrono_tz::US::Eastern;
use databento::{
    dbn::{OhlcvMsg, Schema, SType},
    historical::timeseries::GetRangeParams,
    HistoricalClient,
};
use time;

// A simplified representation of an OHLCV candle for display and aggregation
struct Candle {
    timestamp: DateTime<chrono_tz::Tz>, // Use timezone-aware DateTime
    instrument_id: u32,                 // Added instrument_id from the record header
    symbol: String,                     // Symbol associated with this instrument
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: u64,
}

impl Candle {
    fn new(ohlcv: &OhlcvMsg, symbol_map: &HashMap<u32, String>) -> Self {
        // Convert timestamp from nanos to a DateTime (UTC)
        let ts_nanos = ohlcv.hd.ts_event as i64;
        let seconds = ts_nanos / 1_000_000_000;
        let nanos = (ts_nanos % 1_000_000_000) as u32;
        let utc_timestamp = Utc.timestamp_opt(seconds, nanos).single().unwrap();
        
        // Convert UTC to Eastern Time
        let est_timestamp = utc_timestamp.with_timezone(&Eastern);

        // Convert fixed point prices (with 1e-9 scaling) to floating point
        let scaling_factor = 0.000000001;
        
        // Look up the symbol for this instrument id, or use a placeholder
        let symbol = symbol_map.get(&ohlcv.hd.instrument_id)
            .cloned()
            .unwrap_or_else(|| format!("Unknown_{}", ohlcv.hd.instrument_id));
        
        Candle {
            timestamp: est_timestamp,
            instrument_id: ohlcv.hd.instrument_id,
            symbol,
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

// Aggregate 1-minute candles into 5-minute candles
fn aggregate_to_5min(candles: &[Candle]) -> Vec<Candle> {
    let mut result = Vec::new();
    let mut candle_map: HashMap<(String, u32), Vec<&Candle>> = HashMap::new();

    // Group by 5-minute intervals AND instrument ID
    for candle in candles {
        // Normalize to the nearest 5-minute interval (00, 05, 10, 15, etc.)
        let minute = candle.timestamp.minute();
        let normalized_minute = (minute / 5) * 5;
        
        // Create a key with the format YYYY-MM-DD HH:MM where MM is normalized to 5-min intervals
        let key = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}",
            candle.timestamp.year(),
            candle.timestamp.month(),
            candle.timestamp.day(),
            candle.timestamp.hour(),
            normalized_minute
        );
        
        // Use both timestamp and instrument ID as key
        candle_map.entry((key, candle.instrument_id)).or_default().push(candle);
    }

    // Aggregate each group into a single 5-minute candle
    for ((timestamp_key, instrument_id), group) in candle_map {
        if group.is_empty() {
            continue;
        }

        // Parse the key back to a DateTime in Eastern Time
        let timestamp = match DateTime::parse_from_str(&format!("{}:00 {}", timestamp_key, group[0].timestamp.format("%z").to_string()), "%Y-%m-%d %H:%M:%S %z") {
            Ok(dt) => dt.with_timezone(&Eastern),
            Err(_) => continue,
        };

        // Create a new aggregated candle
        let open = group.first().unwrap().open;
        let close = group.last().unwrap().close;
        let high = group.iter().map(|c| c.high).fold(f64::MIN, f64::max);
        let low = group.iter().map(|c| c.low).fold(f64::MAX, f64::min);
        let volume = group.iter().map(|c| c.volume).sum();

        result.push(Candle {
            timestamp,
            instrument_id,
            symbol: group.first().unwrap().symbol.clone(),
            open,
            high,
            low,
            close,
            volume,
        });
    }

    // Sort by timestamp and then by instrument ID
    result.sort_by(|a, b| a.timestamp.cmp(&b.timestamp).then(a.instrument_id.cmp(&b.instrument_id)));
    return result;
}

// Convert from chrono::DateTime to time::OffsetDateTime
// Note: We convert from Eastern Time to UTC when passing to the API
fn chrono_to_time_datetime(dt: &DateTime<chrono_tz::Tz>) -> time::OffsetDateTime {
    // Convert Eastern time to UTC before getting timestamp
    let utc_dt = dt.with_timezone(&Utc);
    let nanos = utc_dt.timestamp_nanos_opt().unwrap();
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos as i128).unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Starting historical OHLCV example for ES futures...");
    
    // Check if API key is set
    if std::env::var("DATABENTO_API_KEY").is_err() {
        println!("Error: DATABENTO_API_KEY environment variable is not set.");
        println!("Please set it with your API key and try again.");
        return Ok(());
    }
    
    println!("Building client...");
    let mut client = HistoricalClient::builder().key_from_env()?.build()?;
    
    // Get current time in Eastern Time Zone
    let now_eastern = Utc::now().with_timezone(&Eastern);
    
    // Determine a valid market time that avoids weekends and maintenance break
    // Futures market hours: Sunday 6pm to Friday 5pm EST (except 5-6pm EST daily maintenance)
    let mut end_time = now_eastern;
    
    // Adjust for weekend - if it's weekend, move to Friday 4:30pm
    let weekday = end_time.weekday();
    if weekday == chrono::Weekday::Sat || 
       (weekday == chrono::Weekday::Sun && end_time.hour() < 18) || 
       (weekday == chrono::Weekday::Fri && end_time.hour() >= 17) {
        // Find the most recent Friday at 4:30pm EST
        let days_to_subtract = match weekday {
            chrono::Weekday::Sat => 1,
            chrono::Weekday::Sun => if end_time.hour() < 18 { 2 } else { 0 },
            chrono::Weekday::Fri => if end_time.hour() >= 17 { 0 } else { 7 },
            _ => 0,
        };
        
        if days_to_subtract > 0 {
            end_time = (end_time - Duration::days(days_to_subtract))
                .with_hour(16)
                .unwrap()
                .with_minute(30)
                .unwrap()
                .with_second(0)
                .unwrap()
                .with_nanosecond(0)
                .unwrap();
        }
    }
    
    // Avoid the daily maintenance break (5pm-6pm EST)
    if end_time.hour() == 17 {
        // Move to 4:30pm instead
        end_time = end_time
            .with_hour(16)
            .unwrap()
            .with_minute(30)
            .unwrap();
    }
    
    // Calculate start time (5 trading days back)
    // Note: We're using calendar days here, not adjusting for weekends in the start time
    let start_time = end_time - Duration::days(5);
    
    // Convert to time crate's OffsetDateTime for the API
    let end_datetime = chrono_to_time_datetime(&end_time);
    let start_datetime = chrono_to_time_datetime(&start_time);
    
    let dataset = "GLBX.MDP3";
    let symbol = "ES.FUT"; // ES futures
    
    println!("Fetching 1-minute OHLCV data for {} from {} to {} (Eastern Time)...", 
             symbol, start_time.format("%Y-%m-%d %H:%M:%S"), end_time.format("%Y-%m-%d %H:%M:%S"));
    
    // First, resolve symbols to get instrument IDs
    println!("Resolving symbols to instrument IDs...");
    let resolution = client.symbology()
        .resolve(
            &databento::historical::symbology::ResolveParams::builder()
                .dataset(dataset)
                .symbols(symbol)
                .stype_in(SType::Parent)
                .stype_out(SType::InstrumentId)
                .date_range(databento::historical::DateTimeRange::from((start_datetime, end_datetime)))
                .build(),
        )
        .await?;
    
    // Create a mapping from instrument ID to symbol
    let mut instrument_id_to_symbol: HashMap<u32, String> = HashMap::new();
    
    // Print detailed metadata for all instruments
    println!("\nInstrument Metadata from Symbol Resolution:");
    println!("{:<12} | {:<15} | {:<20}", 
             "Instrument ID", "Symbol", "Date Range");
    println!("{:-<12} | {:-<15} | {:-<20}", "", "", "");
    
    for (symbol, mappings) in &resolution.mappings {
        for mapping in mappings {
            let instrument_id = mapping.symbol.parse::<u32>().unwrap_or_default();
            instrument_id_to_symbol.insert(instrument_id, symbol.clone());
            
            // Print metadata for all instruments
            println!("{:<12} | {:<15} | {} to {}", 
                    instrument_id, 
                    symbol,
                    mapping.start_date,
                    mapping.end_date);
        }
    }
    println!();
    
    // Request 1-minute candles
    let mut decoder = client
        .timeseries()
        .get_range(
            &GetRangeParams::builder()
                .dataset(dataset)
                .date_time_range((start_datetime, end_datetime))
                .symbols(symbol)
                .schema(Schema::Ohlcv1M) // 1-minute candles
                .stype_in(SType::Parent)
                .build(),
        )
        .await?;
    
    println!("Got decoder, retrieving OHLCV data...");
    
    // Process the OHLCV messages
    let mut candles = Vec::new();
    while let Some(ohlcv) = decoder.decode_record::<OhlcvMsg>().await? {
        candles.push(Candle::new(&ohlcv, &instrument_id_to_symbol));
    }
    
    println!("Retrieved {} one-minute candles", candles.len());
    
    // Analyze unique instrument IDs
    let mut instrument_stats: HashMap<u32, (f64, f64, u64, String)> = HashMap::new();
    for candle in &candles {
        let entry = instrument_stats.entry(candle.instrument_id).or_insert((f64::MAX, f64::MIN, 0, candle.symbol.clone()));
        entry.0 = entry.0.min(candle.low);  // Min price
        entry.1 = entry.1.max(candle.high); // Max price
        entry.2 += candle.volume;           // Total volume
    }
    
    println!("\nUnique Instruments in Dataset:");
    println!("{:<12} | {:<15} | {:<20} | {:<12}", 
             "Instrument ID", "Symbol", "Price Range", "Total Volume");
    println!("{:-<12} | {:-<15} | {:-<20} | {:-<12}", "", "", "", "");
    
    for (id, (min_price, max_price, total_volume, symbol)) in instrument_stats.iter() {
        println!("{:<12} | {:<15} | {:7.2} - {:7.2} | {:12}", 
                 id, symbol, min_price, max_price, total_volume);
    }
    println!();
    
    // Group 1-minute candles by their 5-minute interval key
    let mut candles_by_interval: HashMap<String, Vec<&Candle>> = HashMap::new();
    for candle in &candles {
        // Normalize to the nearest 5-minute interval (00, 05, 10, 15, etc.)
        let minute = candle.timestamp.minute();
        let normalized_minute = (minute / 5) * 5;
        
        // Create a key with the format YYYY-MM-DD HH:MM where MM is normalized to 5-min intervals
        let key = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}",
            candle.timestamp.year(),
            candle.timestamp.month(),
            candle.timestamp.day(),
            candle.timestamp.hour(),
            normalized_minute
        );
        
        candles_by_interval.entry(key).or_default().push(candle);
    }
    
    // Aggregate into 5-minute candles
    let aggregated_candles = aggregate_to_5min(&candles);
    println!("Aggregated into {} five-minute candles", aggregated_candles.len());
    
    // Group the aggregated candles by instrument ID for display
    let mut candles_by_instrument: HashMap<u32, Vec<&Candle>> = HashMap::new();
    for candle in &aggregated_candles {
        candles_by_instrument.entry(candle.instrument_id).or_default().push(candle);
    }
    
    // Display the 5-minute candles for each instrument separately
    for (instrument_id, instrument_candles) in candles_by_instrument {
        let symbol = instrument_candles[0].symbol.clone();
        println!("\nInstrument ID: {} (Symbol: {})", instrument_id, symbol);
        println!("Timestamp (ET)       | Open     | High     | Low      | Close    | Volume");
        println!("--------------------|----------|----------|----------|----------|--------");
        
        for candle in instrument_candles {
            println!("{} | {:8.2} | {:8.2} | {:8.2} | {:8.2} | {:7}",
                    candle.format_timestamp(), 
                    candle.open, 
                    candle.high, 
                    candle.low, 
                    candle.close, 
                    candle.volume);
        }
    }
    
    println!("\nDone!");
    
    Ok(())
}