//! Example to retrieve and analyze instrument IDs from historical OHLCV data.
use std::{collections::HashMap, error::Error};

use chrono::{DateTime, Duration, TimeZone, Utc};
use chrono_tz::US::Eastern;
use databento::{
    dbn::{OhlcvMsg, Schema, InstrumentDefMsg, SType, MappingInterval},
    historical::timeseries::GetRangeParams,
    historical::symbology::ResolveParams,
    HistoricalClient, Symbols,
};
use time;

// A simplified representation of an OHLCV candle for display and aggregation
struct Candle {
    timestamp: DateTime<chrono_tz::Tz>, // Use timezone-aware DateTime
    instrument_id: u32,                 // Added instrument_id from the record header
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: u64,
    raw_open: i64,
    raw_high: i64, 
    raw_low: i64,
    raw_close: i64,
}

impl Candle {
    fn new(ohlcv: &OhlcvMsg) -> Self {
        // Convert timestamp from nanos to a DateTime (UTC)
        let ts_nanos = ohlcv.hd.ts_event as i64;
        let seconds = ts_nanos / 1_000_000_000;
        let nanos = (ts_nanos % 1_000_000_000) as u32;
        let utc_timestamp = Utc.timestamp_opt(seconds, nanos).single().unwrap();
        
        // Convert UTC to Eastern Time
        let est_timestamp = utc_timestamp.with_timezone(&Eastern);

        // Convert fixed point prices (with 1e-9 scaling) to floating point
        let scaling_factor = 0.000000001;
        
        Candle {
            timestamp: est_timestamp,
            instrument_id: ohlcv.hd.instrument_id,
            open: ohlcv.open as f64 * scaling_factor,
            high: ohlcv.high as f64 * scaling_factor,
            low: ohlcv.low as f64 * scaling_factor,
            close: ohlcv.close as f64 * scaling_factor,
            volume: ohlcv.volume,
            raw_open: ohlcv.open,
            raw_high: ohlcv.high,
            raw_low: ohlcv.low,
            raw_close: ohlcv.close,
        }
    }

    // Format the timestamp to yyyy-mm-dd HH:MM (Eastern Time)
    fn format_timestamp(&self) -> String {
        self.timestamp.format("%Y-%m-%d %H:%M:%S").to_string()
    }
}

// Convert from chrono::DateTime to time::OffsetDateTime
// Note: We convert from Eastern Time to UTC when passing to the API
fn chrono_to_time_datetime(dt: &DateTime<chrono_tz::Tz>) -> time::OffsetDateTime {
    // Convert Eastern time to UTC before getting timestamp
    let utc_dt = dt.with_timezone(&Utc);
    let nanos = utc_dt.timestamp_nanos_opt().unwrap();
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos as i128).unwrap()
}

// Structure to hold instrument details
struct InstrumentInfo {
    name: String,
    symbol: String,
    description: Option<String>,
    asset_class: Option<String>,
    exchange_name: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Starting instrument ID analysis and mapping...");
    
    // Check if API key is set
    if std::env::var("DATABENTO_API_KEY").is_err() {
        println!("Error: DATABENTO_API_KEY environment variable is not set.");
        println!("Please set it with your API key and try again.");
        return Ok(());
    }
    
    println!("Building client...");
    let mut client = HistoricalClient::builder().key_from_env()?.build()?;
    
    // Calculate date range for the last day (to reduce data volume)
    // Use current time in Eastern Time Zone
    let now_eastern = Utc::now().with_timezone(&Eastern);
    // Set end time to 1 hour earlier to ensure it's within available data range
    let end_time = now_eastern - Duration::hours(1);
    let start_time = end_time - Duration::hours(24); // Just 24 hours of data
    
    // Convert to time crate's OffsetDateTime for the API
    let end_datetime = chrono_to_time_datetime(&end_time);
    let start_datetime = chrono_to_time_datetime(&start_time);
    
    // Try using different datasets and symbol approaches
    // Option 1: Try to get all symbols from the CME dataset
    let dataset = "GLBX.MDP3";
    
    println!("Fetching 1-minute OHLCV data for ALL symbols from {} dataset", dataset);
    println!("Time range: {} to {} (Eastern Time)", 
             start_time.format("%Y-%m-%d %H:%M:%S"), end_time.format("%Y-%m-%d %H:%M:%S"));
    
    // Request 1-minute candles with ALL symbols
    let mut decoder = client
        .timeseries()
        .get_range(
            &GetRangeParams::builder()
                .dataset(dataset)
                .date_time_range((start_datetime, end_datetime))
                .symbols(Symbols::All)  // Get all available symbols
                .schema(Schema::Ohlcv1M) // 1-minute candles
                .build(),
        )
        .await?;
    
    println!("Got decoder, retrieving metadata...");
    
    // Get metadata from the decoder
    let metadata = decoder.metadata();
    println!("Dataset: {}", metadata.dataset);
    println!("Schema: {:?}", metadata.schema);
    println!("Start: {}", metadata.start);
    println!("End: {:?}", metadata.end); // Changed to debug format
    
    println!("Retrieving OHLCV data...");
    
    // Process the OHLCV messages
    let mut candles = Vec::new();
    let mut count = 0;
    while let Some(ohlcv) = decoder.decode_record::<OhlcvMsg>().await? {
        candles.push(Candle::new(&ohlcv));
        count += 1;
        if count >= 1000 {
            // Limit to 1000 candles for analysis
            break;
        }
    }
    
    println!("Retrieved {} one-minute candles for analysis", candles.len());
    
    // Analyze unique instrument IDs
    let mut instrument_stats: HashMap<u32, (f64, f64, u64)> = HashMap::new();
    for candle in &candles {
        let entry = instrument_stats.entry(candle.instrument_id).or_insert((f64::MAX, f64::MIN, 0));
        entry.0 = entry.0.min(candle.low);  // Min price
        entry.1 = entry.1.max(candle.high); // Max price
        entry.2 += candle.volume;           // Total volume
    }
    
    // Get the unique instrument IDs we need to look up
    let instrument_ids: Vec<u32> = instrument_stats.keys().cloned().collect();
    
    // Fetch instrument definitions using the symbology endpoint
    println!("\nFetching instrument definitions for {} instruments...", instrument_ids.len());
    
    // Create a mapping of instrument ID to instrument info
    let mut instrument_map: HashMap<u32, InstrumentInfo> = HashMap::new();
    
    // Use the metadata endpoint to get instrument definitions
    if !instrument_ids.is_empty() {
        // We need to fetch metadata by dataset
        let symbology_response = client
            .symbology()
            .get_metadata(
                &GetMetadataParams::builder()
                    .dataset(dataset)
                    .start_date(start_datetime.date())
                    .build(),
            )
            .await?;
            
        // Process the symbology information
        for record in symbology_response.records {
            if instrument_ids.contains(&record.instrument_id) {
                instrument_map.insert(record.instrument_id, InstrumentInfo {
                    name: record.symbol.clone(),
                    symbol: record.symbol,
                    description: record.description,
                    asset_class: Some(record.asset_class),
                    exchange_name: record.exchange_name,
                });
            }
        }
    }
    
    println!("\nInstrument ID to Name Mapping:");
    println!("{:<12} | {:<20} | {:<30} | {:<15} | {:<20}", 
             "Instrument ID", "Symbol", "Description", "Asset Class", "Exchange");
    println!("{:-<12} | {:-<20} | {:-<30} | {:-<15} | {:-<20}", "", "", "", "", "");
    
    for id in instrument_ids {
        if let Some(info) = instrument_map.get(&id) {
            println!("{:<12} | {:<20} | {:<30} | {:<15} | {:<20}", 
                     id, 
                     info.symbol,
                     info.description.as_deref().unwrap_or("N/A"),
                     info.asset_class.as_deref().unwrap_or("N/A"),
                     info.exchange_name.as_deref().unwrap_or("N/A"));
        } else {
            // If we couldn't find the instrument info, just print the ID
            println!("{:<12} | {:<20} | {:<30} | {:<15} | {:<20}", 
                     id, "Unknown", "Not found", "N/A", "N/A");
        }
    }
    
    println!("\nUnique Instruments in Dataset:");
    println!("{:<12} | {:<20} | {:<12} | {:<20}", 
             "Instrument ID", "Price Range", "Total Volume", "Raw Price Example");
    println!("{:-<12} | {:-<20} | {:-<12} | {:-<20}", "", "", "", "");
    
    for (id, (min_price, max_price, total_volume)) in instrument_stats.iter() {
        // Find a sample raw price for this instrument
        let sample = candles.iter().find(|c| c.instrument_id == *id).unwrap();
        
        println!("{:<12} | {:7.2} - {:7.2} | {:12} | {}", 
                 id, min_price, max_price, total_volume, sample.raw_open);
    }
    println!();
    
    // Show detailed candles by instrument ID
    println!("\nDetailed 1-minute candles by instrument ID:");
    
    // Group by instrument ID
    let mut candles_by_instrument: HashMap<u32, Vec<&Candle>> = HashMap::new();
    for candle in &candles {
        candles_by_instrument.entry(candle.instrument_id).or_default().push(candle);
    }
    
    // Display sample candles for each instrument ID
    for (instrument_id, instrument_candles) in candles_by_instrument {
        if instrument_candles.is_empty() {
            continue;
        }
        
        // Get the instrument name if available
        let instrument_name = match instrument_map.get(&instrument_id) {
            Some(info) => {
                if let Some(desc) = &info.description {
                    format!("{} - {}", info.symbol, desc)
                } else {
                    info.symbol.clone()
                }
            },
            None => "Unknown".to_string(),
        };
        
        println!("\nInstrument ID: {} ({})", instrument_id, instrument_name);
        println!("Timestamp (ET)       | Open     | High     | Low      | Close    | Volume | Raw Open | Raw High | Raw Low  | Raw Close");
        println!("--------------------|----------|----------|----------|----------|--------|----------|----------|----------|----------");
        
        // Show up to 10 sample candles
        let sample_size = std::cmp::min(10, instrument_candles.len());
        for i in 0..sample_size {
            let candle = instrument_candles[i];
            println!("{} | {:8.2} | {:8.2} | {:8.2} | {:8.2} | {:6} | {} | {} | {} | {}", 
                    candle.format_timestamp(),
                    candle.open, 
                    candle.high, 
                    candle.low, 
                    candle.close, 
                    candle.volume,
                    candle.raw_open,
                    candle.raw_high,
                    candle.raw_low,
                    candle.raw_close);
        }
    }
    
    println!("\nDone!");
    
    Ok(())
}