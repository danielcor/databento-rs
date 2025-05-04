//! The example from README.md. Having it here ensures it compiles.
use std::error::Error;

use databento::{
    dbn::{Schema, TradeMsg},
    historical::timeseries::GetRangeParams,
    HistoricalClient, Symbols,
};
use time::macros::{date, datetime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Starting historical data example...");
    
    // Check if API key is set
    if std::env::var("DATABENTO_API_KEY").is_err() {
        println!("Error: DATABENTO_API_KEY environment variable is not set.");
        println!("Please set it with your API key and try again.");
        return Ok(());
    }
    
    println!("Building client...");
    let mut client = HistoricalClient::builder().key_from_env()?.build()?;
    
    // Define date range
    let start_time = datetime!(2022-06-10 14:30 UTC);
    let end_time = datetime!(2022-06-10 14:40 UTC);
    let dataset = "GLBX.MDP3";
    
    println!("Fetching historical data from {} between {} and {}...", 
             dataset, start_time, end_time);
    
    println!("Making API request...");
    let mut decoder = client
        .timeseries()
        .get_range(
            &GetRangeParams::builder()
                .dataset(dataset)
                .date_time_range((start_time, end_time))
                .symbols(Symbols::All)
                .schema(Schema::Trades)
                .build(),
        )
        .await?;
    
    println!("Got decoder, retrieving metadata...");
    
    let metadata = decoder.metadata();
    println!("Metadata: {:?}", metadata);
    
    let target_date = date!(2022 - 06 - 10);
    println!("Creating symbol map for date: {}", target_date);
    
    // Process records directly without trying to use symbol_map initially
    println!("Processing records directly...");
    
    let mut trade_count = 0;
    while let Some(trade) = decoder.decode_record::<TradeMsg>().await? {
        trade_count += 1;
        
        // Print the raw instrument ID from the trade record
        println!("Trade record {}: Instrument ID: {}, Price: {}, Size: {}", 
                 trade_count,
                 trade.hd.instrument_id, 
                 trade.price as f64 * 0.000000001, // Convert price to decimal
                 trade.size);
        
        if trade_count >= 10 {
            println!("Limiting output to first 10 trades...");
            break;
        }
    }
    
    println!("Processed {} trades", trade_count);
    
    if trade_count == 0 {
        println!("No trades found in the specified time range.");
        println!("Possible issues:");
        println!("1. The API key may not have access to this data");
        println!("2. The data may not be available for the specified date range");
        println!("3. There might be connectivity issues");
    }
    
    Ok(())
}
