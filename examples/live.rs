//! The example from README.md. Having it here ensures it compiles.
use std::error::Error;

use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::US::Eastern;
use databento::{
    dbn::{Dataset, PitSymbolMap, SType, Schema, TradeMsg},
    live::Subscription,
    LiveClient,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client = LiveClient::builder()
        .key_from_env()?
        .dataset(Dataset::GlbxMdp3)
        .build()
        .await?;
    client
        .subscribe(
            Subscription::builder()
                .symbols("ES.FUT")
                .schema(Schema::Trades)
                .stype_in(SType::Parent)
                .build(),
        )
        .await
        .unwrap();
    client.start().await?;

    let mut symbol_map = PitSymbolMap::new();
    // Continuously process trades
    println!("Listening for trades... Press Ctrl+C to exit.");
    println!("Timestamp (EST)        | Type | Side | Volume | Price");
    println!("---------------------|------|------|--------|--------");
    
    while let Some(rec) = client.next_record().await? {
        if let Some(trade) = rec.get::<TradeMsg>() {
            let symbol = &symbol_map[trade];
            
            // Convert ts_event from nanos to a DateTime
            let ts_nanos = trade.hd.ts_event as i64;
            let seconds = ts_nanos / 1_000_000_000;
            let nanos = (ts_nanos % 1_000_000_000) as u32;
            let utc_time = Utc.timestamp_opt(seconds, nanos).single().unwrap();
            
            // Convert UTC to EST
            let est_time: DateTime<_> = utc_time.with_timezone(&Eastern);
            
            // Determine side (Bid/Ask)
            let side = match trade.side as u8 {
                b'B' => "Bid",
                b'S' => "Ask",
                _ => "Unknown",
            };
            
            // Determine trade type based on action
            let trade_type = match trade.action as u8 {
                b'T' => "Trade",
                _ => "Other",
            };
            
            // Format price (convert from fixed point 1e-9 to decimal)
            let price = trade.price as f64 * 0.000000001;
            
            // Print simplified output
            println!(
                "{} | {:5} | {:4} | {:6} | {:.5}",
                est_time.format("%H:%M:%S"),
                trade_type,
                side,
                trade.size,
                price
            );
        }
        symbol_map.on_record(rec)?;
    }
    Ok(())
}
