# Plan: ES Futures PMZ Indicator Implementation in Rust

## 1. Goal

Implement a Rust application that calculates the Pre-Market Zone (PMZ) for the current ES futures contract based on 5-minute candle data, using logic derived from the provided `Tr3ndyPMZ.ts.txt` ThinkOrSwim script. The calculation should be performed as of 9:25 AM EST each weekday.

## 2. Core Requirements

*   **Indicator:** Pre-Market Zone (PMZ).
*   **Source Logic:** `Tr3ndyPMZ.ts.txt` (ThinkOrSwim script).
*   **Outcome:** A clear understanding of the PMZ formula and the data needed to compute it.
*   **Calculate PMZ Boundaries:** Compute the final PMZ high and low values based on the translated logic and the filtered candle data.

## 3.5. Output Results

*   **Action:** Print the calculated PMZ high and low values clearly to the console. Include the date for which the PMZ was calculated.
*   **Action:** Add comments to explain the code, especially the PMZ calculation logic.

## 4. Deliverable

*   A new Rust file `examples/es_futures_pmz.rs` containing the implementation.
*   Clear console output showing the calculated PMZ high and low for the target date.