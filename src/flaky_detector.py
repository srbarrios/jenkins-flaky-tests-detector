import time
import json
import yaml
import logging
import argparse
import pandas as pd
from datetime import datetime, timedelta
from prometheus_api_client import PrometheusConnect
from google import genai
from google.genai import types

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MetricHarvester:
    def __init__(self, prom_url):
        self.prom = PrometheusConnect(url=prom_url, disable_ssl=True)

    def fetch_history(self, job_name, days, step):
        """Fetches failure age and reconstructs the sparse time series."""
        logger.info(f"Fetching metrics for job: {job_name} over last {days} days...")

        start_time = datetime.now() - timedelta(days=days)
        end_time = datetime.now()

        # Query for FAILED or REGRESSION statuses
        query = f'jenkins_build_test_case_failure_age{{jobname="{job_name}", status=~"FAILED|REGRESSION"}}'

        # Get data as a list of metric objects
        result = self.prom.custom_query_range(
            query=query,
            start_time=start_time,
            end_time=end_time,
            step=step
        )

        if not result:
            logger.warning("No metrics found.")
            return {}

        # Log sample of raw data to debug
        # logger.info(f"Raw Result Sample: {str(result)[:500]}")

        return self._normalize_data(result, start_time, end_time, step)

    def _normalize_data(self, raw_data, start, end, step):
        """
        Converts Prometheus format to a continuous timeline.
        IMPUTATION LOGIC: Reindexes time series to fill missing gaps with 0.
        """
        logger.info("Normalizing data and imputing '0' for missing data points...")
        normalized_history = {}

        pd_start = pd.Timestamp(start).floor(f'{step}s')
        pd_end = pd.Timestamp(end).ceil(f'{step}s')

        full_time_index = pd.date_range(start=pd_start, end=pd_end, freq=f'{step}s')

        for entry in raw_data:
            metric = entry['metric']
            suite = metric.get('suite', 'unknown')
            case = metric.get('case', 'unknown')
            full_name = f"{suite}::{case}"

            # Create DataFrame from specific metric history
            values = entry['values']
            df = pd.DataFrame(values, columns=['ds', 'y'])
            df['ds'] = pd.to_datetime(df['ds'], unit='s')
            df['y'] = pd.to_numeric(df['y'])

            df = df.set_index('ds')

            # Resample and fill missing intervals with 0 (Pass)
            try:
                # 'max' takes the worst status in that hour (if multiple runs occurred)
                df_resampled = df.resample(f'{step}s').max()

                # Reindex aligns the resampled data to our perfect timeline
                df_reindexed = df_resampled.reindex(full_time_index).fillna(0)

                # Store the sequence of integers
                normalized_history[full_name] = df_reindexed['y'].astype(int).tolist()
            except Exception as e:
                logger.error(f"Error normalizing {full_name}: {e}")
                continue

        return normalized_history

class FlakeAnalyzer:
    def __init__(self, api_key, model_name):
        self.client = genai.Client(api_key=api_key)
        self.model_name = model_name

    def analyze_batch(self, test_histories):
        """Sends a batch of time-series data to Gemini for classification."""

        response_schema = types.Schema(
            type=types.Type.ARRAY,
            items=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "test_suite": types.Schema(type=types.Type.STRING),
                    "test_case": types.Schema(type=types.Type.STRING),
                    "flakiness_score": types.Schema(type=types.Type.NUMBER, description="Confidence 0.0 to 1.0"),
                    "failure_pattern": types.Schema(type=types.Type.STRING, enum=["FLAKY", "REGRESSION", "ENVIRONMENTAL", "UNKNOWN"]),
                    "reasoning": types.Schema(type=types.Type.STRING),
                },
                required=["test_suite", "test_case", "flakiness_score", "reasoning"]
            )
        )

        # --- IMPROVED PROMPT ---
        prompt = f"""
        You are a Senior QA Automation Engineer specializing in detecting 'Flaky Tests' in complex infrastructure (Salt/Linux).
        Analyze the following time-series data of 'Failure Age' (consecutive failures) over 15 days.
        
        Data Legend:
        - 0: Passed
        - 1, 2, 3...: The test has failed for X consecutive builds.
        
        CRITICAL CLASSIFICATION RULES:
        
        1. FLAKY (High Priority):
           - Chaotic oscillation: `1, 0, 1, 0, 2, 0` (Randomly fails).
           - **CLUSTER FLAKES (Common):** A test fails for a short burst and then passes WITHOUT a long-term pattern.
             - Example: `0, 0, 1, 2, 0, 1, 0` (Failed 2 times then passed, then failed again once). 
             - Logic: If a failure streak is short (< 3 consecutive) and resolves, assume it is FLAKY/Unstable infrastructure, NOT a code regression.
        
        2. REGRESSION (Real Bugs):
           - Long, sustained failure blocks that require a fix.
           - Example: `0, 0, 1, 2, 3, 4, 5, 6, 7... 15`. (Broken for days).
           - Logic: If failure age climbs high (> 6) or persists for the majority of the dataset, it is a Regression.
        
        3. ENVIRONMENTAL:
           - Example: `25, 25, 25` or `10, 10, 10` (Metric is stuck/stale).
           - Logic: The failure age isn't changing, implying the job isn't running or the metric is frozen.

        Task: Identify the Flaky tests. Be aggressive in marking short failure streaks as FLAKY.
        
        Analyze these tests:
        {json.dumps(test_histories)}
        """

        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=response_schema,
                    temperature=0.2
                )
            )
            return json.loads(response.text)
        except Exception as e:
            logger.error(f"Gemini API Error: {e}")
            return []

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="/etc/monitoring/flaky_config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    # 1. Harvest Data
    harvester = MetricHarvester(config['prometheus']['url'])
    histories = harvester.fetch_history(
        config['prometheus']['job_name'],
        config['prometheus']['lookback_days'],
        config['prometheus']['step_seconds']
    )

    # 2. Pre-Filtering (Optimization)
    candidates = {}
    for test, history in histories.items():
        # Heuristic: Ignore if never failed (sum=0)
        # We also might want to ignore if it is purely all 0s except for the last day?
        # For now, just ensuring it has SOME failures is enough.
        if sum(history) > 0:
            candidates[test] = history

    logger.info(f"Identified {len(candidates)} candidates for AI analysis.")

    # 3. Analyze with Gemini (Batching)
    if not candidates:
        logger.warning("No candidates found. Exiting analysis.")
        # Create empty file to prevent Grafana 404
        output_path = f"{config['output']['directory']}/{config['output']['filename']}"
        with open(output_path, 'w') as f:
            json.dump([], f)
        return

    analyzer = FlakeAnalyzer(config['gemini']['api_key'], config['gemini']['model'])
    final_results = []

    batch_size = 30 # Avoid token limits
    test_items = list(candidates.items())

    for i in range(0, len(test_items), batch_size):
        batch = dict(test_items[i:i+batch_size])
        logger.info(f"Processing batch {i} to {i+batch_size}...")
        results = analyzer.analyze_batch(batch)
        if results:
            final_results.extend(results)
        time.sleep(1) # Rate limit courtesy

    # 4. Serialize Output
    output_path = f"{config['output']['directory']}/{config['output']['filename']}"
    with open(output_path, 'w') as f:
        json.dump(final_results, f, indent=2)

    logger.info(f"Analysis complete. Written to {output_path}")

if __name__ == "__main__":
    main()
