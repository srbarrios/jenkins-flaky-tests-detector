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


def _normalize_data(raw_data, start, end, step):
    # Time alignment logic (Floor/Ceil)
    pd_start = pd.Timestamp(start).floor(f'{step}s')
    pd_end = pd.Timestamp(end).ceil(f'{step}s')
    full_time_index = pd.date_range(start=pd_start, end=pd_end, freq=f'{step}s')

    normalized_history = {}
    for entry in raw_data:
        metric = entry['metric']
        full_name = f"{metric.get('suite', 'unknown')}::{metric.get('case', 'unknown')}"
        try:
            df = pd.DataFrame(entry['values'], columns=['ds', 'y'])
            df['ds'] = pd.to_datetime(df['ds'], unit='s')
            df['y'] = pd.to_numeric(df['y'])
            df = df.set_index('ds')
            df_resampled = df.resample(f'{step}s').max()
            df_reindexed = df_resampled.reindex(full_time_index).fillna(0)
            normalized_history[full_name] = df_reindexed['y'].astype(int).tolist()
        except Exception:
            continue
    return normalized_history


class MetricHarvester:
    """Handles interaction with Prometheus and Data Normalization."""
    def __init__(self, prom_url):
        self.prom = PrometheusConnect(url=prom_url, disable_ssl=True)

    def fetch_history(self, job_name, days, step):
        logger.info(f"Fetching metrics for job: {job_name} over last {days} days...")
        start_time = datetime.now() - timedelta(days=days)
        end_time = datetime.now()

        query = f'jenkins_build_test_case_failure_age{{jobname="{job_name}", status=~"FAILED|REGRESSION"}}'

        try:
            result = self.prom.custom_query_range(
                query=query, start_time=start_time, end_time=end_time, step=step
            )
        except Exception as e:
            logger.error(f"Prometheus Query Failed: {e}")
            return {}

        if not result:
            logger.warning("No metrics found.")
            return {}

        return _normalize_data(result, start_time, end_time, step)


def _count_transitions(lst):
    flips = 0
    for i in range(1, len(lst)):
        if (lst[i-1] > 0) != (lst[i] > 0): flips += 1
    return flips


def _add_result(list_ref, test_id, analysis):
    parts = test_id.split("::")
    list_ref.append({
        "test_suite": parts[0],
        "test_case": parts[1] if len(parts) > 1 else "Unknown",
        "flakiness_score": analysis['score'],
        "failure_pattern": analysis['pattern'],
        "reasoning": analysis['reason']
    })


def _check_rules(history):
    # --- METRICS CALCULATION ---
    current_status = history[-1]      # 0 = Pass, >0 = Fail
    max_streak = max(history)         # Longest failure block
    transitions = _count_transitions(history)

    total_points = len(history)
    failure_points = len([x for x in history if x > 0])
    failure_rate = failure_points / total_points if total_points > 0 else 0

    # --- RULE 1: ENVIRONMENTAL / DEAD ---
    # If it passed 0% of the time, it's not flaky, it's broken infrastructure.
    if failure_rate == 1.0:
        return {"pattern": "ENVIRONMENTAL", "score": 0.0, "reason": "Test has 100% failure rate."}

    # --- RULE 2: FLAKY PATTERNS ---

    # 2a. HIGH OSCILLATION (The "Yo-Yo" Effect)
    # It flips between pass/fail frequently (>= 3 times).
    if transitions >= 3:
        return {"pattern": "FLAKY", "score": 1.0, "reason": f"OSCILLATION: Flipped state {transitions} times."}

    # 2b. SPORADIC / INTERMITTENT
    # It fails rarely (rate < 30%) but it DOES fail, and it IS passing now.
    # This catches tests that fail once (streak=1) but recover immediately.
    if current_status == 0 and failure_rate > 0 and failure_rate < 0.3:
        return {"pattern": "FLAKY", "score": 0.7 + failure_rate, "reason": f"SPORADIC: Low failure rate ({failure_rate:.1%}) but unstable."}

    # 2c. CLUSTER FLAKE
    # It failed for a block (up to 3 runs) then recovered.
    if current_status == 0 and max_streak <= 3:
        return {"pattern": "FLAKY", "score": 0.9, "reason": f"CLUSTER: Failed {max_streak} times then recovered."}

    # --- RULE 3: REGRESSION PATTERNS ---

    # 3a. FIXED REGRESSION
    # It failed for a LONG time (>6) and is now fixed (0).
    if current_status == 0 and max_streak > 6:
        return {"pattern": "FIXED", "score": 0.1, "reason": f"Was broken for {max_streak} builds, now fixed."}

    # 3b. ACTIVE REGRESSION
    # It is failing RIGHT NOW and has been for a while (>6).
    # We lower the threshold to 6 because integration tests shouldn't fail 6 times in a row.
    if current_status >= 6:
        return {"pattern": "REGRESSION", "score": 0.05, "reason": f"Broken for {current_status} consecutive builds."}

    # --- FALLBACK ---
    # E.g. Current status is 1 or 2 (failing recently but not long enough to be active regression)
    # Likely a new flake starting.
    return {"pattern": "AMBIGUOUS", "score": 0.6, "reason": "Suspicious short failure streak."}

class HybridAnalyzer:
    """
    Applies strict rules first. If the pattern is ambiguous, delegates to Gemini.
    """
    def __init__(self, api_key, model_name):
        self.client = genai.Client(api_key=api_key)
        self.model_name = model_name

    def analyze_all(self, test_histories):
        final_results = []
        ambiguous_cases = {}

        # PHASE 1: Algorithmic Filter
        logger.info("Phase 1: Running Heuristic Rules...")
        for test_id, history in test_histories.items():
            if sum(history) == 0: continue # Skip all pass

            rule_result = _check_rules(history)

            if rule_result['pattern'] != 'AMBIGUOUS':
                # We are confident, add to final results directly
                _add_result(final_results, test_id, rule_result)
            else:
                # We are not sure, send to AI
                ambiguous_cases[test_id] = history

        # PHASE 2: AI Analysis for Ambiguous Cases
        if ambiguous_cases:
            logger.info(f"Phase 2: Sending {len(ambiguous_cases)} ambiguous cases to Gemini...")
            ai_results = self._ask_gemini(ambiguous_cases)
            final_results.extend(ai_results)
        else:
            logger.info("Phase 2: Skipped (No ambiguous cases found).")

        return final_results

    def _ask_gemini(self, cases):
        results = []
        batch_size = 20
        items = list(cases.items())

        response_schema = types.Schema(
            type=types.Type.ARRAY,
            items=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "test_id": types.Schema(type=types.Type.STRING),
                    "pattern": types.Schema(type=types.Type.STRING, enum=["FLAKY", "REGRESSION", "ENVIRONMENTAL"]),
                    "reason": types.Schema(type=types.Type.STRING),
                    "confidence": types.Schema(type=types.Type.NUMBER)
                },
                required=["test_id", "pattern", "reason"]
            )
        )

        for i in range(0, len(items), batch_size):
            batch = dict(items[i:i+batch_size])
            prompt = f"""
            Analyze these ambiguous test failure patterns (0=Pass, N=Failure Age).
            Determine if they are FLAKY (random/short-lived) or REGRESSION (systematic).
            
            Strictly output JSON.
            Data: {json.dumps(batch)}
            """

            try:
                resp = self.client.models.generate_content(
                    model=self.model_name,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=response_schema
                    )
                )
                ai_data = json.loads(resp.text)

                # Map back to internal format
                for item in ai_data:
                    _add_result(results, item['test_id'], {
                        'pattern': item['pattern'],
                        'score': 0.8 if item['pattern'] == 'FLAKY' else 0.2,
                        'reason': f"AI: {item['reason']}"
                    })
            except Exception as e:
                logger.error(f"Gemini Batch Error: {e}")
                # Treat failed AI calls as Unknown Regressions to be safe
                for tid in batch.keys():
                    _add_result(results, tid, {'pattern': 'UNKNOWN', 'score': 0.5, 'reason': 'AI Analysis Failed'})

            time.sleep(1)

        return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="/etc/monitoring/flaky_config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    harvester = MetricHarvester(config['prometheus']['url'])
    histories = harvester.fetch_history(
        config['prometheus']['job_name'],
        config['prometheus']['lookback_days'],
        config['prometheus']['step_seconds']
    )

    # Initialize Hybrid Analyzer
    analyzer = HybridAnalyzer(config['gemini']['api_key'], config['gemini']['model'])
    results = analyzer.analyze_all(histories)

    output_path = f"{config['output']['directory']}/{config['output']['filename']}"
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)

    logger.info(f"Analysis complete. Written to {output_path}")

if __name__ == "__main__":
    main()
