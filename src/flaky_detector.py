import time
import json
import yaml
import logging
import argparse
import pandas as pd
from datetime import datetime, timedelta
from prometheus_api_client import PrometheusConnect
from google import genai

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _normalize_data(raw_data, start, end, step):
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
    def __init__(self, prom_url):
        self.prom = PrometheusConnect(url=prom_url, disable_ssl=True)

    def get_all_jobs(self):
        """Discovers all job names that have failure age metrics."""
        logger.info("Discovering jobs from Prometheus...")
        # We query for the metric name to find all unique 'jobname' labels
        # We look back 1 hour just to see what's active/exists
        try:
            # Querying the series API or a simple instant query
            query = 'count by (jobname) (max_over_time(jenkins_build_test_case_failure_age[30d]))'
            result = self.prom.custom_query(query=query)
            jobs = [x['metric']['jobname'] for x in result if 'jobname' in x['metric']]
            logger.info(f"Found {len(jobs)} jobs: {jobs}")
            return jobs
        except Exception as e:
            logger.error(f"Failed to discover jobs: {e}")
            return []

    def fetch_history(self, job_name, days, step):
        logger.info(f"Fetching metrics for job: {job_name}...")
        start_time = datetime.now() - timedelta(days=days)
        end_time = datetime.now()

        query = f'jenkins_build_test_case_failure_age{{jobname="{job_name}", status=~"FAILED|REGRESSION"}}'

        try:
            result = self.prom.custom_query_range(
                query=query, start_time=start_time, end_time=end_time, step=step
            )
        except Exception as e:
            logger.error(f"Prometheus Query Failed for {job_name}: {e}")
            return {}

        if not result:
            return {}

        return _normalize_data(result, start_time, end_time, step)


def _count_transitions(lst):
    flips = 0
    for i in range(1, len(lst)):
        if (lst[i-1] > 0) != (lst[i] > 0): flips += 1
    return flips


def _check_rules(history):
    # --- METRICS CALCULATION ---
    current_status = history[-1]      # 0 = Pass, >0 = Fail
    max_streak = max(history)         # Longest failure block
    transitions = _count_transitions(history)

    total_points = len(history)
    failure_points = len([x for x in history if x > 0])
    failure_rate = failure_points / total_points if total_points > 0 else 0

    # Calculate Recency (How many steps since last failure)
    # Iterate backwards to find the first non-zero
    steps_since_fail = 0
    if current_status == 0 and failure_rate > 0:
        for i, val in enumerate(reversed(history)):
            if val > 0:
                steps_since_fail = i
                break

    # Thresholds
    FIXED_THRESHOLD = 48 # If it hasn't failed in 48 steps (hours), consider it Fixed

    # --- RULE 1: ENVIRONMENTAL / DEAD ---
    if failure_rate == 1.0:
        return {"pattern": "ENVIRONMENTAL", "score": 0.0, "reason": "INFRA: Test has 100% failure rate."}

    # --- RULE 2: FLAKY PATTERNS ---

    # 2a. HIGH OSCILLATION (The "Yo-Yo" Effect)
    if transitions >= 3:
        return {"pattern": "FLAKY", "score": 1.0, "reason": f"OSCILLATION: Flipped state {transitions} times."}

    # 2b. SPORADIC / INTERMITTENT
    # Covers tests with low failure rate (0.1% to 30%)
    if current_status == 0 and failure_rate > 0 and failure_rate < 0.3:
        if steps_since_fail > FIXED_THRESHOLD:
            return {
                "pattern": "FIXED",
                "score": 0.0,
                "reason": f"HEALED: Sporadic failures (rate {failure_rate:.1%}) but passed last {steps_since_fail} runs."
            }
        else:
            return {
                "pattern": "FLAKY",
                "score": 0.95,
                "reason": f"SPORADIC: Low failure rate ({failure_rate:.1%}) but unstable."
            }

    # 2c. CLUSTER FLAKE
    if current_status == 0 and max_streak <= 6:
        if steps_since_fail > FIXED_THRESHOLD:
            return {
                "pattern": "FIXED",
                "score": 0.0,
                "reason": f"HEALED: Had short failure bursts but passed last {steps_since_fail} runs."
            }
        else:
            return {
                "pattern": "FLAKY",
                "score": 0.9,
                "reason": f"CLUSTER: Failed {max_streak} times then recovered."
            }

    # --- RULE 3: REGRESSION PATTERNS ---

    # 3a. FIXED REGRESSION (Long burst)
    if current_status == 0 and max_streak > 6:
        return {"pattern": "FIXED", "score": 0.0, "reason": f"FIXED: Was broken for {max_streak} builds, now fixed."}

    # 3b. ACTIVE REGRESSION (Failing now)
    if current_status >= 3:
        return {"pattern": "REGRESSION", "score": 0.05, "reason": f"ACTIVE: Broken for {current_status} consecutive builds."}

    # --- FALLBACK ---
    return {"pattern": "AMBIGUOUS", "score": 0.6, "reason": "Suspicious failure pattern."}


def _add_result(list_ref, job_name, test_id, analysis):
    parts = test_id.split("::")
    list_ref.append({
        "job_name": job_name, # Added field
        "test_suite": parts[0],
        "test_case": parts[1] if len(parts) > 1 else "Unknown",
        "flakiness_score": analysis['score'],
        "failure_pattern": analysis['pattern'],
        "reasoning": analysis['reason']
    })


class HybridAnalyzer:
    def __init__(self, api_key, model_name):
        self.client = genai.Client(api_key=api_key)
        self.model_name = model_name

    def analyze_all(self, job_name, test_histories):
        """Analyzes a specific job's history."""
        results = []
        ambiguous_cases = {}

        for test_id, history in test_histories.items():
            if sum(history) == 0: continue

            rule_result = _check_rules(history)

            if rule_result['pattern'] != 'AMBIGUOUS':
                _add_result(results, job_name, test_id, rule_result)
            else:
                ambiguous_cases[test_id] = history

        # Fallback for ambiguous
        if ambiguous_cases:
            for tid, hist in ambiguous_cases.items():
                _add_result(results, job_name, tid, {
                    'pattern': 'UNKNOWN', 'score': 0.5, 'reason': f"Complex pattern: {hist[-5:]}"
                })

        return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="/etc/monitoring/flaky_config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    harvester = MetricHarvester(config['prometheus']['url'])
    analyzer = HybridAnalyzer(config.get('gemini', {}).get('api_key'), config.get('gemini', {}).get('model'))

    # 1. Discover Jobs
    all_jobs = harvester.get_all_jobs()

    # Filter if config has specific include list (Optional feature, implementing simple filter)
    target_job_config = config['prometheus'].get('job_name')
    if target_job_config and target_job_config != "all":
        all_jobs = [target_job_config]

    full_report = []

    # 2. Process each job
    for job in all_jobs:
        histories = harvester.fetch_history(
            job,
            config['prometheus']['lookback_days'],
            config['prometheus']['step_seconds']
        )

        job_results = analyzer.analyze_all(job, histories)
        full_report.extend(job_results)

    # 3. Save Master Report
    output_path = f"{config['output']['directory']}/{config['output']['filename']}"
    with open(output_path, 'w') as f:
        json.dump(full_report, f, indent=2)

    logger.info(f"Analysis complete. Processed {len(all_jobs)} jobs. Total issues: {len(full_report)}")

if __name__ == "__main__":
    main()
