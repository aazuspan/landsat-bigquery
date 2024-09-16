import sys
from google.cloud import bigquery


def estimate_query_cost(*, client: bigquery.Client, query: str, cost_per_tb: float = 6) -> float:
    config = bigquery.QueryJobConfig(
        dry_run=True,
        use_query_cache=False,
    )

    dry_run = client.query(query, job_config=config)
    return (dry_run.total_bytes_processed / 1024 ** 4) * cost_per_tb


def run_query(
        *, 
        client: bigquery.Client, 
        query: str, 
        config: bigquery.QueryJobConfig=None, 
        warning_threshold: float=0.001
    ) -> bigquery.QueryJob:
    estimated_cost = estimate_query_cost(client=client, query=query, cost_per_tb=6.25)
    if estimated_cost > warning_threshold and input(
        f"Estimated query cost: ${estimated_cost:.4f}. Continue? (y/N): "
    ).lower() != "y":
        print("Query cancelled.")
        sys.exit(0)
    
    return client.query(query, job_config=config)