#!/usr/bin/env python3
"""
Databricks Command Skill
Wrapper that uses DatabricksAPIClient from dbrik_api_caller.py
"""

import sys
import json
from typing import Any, Dict

# Import the API client from dbrik_api_caller
from dbrik_api_caller import DatabricksAPIClient


def main():
    """Main entry point for the skill"""
    if len(sys.argv) < 3:
        print(json.dumps({
            "success": False,
            "error": "Usage: dbriks_cmd <profile> <command> [args...]",
            "available_commands": [
                "list-clusters",
                "get-cluster <cluster_id>",
                "list-jobs",
                "get-job <job_id>",
                "workspace-info",
                "list-tables [catalog]",
                "run-job <job_id>",
                "get-job-status <run_id>"
            ]
        }))
        sys.exit(1)

    profile = sys.argv[1].strip()
    command = sys.argv[2].strip()
    args = sys.argv[3:] if len(sys.argv) > 3 else []

    # Initialize the API client
    print(profile, command)
    client = DatabricksAPIClient(profile)
    print(client)
    # Route commands to appropriate API client methods
    result = None

    if command == "list-clusters":
        result = client.list_clusters()
    elif command == "get-cluster":
        if not args:
            result = {"success": False, "error": "get-cluster requires cluster_id argument"}
        else:
            result = client.get_cluster(args[0])
    elif command == "list-jobs":
        result = client.list_jobs()
    elif command == "get-job":
        if not args:
            result = {"success": False, "error": "get-job requires job_id argument"}
        else:
            result = client.get_job(int(args[0]))
    elif command == "workspace-info":
        result = client.list_workspaces()
    elif command == "list-tables":
        catalog = args[0] if args else "hive_metastore"
        result = client.list_tables(catalog)
    elif command == "run-job":
        if not args:
            result = {"success": False, "error": "run-job requires job_id argument"}
        else:
            result = client.run_job(int(args[0]))
    elif command == "get-job-status":
        if not args:
            result = {"success": False, "error": "get-job-status requires run_id argument"}
        else:
            result = client.get_job_status(int(args[0]))
    else:
        result = {
            "success": False,
            "error": f"Unknown command: {command}",
            "available_commands": [
                "list-clusters",
                "get-cluster <cluster_id>",
                "list-jobs",
                "get-job <job_id>",
                "workspace-info",
                "list-tables [catalog]",
                "run-job <job_id>",
                "get-job-status <run_id>"
            ]
        }

    # Output the result as JSON
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
