#!/usr/bin/env python3
"""
Databricks API Caller
Initialize WorkspaceClient and provide methods for calling Databricks workspace APIs
"""

import sys
import json
from typing import Any, Dict, List, Optional
from datetime import datetime

try:
    from databricks.sdk import WorkspaceClient
except ImportError:
    print(json.dumps({
        "success": False,
        "error": "databricks-sdk not installed. Install with: pip install databricks-sdk"
    }))
    sys.exit(1)


class DatabricksAPIClient:
    """Wrapper for Databricks WorkspaceClient with structured output"""

    def __init__(self, profile: str):
        """
        Initialize WorkspaceClient with specified profile

        Args:
            profile: Profile name from ~/.databrickscfg
        """
        try:
            self.profile = profile
            self.client = WorkspaceClient(profile=profile)
        except Exception as e:
            self._handle_error(f"Failed to initialize WorkspaceClient: {str(e)}")

    def list_clusters(self) -> Dict[str, Any]:
        """List all clusters"""
        try:
            clusters = []
            for cluster in self.client.clusters.list():
                clusters.append({
                    "cluster_id": cluster.cluster_id,
                    "cluster_name": cluster.cluster_name,
                    "state": cluster.state,
                    "spark_version": getattr(cluster, "spark_version", None)
                })
            return self._success_response("list_clusters", clusters)
        except Exception as e:
            return self._error_response("list_clusters", str(e))

    def get_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """Get cluster details"""
        try:
            cluster = self.client.clusters.get(cluster_id)
            return self._success_response("get_cluster", {
                "cluster_id": cluster.cluster_id,
                "cluster_name": cluster.cluster_name,
                "state": cluster.state,
                "spark_version": getattr(cluster, "spark_version", None),
                "node_type_id": getattr(cluster, "node_type_id", None),
                "num_workers": getattr(cluster, "num_workers", None)
            })
        except Exception as e:
            return self._error_response("get_cluster", str(e))

    def list_jobs(self) -> Dict[str, Any]:
        """List all jobs"""
        try:
            jobs = []
            for job in self.client.jobs.list():
                jobs.append({
                    "job_id": job.job_id,
                    "settings": job.settings
                })
            return self._success_response("list_jobs", jobs)
        except Exception as e:
            return self._error_response("list_jobs", str(e))

    def get_job(self, job_id: int) -> Dict[str, Any]:
        """Get job details"""
        try:
            job = self.client.jobs.get(job_id)
            return self._success_response("get_job", {
                "job_id": job.job_id,
                "settings": job.settings
            })
        except Exception as e:
            return self._error_response("get_job", str(e))

    def list_workspaces(self) -> Dict[str, Any]:
        """List workspace information"""
        try:
            workspace = self.client.workspace.get_status("/")
            return self._success_response("workspace_info", {
                "path": workspace.path,
                "object_type": workspace.object_type
            })
        except Exception as e:
            return self._error_response("workspace_info", str(e))

    def list_tables(self, catalog: str = "hive_metastore") -> Dict[str, Any]:
        """List tables in a catalog"""
        try:
            tables = []
            for schema in self.client.schemas.list(catalog_name=catalog):
                for table in self.client.tables.list(
                    catalog_name=catalog,
                    schema_name=schema.name
                ):
                    tables.append({
                        "catalog": catalog,
                        "schema": schema.name,
                        "table": table.name,
                        "table_type": table.table_type
                    })
            return self._success_response("list_tables", tables)
        except Exception as e:
            return self._error_response("list_tables", str(e))

    def run_job(self, job_id: int) -> Dict[str, Any]:
        """Run a job and return run_id"""
        try:
            run = self.client.jobs.run_now(job_id)
            return self._success_response("run_job", {
                "run_id": run.run_id,
                "job_id": job_id
            })
        except Exception as e:
            return self._error_response("run_job", str(e))

    def get_job_status(self, run_id: int) -> Dict[str, Any]:
        """Get job run status"""
        try:
            run = self.client.jobs.get_run(run_id)
            return self._success_response("get_job_status", {
                "run_id": run.run_id,
                "state": run.state,
                "start_time": run.start_time,
                "end_time": getattr(run, "end_time", None),
                "state_message": run.state_message
            })
        except Exception as e:
            return self._error_response("get_job_status", str(e))

    def _success_response(self, operation: str, data: Any = None) -> Dict[str, Any]:
        """Create a success response"""
        return {
            "success": True,
            "operation": operation,
            "profile": self.profile,
            "data": data,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

    def _error_response(self, operation: str, error: str) -> Dict[str, Any]:
        """Create an error response"""
        return {
            "success": False,
            "operation": operation,
            "profile": self.profile,
            "error": error,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

    def _handle_error(self, error: str):
        """Handle initialization errors"""
        print(json.dumps(self._error_response("init", error)))
        sys.exit(1)


def main():
    """Main entry point"""
    if len(sys.argv) < 3:
        print(json.dumps({
            "success": False,
            "error": "Usage: dbrik_api_caller.py <profile> <operation> [args...]",
            "available_operations": [
                "list_clusters",
                "get_cluster <cluster_id>",
                "list_jobs",
                "get_job <job_id>",
                "workspace_info",
                "list_tables [catalog]",
                "run_job <job_id>",
                "get_job_status <run_id>"
            ]
        }))
        sys.exit(1)

    profile = sys.argv[1]
    operation = sys.argv[2]
    args = sys.argv[3:] if len(sys.argv) > 3 else []

    # Initialize client
    client = DatabricksAPIClient(profile)

    # Route operations
    if operation == "list_clusters":
        result = client.list_clusters()
    elif operation == "get_cluster":
        if not args:
            result = {"success": False, "error": "get_cluster requires cluster_id argument"}
        else:
            result = client.get_cluster(args[0])
    elif operation == "list_jobs":
        result = client.list_jobs()
    elif operation == "get_job":
        if not args:
            result = {"success": False, "error": "get_job requires job_id argument"}
        else:
            result = client.get_job(int(args[0]))
    elif operation == "workspace_info":
        result = client.list_workspaces()
    elif operation == "list_tables":
        catalog = args[0] if args else "hive_metastore"
        result = client.list_tables(catalog)
    elif operation == "run_job":
        if not args:
            result = {"success": False, "error": "run_job requires job_id argument"}
        else:
            result = client.run_job(int(args[0]))
    elif operation == "get_job_status":
        if not args:
            result = {"success": False, "error": "get_job_status requires run_id argument"}
        else:
            result = client.get_job_status(int(args[0]))
    else:
        result = {"success": False, "error": f"Unknown operation: {operation}"}

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
