---
name: dbriks_cmd
description: Perform Databricks functionality using provided scripts. Use this skill when the user wants to interact with Databricks clusters, jobs, workspaces, or run Databricks CLI commands. This skill wraps Databricks operations and returns clean JSON output.
argument-hint: '[profile] [command]'
---

# Databricks Command Skill

Execute Databricks operations through a clean CLI that returns JSON metadata and structured output.

## Design Principles

| Principle | Meaning |
|---|---|
| **Clean context** | Claude sees only JSON metadata and structured output — never raw CLI noise |
| **Structured output** | All operations return JSON for easy parsing and integration |
| **Error handling** | Clear error messages and status codes for debugging |
| **Extensible** | Easy to add new Databricks operations and scripts |

## Requirements

Before using this skill, install the required Databricks SDK:

```bash
pip install databricks-sdk
```

You also need valid Databricks credentials configured in `~/.databrickscfg`:

```ini
[Profile_name]
host = https://your-workspace.databricks.com
```

## Available Commands

- **list-clusters** — List all Databricks clusters
- **list-jobs** — List all Databricks jobs
- **get-cluster** — Get details about a specific cluster
- **run-job** — Execute a specific job
- **get-job-status** — Check job execution status
- **list-tables** — List tables in Unity Catalog
- **workspace-info** — Get workspace information

## When Invoked

Arguments: `$0` = Databricks CLI profile name from `~/.databrickscfg`, `$1` = command name.

1. **Validate the command** — check if it's a supported operation
2. **Execute the command** — run the appropriate Python script
3. **Parse output** — return structured JSON response
4. **Handle errors** — provide clear error messages if the command fails

## Examples

```bash
# List all clusters
python3 dbriks_cmd.py default list-clusters

# Get details for a specific cluster
python3 dbriks_cmd.py default get-cluster abc123

# List all jobs
python3 dbriks_cmd.py default list-jobs

# Get job details
python3 dbriks_cmd.py default get-job 456

# Get workspace information
python3 dbriks_cmd.py default workspace-info

# List tables in a catalog
python3 dbriks_cmd.py default list-tables hive_metastore

# Run a job
python3 dbriks_cmd.py default run-job 789

# Get job run status
python3 dbriks_cmd.py default get-job-status 999
```

## Output Format

All commands return structured JSON:

**Success Response:**
```json
{
  "success": true,
  "operation": "list_clusters",
  "profile": "default",
  "data": [
    {
      "cluster_id": "cluster-123",
      "cluster_name": "my-cluster",
      "state": "RUNNING",
      "spark_version": "13.3.x-scala2.12"
    }
  ],
  "timestamp": "2024-02-27T14:30:00.000000Z"
}
```

**Error Response:**
```json
{
  "success": false,
  "operation": "list_clusters",
  "profile": "default",
  "error": "Failed to authenticate with profile 'default'",
  "timestamp": "2024-02-27T14:30:00.000000Z"
}
```
