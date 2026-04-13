# dbriks_cmd - Databricks Command Skill

A Claude Code skill for performing Databricks functionality using provided scripts.

## Structure

```
.
├── .claude-plugin/
│   └── plugin.json          # Plugin configuration
├── skills/
│   └── dbriks_cmd/
│       ├── SKILL.md         # Skill documentation
│       ├── dbriks_cmd.py    # Main implementation
│       └── LICENSE.txt      # License
├── .gitignore
└── README.md
```

## Overview

This skill enables Claude Code to interact with Databricks through a clean CLI interface that returns structured JSON output.

### Supported Commands

- `list-clusters` — List all Databricks clusters
- `list-jobs` — List all Databricks jobs
- `get-cluster` — Get details about a specific cluster
- `run-job` — Execute a specific job

## Usage

```bash
dbriks_cmd <profile> <command> [args...]
```

### Example

```bash
# List all clusters
dbriks_cmd default list-clusters

# Run a job
dbriks_cmd default run-job 123

# Get cluster details
dbriks_cmd default get-cluster abc123
```

## Output

All commands return JSON:

```json
{
  "success": true,
  "command": "list-clusters",
  "data": [...],
  "timestamp": "2024-01-01T00:00:00Z"
}
```

## Requirements

- Python 3.6+
- Databricks CLI (`pip install databricks-cli`)
- Valid Databricks credentials in `~/.databrickscfg`

## License

MIT
