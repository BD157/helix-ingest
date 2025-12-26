

# Development Environment Contract

This document defines the supported development environment for the Helix Ingest project.

## Supported Runtime

Helix Ingest is developed and validated under the following environment:

- OS: WSL Ubuntu (primary and recommended)
- Java: 17 (Temurin)
- Scala: 2.12.18
- sbt: 1.9.x
- Spark: 3.5.x (local mode)

If Helix Ingest runs correctly under this environment, it is considered **working as designed**.

## Unsupported / Best-Effort Environments

The following environments are not guaranteed to work and are not actively supported:

- Native Windows filesystem execution
- Hadoop `winutils.exe`-dependent setups
- Mixed Windows + WSL execution
- Docker-based execution (future consideration)

Issues encountered exclusively in these environments are considered **out of scope** for Step 1 and Step 2.

## Rationale

Helix Ingest interacts heavily with filesystem and Hadoop semantics.  
Windows-native environments introduce behavior that diverges from typical production Linux runtimes.

WSL provides a closer approximation of real-world Spark execution environments while minimizing local setup friction.

## Scope Control

This project prioritizes:
- Deterministic execution
- Reproducibility
- Clear failure boundaries

Environment-specific issues outside the supported contract are intentionally excluded to prevent setup churn and ambiguous failure states.
