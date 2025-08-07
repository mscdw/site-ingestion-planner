# Site Ingestion Planner

## Overview

The Site Ingestion Planner is a command-line utility designed to connect to a target Avigilon site and scan for events within a specified date range. It provides a detailed report of event counts, grouped by type, along with monthly volume estimates and an assessment of each event type's value for facial recognition.

This tool is invaluable for:
-   Understanding the data volume and variety a site produces.
-   Planning for data ingestion and storage costs.
-   Making informed decisions about which event types are most valuable to process.

This utility is standalone and does not connect to any central database or other internal APIs.

## Features

-   **Secure Authentication**: Connects to Avigilon sites using the standard challenge-response mechanism.
-   **Comprehensive Scanning**: Discovers and scans all servers within a site for both generic and appearance events.
-   **Targeted Filtering**: Allows you to scan for specific event types to speed up analysis on busy sites.
-   **Detailed Reporting**: Generates a clear, easy-to-read report with:
    -   Total event counts for the scanned period.
    -   Projected monthly event volume.
    -   A "Facial Recognition Yield" estimate for each event type based on historical data.
    -   The actual time range of events found.
-   **Secure Logging**: Automatically redacts sensitive session tokens from all log output.

## Prerequisites

-   Python 3.8+
-   An active Avigilon site.
-   Avigilon API credentials (`nonce` and `key`).

## Installation

1.  Navigate to the `utils` directory.
2.  Install the required Python package using the `requirements.txt` file:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

The script is run from the command line with several required arguments. For security, you can omit `--password` and `--key` to be prompted for them securely.

### Command-Line Arguments

| Argument          | Required | Description                                                                                                                                                             |
| ----------------- | :------: | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--site-url`      |   Yes    | The base URL of the Avigilon site (e.g., `https://192.168.1.100`).                                                                                                        |
| `--username`      |   Yes    | Username for authenticating with the site.                                                                                                                              |
| `--password`      |    No    | Password for the site. If omitted, you will be prompted.                                                                                                                |
| `--nonce`         |   Yes    | The user-specific nonce for API authentication.                                                                                                                         |
| `--key`           |    No    | The user-specific key for API authentication. If omitted, you will be prompted.                                                                                         |
| `--start-date`    |   Yes    | The start date for the event scan, in `YYYY-MM-DD` format.                                                                                                              |
| `--end-date`      |   Yes    | The end date for the event scan, in `YYYY-MM-DD` format.                                                                                                                |
| `--target-events` |    No    | A comma-separated list of event types to scan for (e.g., `DEVICE_FACET_START,APPEARANCE_EVENT`). If omitted, all discoverable event types are counted, which may be slow. |

### Example Command

```bash
python site_ingestion_planner.py \
    --site-url "https://10.89.26.169:8443" \
    --username "service-acct" \
    --nonce "your-api-nonce" \
    --start-date "2025-08-01" \
    --end-date "2025-08-07"
```

## Example Report

The tool produces a detailed summary report directly in your console upon completion.

```
====================================================================================================
           SITE INGESTION PLANNER - FINAL REPORT
====================================================================================================
Site URL:                  https://10.89.26.169:8443
Site Name:                 Main Campus Site
Servers Scanned:           2 (VH1DUCSC220SRV1, VH1DUCSC220SRV2)
Time Range Analyzed:       2025-08-01T00:00:00Z to 2025-08-08T00:00:00Z (7 day scan window)
Total Scan Time:           125.43 seconds
Total API Events Fetched:  1,234,567 events
----------------------------------------------------------------------------------------------------
EVENT COUNT & FACIAL RECOGNITION YIELD ESTIMATES
(Only event types known to the planner are shown. Yield is an estimate of value for finding faces.)
Event Type                                 Count in Scan    Monthly Est.   Yield (Face Recognition)
----------------------------------------------------------------------------------------------------
APPEARANCE EVENTS (/appearance/search)             5,054         ~21,848   Moderate (~19.6%)
  - Male Appearances                               2,530         ~10,939
  - Female Appearances                             2,524         ~10,909
----------------------------------------------------------------------------------------------------
GENERIC EVENTS (/events/search)                  320,458       ~1,385,502
  - DEVICE_MOTION_START                          155,823        ~673,631   Low (~3.2%)
  - DEVICE_MOTION_STOP                           149,310        ~645,489   Low (~2.8%)
  - DEVICE_ANALYTICS_START                         4,407         ~19,051   Moderate (~18.7%)
  - DEVICE_FACET_START                             1,514          ~6,545   Excellent (~76.0%)
  ...
----------------------------------------------------------------------------------------------------
GRAND TOTAL (Known Types)                        325,512       ~1,407,350
====================================================================================================
```

## Obtaining API Credentials

This tool requires an Avigilon User Nonce and User Key for authentication. If your organization does not have these, they can be requested from Avigilon's developer portal:

**https://support.avigilon.com/flow/API_Credentials_Dev_Tools_Request**
