"""
Site Ingestion Planner Utility

This command-line tool connects to a target Avigilon site to scan for events
within a specified date range. It counts the events found, grouped by type,
and provides a summary report. This is useful for understanding the volume and
variety of data a site produces before configuring it for live ingestion.

This utility is standalone and does not connect to any central database or other internal APIs.
It reuses event fetching logic from the `event_ingestion` worker.

Prerequisites:
- Python 3.8+
- httpx (`pip install httpx`)

Note on API Credentials:
This tool requires an Avigilon User Nonce and User Key for authentication.
If you do not have these, they can be requested from Avigilon's developer portal:
https://support.avigilon.com/flow/API_Credentials_Dev_Tools_Request

Usage:
python tools/site_ingestion_planner.py \
    --site-url "https://your-avigilon-site.com" \
    --username "your_user" \
    --nonce "your_nonce" \
    --start-date "2023-10-01" \
    --end-date "2023-10-31" \
    --target-events "DEVICE_CLASSIFIED_OBJECT_MOTION_START,APPEARANCE_EVENT"

You will be prompted for your password and user key for security.
"""
import asyncio
import httpx
import argparse
import hashlib
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, urlunparse
from collections import Counter
from getpass import getpass
import logging
import re

# --- Setup logging for the command-line tool ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ingestion-planner")

# --- Redact session tokens from httpx's default logging ---
class UrlRedactingFilter(logging.Filter):
    """A logging filter to redact sensitive session tokens from URLs."""
    def filter(self, record):
        # The default httpx log format is "HTTP Request: %s %s ..."
        # We modify the arguments passed to the logger, not the message string itself,
        # to avoid breaking the string formatting which causes a TypeError.
        if "HTTP Request:" in record.msg and record.args and len(record.args) > 1:
            # The URL is typically the second argument in the log record's args tuple.
            # It can be a string or an httpx.URL object, so we convert it to a string.
            url_arg_index = 1
            original_url = str(record.args[url_arg_index])
            redacted_url = re.sub(r'([?&]session=)[^&]+', r'\1REDACTED', original_url)
            # LogRecord.args is a tuple, which is immutable. We must create a new one.
            new_args = list(record.args)
            new_args[url_arg_index] = redacted_url
            record.args = tuple(new_args)
        return True

# Apply the filter to the httpx logger to prevent session tokens from being exposed.
httpx_logger = logging.getLogger("httpx")
httpx_logger.addFilter(UrlRedactingFilter())

# --- Constants ---
# These are based on common Avigilon API patterns but may need adjustment.
LOGIN_ENDPOINT = "/login"
SERVERS_ENDPOINT = "/servers"
SITES_ENDPOINT = "/sites"
EVENTS_SEARCH_ENDPOINT = "/events/search"
APPEARANCE_SEARCH_ENDPOINT = "/appearance/search-by-description"
GENERIC_EVENTS_PAGE_SIZE = 1000
APPEARANCE_PAGE_SIZE = 100  # Appearance search has a smaller limit
VERIFY_SSL = False  # Set to True for production environments with valid certs

# --- Constants for Appearance Search ---
FACET_GENDER = "GENDER"
TAG_MALE = "MALE"
TAG_FEMALE = "FEMALE"

# This guide provides estimates on how likely an event type is to result in a
# successful face match, based on historical lab data. This helps in planning
# which events are most valuable to ingest for facial recognition purposes.
EVENT_TYPE_PLANNING_GUIDE = {
    # Excellent Yield (> 50%)
    "DEVICE_FACET_START": {"success_rate": 76.02, "yield": "Excellent"},
    "DEVICE_FACE_MATCH_START": {"success_rate": 63.24, "yield": "Excellent"},
    # Good Yield (20% - 50%)
    "DEVICE_CLASSIFIED_OBJECT_ANOMALY_START": {"success_rate": 45.46, "yield": "Good"},
    "DEVICE_FACET_STOP": {"success_rate": 34.58, "yield": "Good"},
    "DEVICE_UNUSUAL_STARTED": {"success_rate": 25.79, "yield": "Good"},
    # Moderate Yield (10% - 20%)
    "APPEARANCE_EVENT": {"success_rate": 19.59, "yield": "Moderate"},
    "DEVICE_FACE_MATCH_STOP": {"success_rate": 19.40, "yield": "Moderate"},
    "DEVICE_ANALYTICS_START": {"success_rate": 18.72, "yield": "Moderate"},
    "DEVICE_CLASSIFIED_OBJECT_MOTION_START": {"success_rate": 18.53, "yield": "Moderate"},
    "DEVICE_UNUSUAL_STOPPED": {"success_rate": 16.88, "yield": "Moderate"},
    "DEVICE_CLASSIFIED_OBJECT_ANOMALY_STOP": {"success_rate": 15.64, "yield": "Moderate"},
    "DEVICE_ANALYTICS_STOP": {"success_rate": 11.38, "yield": "Moderate"},
    "DEVICE_CLASSIFIED_OBJECT_MOTION_STOP": {"success_rate": 11.25, "yield": "Moderate"},
    # Low Yield (1% - 10%)
    "DEVICE_MOTION_START": {"success_rate": 3.19, "yield": "Low"},
    "DEVICE_MOTION_STOP": {"success_rate": 2.75, "yield": "Low"},
    # Very Low Yield (< 1%)
    "DEVICE_RECORDING_STARTED": {"success_rate": 0.81, "yield": "Very Low"},
    "DEVICE_RECORDING_STOPPED": {"success_rate": 0.71, "yield": "Very Low"},
    # Events with unknown yield from this dataset
    "DEVICE_ANOMALY_START": {"success_rate": 0.0, "yield": "Unknown"},
    "DEVICE_ANOMALY_STOP": {"success_rate": 0.0, "yield": "Unknown"},
    "DEVICE_OBJECT_DESCRIPTION": {"success_rate": 0.0, "yield": "Unknown"},
    "DEVICE_PRESENCE_DWELL_STARTED": {"success_rate": 0.0, "yield": "Unknown"},
    "DEVICE_PRESENCE_DWELL_STOPPED": {"success_rate": 0.0, "yield": "Unknown"},
    "DEVICE_PRESENCE_STARTED": {"success_rate": 0.0, "yield": "Unknown"},
    "DEVICE_PRESENCE_STOPPED": {"success_rate": 0.0, "yield": "Unknown"},
    "DEVICE_TAMPERING": {"success_rate": 0.0, "yield": "Unknown"},
    "DEVICE_USER_DEFINED_ARBITRARY_EVENT_ERROR": {"success_rate": 0.0, "yield": "Unknown"},
    "DEVICE_USER_DEFINED_ARBITRARY_EVENT_STARTED": {"success_rate": 0.0, "yield": "Unknown"},
    "DEVICE_USER_DEFINED_ARBITRARY_EVENT_STOPPED": {"success_rate": 0.0, "yield": "Unknown"},
}


async def fetch_events_with_token_pagination(client: httpx.AsyncClient, session_token: str, base_url: str, server_id: str, from_time: str, to_time: str, limit: int):
    """
    Asynchronously fetches events from a source API using token-based pagination.
    This function is adapted from the event_ingestion worker.

    Yields:
        A list of event dictionaries per page.
    """
    url = f"{base_url.rstrip('/')}{EVENTS_SEARCH_ENDPOINT}"

    params = {
        "serverId": server_id,
        "session": session_token,
        "queryType": "TIME_RANGE",
        "from": from_time,
        "to": to_time,
        "limit": limit
    }
    page_num = 0

    while True:
        page_num += 1
        try:
            # Redact session token for logging to avoid exposing it in output
            log_params = params.copy()
            if "session" in log_params:
                log_params["session"] = "REDACTED"
            logger.debug(f"Fetching event page {page_num} from source API with params: {log_params}")
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            result_data = data.get("result", {})
            events = result_data.get("events", [])
            if events:
                first_event_ts = events[0].get("timestamp", "N/A")
                last_event_ts = events[-1].get("timestamp", "N/A")
                logger.info(f"Fetched event page {page_num} ({len(events)} events). Timestamp range: {first_event_ts} to {last_event_ts}")
                yield events

            token = result_data.get("token")
            if token:
                params = {
                    "session": session_token,
                    "queryType": "CONTINUE",
                    "token": token,
                }
            else:
                logger.info(f"No more pagination tokens found for server {server_id}. Fetched a total of {page_num} page(s).")
                break
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error on page {page_num} while fetching events: {e.response.status_code} - {e.response.text}")
            break
        except Exception as e:
            logger.error(f"Error fetching event page {page_num}: {e}", exc_info=True)
            break


async def fetch_appearances_for_planning(client: httpx.AsyncClient, session_token: str, base_url: str, query_descriptors: list, from_time: str, to_time: str, limit: int):
    """
    Asynchronously fetches appearance events from a source API using token-based pagination.
    This is adapted from the event_ingestion worker for planning purposes.

    Yields:
        A list of appearance event dictionaries per page.
    """
    url = f"{base_url.rstrip('/')}{APPEARANCE_SEARCH_ENDPOINT}"
    gender_tag = query_descriptors[0].get('tag', 'UNKNOWN')

    json_payload = {
        "session": session_token,
        "queryType": "TIME_RANGE",
        "queryDescriptors": query_descriptors,
        "from": from_time,
        "to": to_time,
        "limit": limit,
        "scanType": "FULL"
    }
    page_num = 0

    while True:
        page_num += 1
        try:
            # Redact session token for logging to avoid exposing it in output
            log_payload = json_payload.copy()
            if "session" in log_payload:
                log_payload["session"] = "REDACTED"
            logger.debug(f"Fetching appearance page {page_num} for {gender_tag} with payload: {log_payload}")
            response = await client.post(url, json=json_payload)
            response.raise_for_status()
            data = response.json()

            result_data = data.get("result", {})
            appearances = result_data.get("results", [])
            if appearances:
                first_event_ts = appearances[0].get("timestamp", "N/A")
                last_event_ts = appearances[-1].get("timestamp", "N/A")
                logger.info(f"Fetched appearance page {page_num} for {gender_tag} ({len(appearances)} events). Timestamp range: {first_event_ts} to {last_event_ts}")
                yield appearances

            token = result_data.get("token")
            if token:
                json_payload = {
                    "session": session_token,
                    "queryType": "CONTINUE",
                    "token": token,
                }
            else:
                logger.info(f"No more pagination tokens for {gender_tag} appearances. Fetched a total of {page_num} page(s).")
                break
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error on page {page_num} while fetching {gender_tag} appearances: {e.response.status_code} - {e.response.text}")
            break
        except Exception as e:
            logger.error(f"Error fetching {gender_tag} appearance page {page_num}: {e}", exc_info=True)
            break

async def authenticate_to_site(client: httpx.AsyncClient, site_url: str, username: str, password: str, nonce: str, key: str) -> str | None:
    """
    Authenticates to the Avigilon site to get a session token using the
    challenge-response mechanism.
    """
    login_url = f"{site_url.rstrip('/')}{LOGIN_ENDPOINT}"
    
    try:
        # Generate the required authorization token for the login request
        epoch = int(time.time())
        hash_input = f"{epoch}{key}".encode('utf-8')
        hash_output = hashlib.sha256(hash_input).hexdigest()
        auth_token = f"{nonce}:{epoch}:{hash_output}"

        payload = {
            "username": username,
            "password": password,
            "clientName": "Site-Ingestion-Planner", # A unique name for the planner
            "authorizationToken": auth_token
        }
        
        logger.info(f"Attempting to authenticate to {site_url}...")
        # The API expects a JSON payload
        response = await client.post(login_url, json=payload, headers={"content-type": "application/json"})
        response.raise_for_status()
        
        json_response = response.json()
        # The correct key for the session token is 'session' inside the 'result' object.
        token = json_response.get("result", {}).get("session")
        
        if not token:
            logger.error("Authentication successful, but no session token found in response.")
            return None
        logger.info("Successfully authenticated and received session token.")
        return token
    except httpx.HTTPStatusError as e:
        logger.error(f"Authentication failed: {e.response.status_code} - {e.response.text}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during authentication: {e}", exc_info=True)
        return None

async def get_site_info(client: httpx.AsyncClient, site_url: str, session_token: str) -> tuple[str | None, str | None]:
    """
    Fetches the site name and ID. Assumes the first site returned is the target.
    """
    site_info_url = f"{site_url.rstrip('/')}{SITES_ENDPOINT}"
    params = {"session": session_token}
    try:
        logger.info("Fetching site information...")
        response = await client.get(site_info_url, params=params)
        response.raise_for_status()
        sites_list = response.json().get("result", {}).get("sites", [])
        if not sites_list:
            logger.warning("The API returned an empty list of sites. Could not determine site name.")
            return None, None
        
        site_id = sites_list[0].get("id")
        site_name = sites_list[0].get("name")
        if site_name:
            logger.info(f"Discovered site: {site_name} (ID: {site_id})")
        return site_name, site_id
    except httpx.HTTPStatusError as e:
        logger.error(f"Failed to fetch site info: {e.response.status_code} - {e.response.text}")
        return None, None
    except Exception as e:
        logger.error(f"An unexpected error occurred while fetching site info: {e}", exc_info=True)
        return None, None

async def get_site_servers(client: httpx.AsyncClient, site_url: str, session_token: str) -> list:
    """
    Fetches the list of servers associated with a site.
    """
    servers_url = f"{site_url.rstrip('/')}{SERVERS_ENDPOINT}"
    params = {"session": session_token}
    try:
        logger.info("Fetching list of servers from the site...")
        response = await client.get(servers_url, params=params)
        response.raise_for_status()
        servers = response.json().get("result", {}).get("servers", [])
        if not servers:
            logger.warning("Could not find any servers associated with this site.")
        else:
            logger.info(f"Found {len(servers)} servers.")
        return servers
    except httpx.HTTPStatusError as e:
        logger.error(f"Failed to fetch servers: {e.response.status_code} - {e.response.text}")
        return []
    except Exception as e:
        logger.error(f"An unexpected error occurred while fetching servers: {e}", exc_info=True)
        return []


async def main(args):
    """Main execution logic for the planning tool."""
    site_url = args.site_url
    username = args.username
    password = args.password or getpass("Enter site password: ")
    nonce = args.nonce
    key = args.key or getpass("Enter user key: ")
    target_events = set(args.target_events.split(',')) if args.target_events else set()

    try:
        # The user-provided date range is inclusive.
        from_time_dt = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        to_time_dt_inclusive = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        # The API's 'to' parameter is exclusive, so add one day to include the full end date.
        to_time_dt_for_api = to_time_dt_inclusive + timedelta(days=1)

        from_time_iso = from_time_dt.isoformat().replace("+00:00", "Z")
        to_time_iso_for_api = to_time_dt_for_api.isoformat().replace("+00:00", "Z")

        # Add +1 because the range is inclusive (e.g., Oct 1 to Oct 1 is 1 day)
        scan_duration_days = (to_time_dt_inclusive - from_time_dt).days + 1
        if scan_duration_days <= 0:
            logger.error("End date must be on or after the start date.")
            return
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD.")
        return

    logger.info(f"--- Site Ingestion Planner ---\nSite: {site_url}\nTime Range: {from_time_iso} to {to_time_iso_for_api}\nTarget Event Types: {target_events or 'ALL'}\n" + "-"*33)

    timeout_config = httpx.Timeout(10.0, read=300.0)
    async with httpx.AsyncClient(verify=VERIFY_SSL, timeout=timeout_config) as client:
        session_token = await authenticate_to_site(client, site_url, username, password, nonce, key)
        if not session_token: return

        site_name, _ = await get_site_info(client, site_url, session_token)

        servers = await get_site_servers(client, site_url, session_token)
        if not servers: return

        scanned_server_names = [s.get("name", "Unknown") for s in servers]
        total_event_counts = Counter()
        total_events_scanned = 0
        min_event_ts = None
        max_event_ts = None

        grand_total_start_time = time.monotonic()

        for server in servers:
            server_id = server.get("id")
            server_name = server.get("name", "Unknown Server")

            # --- FIX: Construct server-specific URL from reachableIPs ---
            # While discovery happens at the site level, event queries must be sent
            # to the specific server's IP address for accurate results.
            server_base_url = site_url # Default to site base URL
            reachable_ips = server.get('reachableIPs')
            if reachable_ips and isinstance(reachable_ips, list) and len(reachable_ips) > 0:
                first_ip = reachable_ips[0]
                try:
                    parsed_site_url = urlparse(site_url)
                    # Reconstruct netloc with the new IP and original port
                    new_netloc = f"{first_ip}:{parsed_site_url.port}" if parsed_site_url.port else first_ip
                    server_base_url = urlunparse(parsed_site_url._replace(netloc=new_netloc))
                    logger.info(f"Using server-specific URL '{server_base_url}' for server '{server_name}'.")
                except Exception as e:
                    logger.warning(f"Could not parse site URL to construct server-specific URL for '{server_name}'. Falling back to site URL. Error: {e}")

            if not server_id:
                logger.warning(f"Skipping a server entry because it has no 'id': {server}")
                continue

            logger.info(f"\n>>> Processing Server: {server_name} (ID: {server_id})")
            server_start_time = time.monotonic()
            server_events_scanned = 0
            server_appearance_events_counted = 0
            server_generic_events_counted = 0
            
            # --- 1. Process Appearance Events ---
            # Run if 'APPEARANCE_EVENT' is in targets, or if no targets are specified (all).
            if not target_events or "APPEARANCE_EVENT" in target_events:
                logger.info(f"--- Starting APPEARANCE_EVENT scan for server: {server_name} ---")
                gender_descriptors = [
                    ([{"facet": FACET_GENDER, "tag": TAG_MALE}], "APPEARANCE_EVENT_MALE"),
                    ([{"facet": FACET_GENDER, "tag": TAG_FEMALE}], "APPEARANCE_EVENT_FEMALE")
                ]
                for descriptors, counter_key in gender_descriptors:
                    async for appearance_page in fetch_appearances_for_planning(client, session_token, server_base_url, descriptors, from_time_iso, to_time_iso_for_api, limit=APPEARANCE_PAGE_SIZE):
                        page_count = len(appearance_page)
                        for appearance in appearance_page:
                            ts_str = appearance.get("timestamp")
                            if ts_str:
                                try:
                                    event_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                                    if min_event_ts is None or event_dt < min_event_ts: min_event_ts = event_dt
                                    if max_event_ts is None or event_dt > max_event_ts: max_event_ts = event_dt
                                except ValueError:
                                    logger.debug(f"Could not parse timestamp from appearance: {ts_str}")
                        total_events_scanned += page_count
                        server_events_scanned += page_count
                        server_appearance_events_counted += page_count
                        total_event_counts[counter_key] += page_count
                        # Also count towards the main APPEARANCE_EVENT total for a combined view
                        total_event_counts["APPEARANCE_EVENT"] += page_count

            # --- 2. Process Generic Events (/events/search) ---
            # This endpoint returns all event types other than appearances.
            # We only run this scan if:
            #  a) No specific targets were provided (scan for everything).
            #  b) Specific targets were provided, and at least one is not 'APPEARANCE_EVENT'.
            should_scan_generic = not target_events or any(t != "APPEARANCE_EVENT" for t in target_events)

            if should_scan_generic:
                logger.info(f"--- Starting generic event scan for server: {server_name} ---")
                async for event_page in fetch_events_with_token_pagination(client, session_token, server_base_url, server_id, from_time_iso, to_time_iso_for_api, limit=GENERIC_EVENTS_PAGE_SIZE):
                    total_events_scanned += len(event_page)
                    server_events_scanned += len(event_page)
                    for event in event_page:
                        ts_str = event.get("timestamp")
                        if ts_str:
                            try:
                                event_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                                if min_event_ts is None or event_dt < min_event_ts: min_event_ts = event_dt
                                if max_event_ts is None or event_dt > max_event_ts: max_event_ts = event_dt
                            except ValueError:
                                logger.debug(f"Could not parse timestamp from event: {ts_str}")

                        event_type = event.get("type")
                        if not event_type:
                            continue

                        if not target_events or event_type in target_events:
                            server_generic_events_counted += 1
                            total_event_counts[event_type] += 1

            server_duration = time.monotonic() - server_start_time
            logger.info(f"<<< Finished Server: {server_name}. Scanned {server_events_scanned} total events. Counted {server_appearance_events_counted} appearances and {server_generic_events_counted} generic events in {server_duration:.2f}s.")

    grand_total_duration = time.monotonic() - grand_total_start_time

    # --- Report Generation ---
    # Helper to estimate monthly event counts based on the scan duration
    def get_monthly_estimate(count, days):
        if days <= 0: return 0
        return round((count / days) * 30.44)

    # Filter out unknown events and calculate totals BEFORE printing
    appearance_male = total_event_counts.pop("APPEARANCE_EVENT_MALE", 0)
    appearance_female = total_event_counts.pop("APPEARANCE_EVENT_FEMALE", 0)
    appearance_total = total_event_counts.pop("APPEARANCE_EVENT", 0)

    known_generic_counts = {
        et: count for et, count in total_event_counts.items() if et in EVENT_TYPE_PLANNING_GUIDE
    }
    total_generic_events = sum(known_generic_counts.values())
    total_counted_events = appearance_total + total_generic_events

    # Format the actual time range based on events found
    if min_event_ts and max_event_ts:
        actual_from_iso = min_event_ts.isoformat().replace("+00:00", "Z")
        actual_to_iso = max_event_ts.isoformat().replace("+00:00", "Z")
        time_range_display = f"{actual_from_iso} to {actual_to_iso} ({scan_duration_days} day scan window)"
    else:
        time_range_display = f"No events found in requested range ({from_time_iso} to {to_time_iso_for_api})"

    print("\n" + "="*100 + "\n           SITE INGESTION PLANNER - FINAL REPORT\n" + "="*100)
    print(f"{'Site URL:':<25} {site_url}")
    print(f"{'Site Name:':<25} {site_name or 'Not Found'}")
    print(f"{'Servers Scanned:':<25} {len(scanned_server_names)} ({', '.join(scanned_server_names)})")
    print(f"{'Time Range Analyzed:':<25} {time_range_display}")
    print(f"{'Total Scan Time:':<25} {grand_total_duration:.2f} seconds")
    print(f"{'Total API Events Fetched:':<25} {total_events_scanned:,} events")
    print("-"*100)
    print("EVENT COUNT & FACIAL RECOGNITION YIELD ESTIMATES")
    print("(Only event types known to the planner are shown. Yield is an estimate of value for finding faces.)")
    print(f"{'Event Type':<40} {'Count in Scan':>15} {'Monthly Est.':>15}   {'Yield (Face Recognition)':<25}")
    print("-"*100)

    if total_counted_events == 0:
        print("  No matching events found in the specified time range.")
    else:
        # --- Print Appearance Event Summary ---
        if appearance_total > 0:
            planning_info = EVENT_TYPE_PLANNING_GUIDE.get("APPEARANCE_EVENT")
            note = f"{planning_info['yield']} (~{planning_info['success_rate']:.1f}%)" if planning_info else ""
            monthly_est_str = f"~{get_monthly_estimate(appearance_total, scan_duration_days):,}"
            print(f"{'APPEARANCE EVENTS (/appearance/search)':<40} {appearance_total:>15,} {monthly_est_str:>15}   {note:<25}")
            
            monthly_male_est_str = f"~{get_monthly_estimate(appearance_male, scan_duration_days):,}"
            print(f"{'  - Male Appearances':<40} {appearance_male:>15,} {monthly_male_est_str:>15}")
            
            monthly_female_est_str = f"~{get_monthly_estimate(appearance_female, scan_duration_days):,}"
            print(f"{'  - Female Appearances':<40} {appearance_female:>15,} {monthly_female_est_str:>15}")

        # --- Print Generic Event Summary ---
        if total_generic_events > 0:
            if appearance_total > 0:
                print("-" * 100)
            
            monthly_generic_est_str = f"~{get_monthly_estimate(total_generic_events, scan_duration_days):,}"
            print(f"{'GENERIC EVENTS (/events/search)':<40} {total_generic_events:>15,} {monthly_generic_est_str:>15}")
            
            sorted_generic_counts = sorted(known_generic_counts.items(), key=lambda item: item[1], reverse=True)
            for event_type, count in sorted_generic_counts:
                planning_info = EVENT_TYPE_PLANNING_GUIDE.get(event_type)
                note = f"{planning_info['yield']} (~{planning_info['success_rate']:.1f}%)" if planning_info and planning_info['yield'] != 'Unknown' else ""
                monthly_est_str = f"~{get_monthly_estimate(count, scan_duration_days):,}"
                print(f"{'  - ' + event_type:<40} {count:>15,} {monthly_est_str:>15}   {note:<25}")

        # --- Print Grand Total Footer ---
        print("-" * 100)
        total_monthly_est_str = f"~{get_monthly_estimate(total_counted_events, scan_duration_days):,}"
        print(f"{'GRAND TOTAL (Known Types)':<40} {total_counted_events:>15,} {total_monthly_est_str:>15}")
        print("="*100)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A utility to scan an Avigilon site and count events to plan for data ingestion.")
    parser.add_argument("--site-url", required=True, help="The base URL of the Avigilon site (e.g., https://192.168.1.100).")
    parser.add_argument("--username", required=True, help="Username for authenticating with the site.")
    parser.add_argument("--password", help="Password for the site. If not provided, it will be prompted for securely.")
    parser.add_argument("--nonce", required=True, help="The user-specific nonce for API authentication.")
    parser.add_argument("--key", help="The user-specific key for API authentication. If not provided, it will be prompted for securely.")
    parser.add_argument("--start-date", required=True, help="The start date for the event scan, in YYYY-MM-DD format.")
    parser.add_argument("--end-date", required=True, help="The end date for the event scan, in YYYY-MM-DD format.")
    parser.add_argument(
        "--target-events",
        help="Optional. A comma-separated list of specific event types to scan for. "
             "This is useful for speeding up the scan on noisy sites by focusing only on high-value events. "
             "See the EVENT_TYPE_PLANNING_GUIDE in the script for a list of known types. "
             "If omitted, the planner will scan for and count all discoverable event types.")

    args = parser.parse_args()
    asyncio.run(main(args))