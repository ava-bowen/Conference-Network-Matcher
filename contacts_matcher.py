import argparse
import sqlite3
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process

DB_PATH = "network.db"


# ---------- DB UTILITIES ----------

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create the contacts table if it doesn't exist."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS contacts (
            id INTEGER PRIMARY KEY,
            full_name TEXT,
            first_name TEXT,
            last_name TEXT,
            company TEXT,
            title TEXT,
            email TEXT,
            linkedin_url TEXT,
            source TEXT,
            owner TEXT
        );
        """
    )

    conn.commit()
    conn.close()


# ---------- HELPERS ----------

def normalize_string(s: str) -> str:
    if s is None:
        return ""
    return " ".join(str(s).strip().lower().split())


def split_name(full_name: str):
    """Very basic splitter: last word is last name, rest is first name."""
    if not full_name:
        return "", ""
    parts = full_name.strip().split()
    if len(parts) == 1:
        return parts[0], ""
    first = " ".join(parts[:-1])
    last = parts[-1]
    return first, last


# ---------- CONTACTS LOADING / UPDATING ----------

def load_contacts_from_csv(csv_path: str, owner: str, source: str):
    """
    Load or refresh LinkedIn contacts for a given owner+source.

    Behavior:
        - First deletes any existing contacts with this (owner, source).
        - Then inserts rows from the CSV.

    Expected columns (flexible, script will try to map):
        - full_name or Name (required, or First Name + Last Name)
        - company or Company
        - title or Title
        - email or Email
        - linkedin_url or URL / LinkedIn URL
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(path)

    # Try to detect columns
    col_map = {}
    for col in df.columns:
        lc = col.strip().lower()
        if lc in ["full_name", "name"]:
            col_map["full_name"] = col
        elif lc in ["first name", "firstname", "given name"]:
            col_map["first_name"] = col
        elif lc in ["last name", "lastname", "surname", "family name"]:
            col_map["last_name"] = col
        elif lc in ["company", "current company", "organization"]:
            col_map["company"] = col
        elif lc in ["title", "position", "job title"]:
            col_map["title"] = col
        elif lc in ["email", "email address"]:
            col_map["email"] = col
        elif lc in ["url", "linkedin url", "profile url"]:
            col_map["linkedin_url"] = col

    if "full_name" not in col_map and not (
        "first_name" in col_map and "last_name" in col_map
    ):
        raise ValueError(
            "Could not find a 'Name'/'full_name' column or 'First Name' + 'Last Name' in the contacts CSV."
        )

    # Build a full_name column if needed
    if "full_name" not in col_map:
        fn_col = col_map["first_name"]
        ln_col = col_map["last_name"]
        df["__full_name_tmp"] = (
            df[fn_col].fillna("").astype(str).str.strip()
            + " "
            + df[ln_col].fillna("").astype(str).str.strip()
        )
        col_map["full_name"] = "__full_name_tmp"

    conn = get_conn()
    cur = conn.cursor()

    # Delete existing contacts for this owner+source (update behavior)
    cur.execute(
        "DELETE FROM contacts WHERE owner = ? AND source = ?",
        (owner, source),
    )
    deleted = cur.rowcount or 0

    inserted = 0
    for _, row in df.iterrows():
        full_name_raw = str(row[col_map["full_name"]]).strip()
        if not full_name_raw:
            continue

        company = str(row[col_map.get("company", "")]) if "company" in col_map else ""
        title = str(row[col_map.get("title", "")]) if "title" in col_map else ""
        email = str(row[col_map.get("email", "")]) if "email" in col_map else ""
        linkedin_url = (
            str(row[col_map.get("linkedin_url", "")])
            if "linkedin_url" in col_map
            else ""
        )

        first_name, last_name = split_name(full_name_raw)

        cur.execute(
            """
            INSERT INTO contacts (
                full_name, first_name, last_name, company, title, email, linkedin_url, source, owner
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                full_name_raw,
                first_name,
                last_name,
                company,
                title,
                email,
                linkedin_url,
                source,
                owner,
            ),
        )
        inserted += 1

    conn.commit()
    conn.close()
    print(
        f"Refreshed contacts for owner='{owner}', source='{source}'. "
        f"Deleted {deleted} old rows, inserted {inserted} new contacts from {csv_path}."
    )


# ---------- MATCHING: CONTACTS vs EPHEMERAL CONFERENCE CSV ----------

def build_contact_key(row):
    name = normalize_string(row["full_name"])
    company = normalize_string(row["company"])
    return f"{name} | {company}".strip(" |")


def build_attendee_key(row):
    name = normalize_string(row["attendee_name"])
    company = normalize_string(row["attendee_company"])
    return f"{name} | {company}".strip(" |")


def match_attendees_from_csv(csv_path: str, threshold: int = 85) -> pd.DataFrame:
    """
    Compare an attendee CSV (conference list) against the contacts database.

    DOES NOT store anything about the attendees. All in-memory + returns a DataFrame.

    Expected attendee columns (flexible):
        - name or full name
        - company or organization/org
        - email or email address (optional)
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Attendee CSV not found: {csv_path}")

    # Load contacts
    conn = get_conn()
    contacts_df = pd.read_sql_query("SELECT * FROM contacts", conn)
    conn.close()

    if contacts_df.empty:
        raise ValueError("No contacts in database. Load contacts first.")

    contacts_df["match_key"] = contacts_df.apply(build_contact_key, axis=1)

    # Load attendees CSV
    raw_df = pd.read_csv(path)

    # Map columns
    col_map = {}
    for col in raw_df.columns:
        lc = col.strip().lower()
        if lc in ["name", "full name"]:
            col_map["name"] = col
        elif lc in ["company", "organization", "org", "employer"]:
            col_map["company"] = col
        elif lc in ["email", "email address"]:
            col_map["email"] = col

    if "name" not in col_map:
        raise ValueError("Could not find a 'name' or 'full name' column in attendee CSV.")

    attendees_df = pd.DataFrame()
    attendees_df["attendee_name"] = raw_df[col_map["name"]].astype(str).fillna("").str.strip()
    attendees_df["attendee_company"] = (
        raw_df[col_map["company"]].astype(str).fillna("").str.strip()
        if "company" in col_map
        else ""
    )
    attendees_df["attendee_email"] = (
        raw_df[col_map["email"]].astype(str).fillna("").str.strip()
        if "email" in col_map
        else ""
    )

    attendees_df["match_key"] = attendees_df.apply(build_attendee_key, axis=1)

    # Prep for fuzzy match
    contact_keys = contacts_df["match_key"].tolist()
    # map key -> index in contacts_df
    key_to_index = {row["match_key"]: idx for idx, row in contacts_df.iterrows()}

    matches_export = []

    for _, att in attendees_df.iterrows():
        query_key = att["match_key"]
        if not query_key:
            continue

        best_match = process.extractOne(
            query_key,
            contact_keys,
            scorer=fuzz.token_sort_ratio,
        )

        if best_match is None:
            continue

        best_key, score, _ = best_match

        if score < threshold:
            continue

        contact_idx = key_to_index[best_key]
        contact_row = contacts_df.iloc[contact_idx]

        matches_export.append(
            {
                "attendee_name": att["attendee_name"],
                "attendee_company": att["attendee_company"],
                "attendee_email": att["attendee_email"],
                "contact_name": contact_row["full_name"],
                "contact_company": contact_row["company"],
                "contact_title": contact_row["title"],
                "contact_owner": contact_row["owner"],
                "contact_source": contact_row["source"],
                "contact_email": contact_row["email"],
                "match_score": score,
            }
        )

    return pd.DataFrame(matches_export)


# ---------- CLI (optional, still works) ----------

def main():
    parser = argparse.ArgumentParser(
        description="Contacts database + conference matcher."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # init
    subparsers.add_parser("init-db", help="Initialize the SQLite database.")

    # load-contacts (refresh behavior)
    sp_contacts = subparsers.add_parser(
        "load-contacts",
        help="Load/refresh LinkedIn contacts from CSV for a given owner+source.",
    )
    sp_contacts.add_argument("--csv", required=True, help="Path to LinkedIn contacts CSV.")
    sp_contacts.add_argument("--owner", required=True, help="Name of person who owns these contacts.")
    sp_contacts.add_argument(
        "--source",
        default="LinkedIn",
        help="Source label (e.g. LinkedIn_Ava). Default: LinkedIn",
    )

    # match-csv: compare a conference CSV against the DB and write matches CSV
    sp_match = subparsers.add_parser(
        "match-csv",
        help="Match a conference CSV against the contacts DB (ephemeral, not stored).",
    )
    sp_match.add_argument("--csv", required=True, help="Path to attendee CSV.")
    sp_match.add_argument(
        "--threshold",
        type=int,
        default=85,
        help="Match score threshold (0–100). Default: 85.",
    )
    sp_match.add_argument(
        "--output",
        default="matches.csv",
        help="Output CSV file for matches (default: matches.csv).",
    )

    args = parser.parse_args()

    if args.command == "init-db":
        init_db()
        print(f"Initialized database at {DB_PATH}")

    elif args.command == "load-contacts":
        init_db()
        load_contacts_from_csv(args.csv, owner=args.owner, source=args.source)

    elif args.command == "match-csv":
        init_db()
        df_matches = match_attendees_from_csv(args.csv, threshold=args.threshold)
        if df_matches.empty:
            print("No matches found above threshold.")
        else:
            df_matches.to_csv(args.output, index=False)
            print(f"Found {len(df_matches)} matches. Exported to {args.output}.")


if __name__ == "__main__":
    main()
