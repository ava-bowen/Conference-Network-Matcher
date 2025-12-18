import tempfile

import streamlit as st
import pandas as pd

from contacts_matcher import (
    init_db,
    load_contacts_from_csv,
    match_attendees_from_csv,
)

# Make sure DB exists
init_db()

st.set_page_config(page_title="Conference Network Matcher", layout="wide")

# --- SIDEBAR: Admin toggle ---
st.sidebar.title("Settings")
admin_mode = st.sidebar.checkbox("Show admin panel (manage LinkedIn contacts)", value=False)

st.title("Conference Network Matcher")
st.write(
    "Compare a conference attendee list against your firm's existing LinkedIn network. "
    "Conference files are not stored; they're only used in-memory for matching."
)


def save_uploaded_file(uploaded_file) -> str:
    """Save uploaded CSV to a temporary file and return its path."""
    import os
    import tempfile as tf

    fd, temp_path = tf.mkstemp(suffix=".csv")
    with open(temp_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    os.close(fd)
    return temp_path


# --- ADMIN PANEL: Load / refresh LinkedIn contacts ---

if admin_mode:
    st.header("Admin: Load or Refresh LinkedIn Contacts")

    st.info(
        "Use this panel to load or update LinkedIn exports into the master contacts database.\n\n"
        "- Re-uploading for the same owner + source will **replace** their old contacts.\n"
        "- End users don't need to touch this; they just upload conference lists below."
    )

    contacts_file = st.file_uploader(
        "Upload LinkedIn connections CSV",
        type="csv",
        key="contacts_uploader",
        help="CSV exported from LinkedIn. Supports either a single 'Name' column or 'First Name' + 'Last Name'.",
    )

    col1, col2 = st.columns(2)
    with col1:
        owner_name = st.text_input(
            "Owner name (who these contacts belong to)",
            value="",
            placeholder="e.g. Ava Bowen",
        )
    with col2:
        source_label = st.text_input(
            "Source label",
            value="LinkedIn",
            placeholder="e.g. LinkedIn_Ava",
        )

    if st.button("Load / Refresh Contacts", type="primary"):
        if contacts_file is None:
            st.error("Please upload a LinkedIn CSV first.")
        elif not owner_name.strip():
            st.error("Please enter the owner name.")
        else:
            temp_path = save_uploaded_file(contacts_file)
            try:
                load_contacts_from_csv(
                    temp_path,
                    owner=owner_name.strip(),
                    source=source_label.strip(),
                )
                st.success(
                    f"Contacts for owner '{owner_name.strip()}' and source '{source_label.strip()}' "
                    "have been refreshed in the database."
                )
            except Exception as e:
                st.error(f"Something went wrong loading contacts: {e}")

    st.divider()

# --- USER FLOW: Upload conference list & see matches ---

st.header("1. Upload Conference Attendee List")

attendees_file = st.file_uploader(
    "Upload conference attendee CSV",
    type="csv",
    key="attendees_uploader",
    help="CSV with attendee names and companies. Needs a 'Name' or 'Full Name' column. "
         "Company/Organization column is recommended for better matching.",
)

col3, col4 = st.columns(2)
with col3:
    threshold = st.slider(
        "Match strictness (higher = fewer, stronger matches)",
        min_value=50,
        max_value=100,
        value=85,
        step=1,
    )
with col4:
    show_scores = st.checkbox("Show match scores", value=True)

if st.button("Compare Against Our Network", type="primary"):
    if attendees_file is None:
        st.error("Please upload a conference attendee CSV first.")
    else:
        temp_path = save_uploaded_file(attendees_file)
        try:
            matches_df = match_attendees_from_csv(temp_path, threshold=int(threshold))

            if matches_df.empty:
                st.warning(
                    "No matches found above the selected threshold. "
                    "You can try lowering the strictness slider."
                )
            else:
                st.subheader("Matches Found")

                # Optionally hide score if user doesn't care
                display_df = matches_df.copy()
                if not show_scores and "match_score" in display_df.columns:
                    display_df = display_df.drop(columns=["match_score"])

                # Reorder to focus on attendee + contact name & company
                preferred_cols = [
                    "attendee_name",
                    "attendee_company",
                    "contact_name",
                    "contact_company",
                    "contact_title",
                    "contact_owner",
                    "contact_email",
                    "match_score",
                ]
                cols = [c for c in preferred_cols if c in display_df.columns] + [
                    c for c in display_df.columns if c not in preferred_cols
                ]
                display_df = display_df[cols]

                st.dataframe(display_df, use_container_width=True)

                # Download button for matches
                csv_bytes = matches_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Download matches as CSV",
                    data=csv_bytes,
                    file_name="conference_matches.csv",
                    mime="text/csv",
                )

                st.caption(
                    "Conference data is not stored in the database; this result is based only on the file you just uploaded."
                )
        except Exception as e:
            st.error(f"Something went wrong when matching: {e}")
