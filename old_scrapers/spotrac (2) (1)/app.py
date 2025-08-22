# app.py
import os
from datetime import datetime
from typing import List, Dict, Any

import streamlit as st
from pymongo import MongoClient
from bson import ObjectId

# -----------------------
# CONFIG
# -----------------------
st.set_page_config(page_title="Streamlit + MongoDB CRUD", layout="wide")

# Prefer env vars; fallback to a local dev string
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "demo_db")
COLL_NAME = os.getenv("MONGO_COLL", "people")

# -----------------------
# HELPERS
# -----------------------
@st.cache_resource(show_spinner=False)
def get_client(uri: str):
    return MongoClient(uri)

def to_view(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Convert Mongo doc to a UI-friendly dict (stringify _id)."""
    d = dict(doc)
    d["_id"] = str(d["_id"])
    return d

def to_objectid(id_str: str) -> ObjectId:
    return ObjectId(id_str)

def build_query(q: str) -> Dict[str, Any]:
    if not q:
        return {}
    # simple case-insensitive search on name/email fields
    return {"$or": [
        {"name": {"$regex": q, "$options": "i"}},
        {"email": {"$regex": q, "$options": "i"}}
    ]}

# -----------------------
# DB
# -----------------------
client = get_client(MONGO_URI)
db = client[DB_NAME]
coll = db[COLL_NAME]

# Optional: ensure an index or two
coll.create_index("email", unique=False)
coll.create_index([("name", 1)])

# -----------------------
# UI — SIDEBAR
# -----------------------
st.sidebar.header("Filters & Actions")
query_text = st.sidebar.text_input("Search (name/email)")
page_size = st.sidebar.selectbox("Rows per page", [5, 10, 20, 50], index=1)
page = st.sidebar.number_input("Page (0-based)", min_value=0, step=1, value=0)

# -----------------------
# CREATE
# -----------------------
with st.expander("➕ Create new person", expanded=False):
    with st.form("create_form", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            name = st.text_input("Name", placeholder="Jane Doe")
        with col2:
            email = st.text_input("Email", placeholder="jane@example.com")
        with col3:
            age = st.number_input("Age", min_value=0, step=1)
        notes = st.text_area("Notes", placeholder="Optional notes")
        submitted = st.form_submit_button("Create")
        if submitted:
            if not name or not email:
                st.warning("Name and Email are required.")
            else:
                doc = {
                    "name": name.strip(),
                    "email": email.strip(),
                    "age": int(age),
                    "notes": notes.strip(),
                    "createdAt": datetime.utcnow(),
                    "updatedAt": datetime.utcnow(),
                }
                coll.insert_one(doc)
                st.success("✅ Created!")

# -----------------------
# READ (with pagination)
# -----------------------
st.header("People")
query = build_query(query_text)
total = coll.count_documents(query)
docs = list(
    coll.find(query)
        .sort("createdAt", -1)
        .skip(page * page_size)
        .limit(page_size)
)

if not docs:
    st.info("No documents found.")
else:
    view_rows: List[Dict[str, Any]] = [to_view(d) for d in docs]

    st.caption(f"Showing {len(view_rows)} of {total} total")
    edited = st.data_editor(
        view_rows,
        num_rows="fixed",
        use_container_width=True,
        key=f"editor_page_{page}",
        column_config={
            "_id": st.column_config.TextColumn(disabled=True),
            "createdAt": st.column_config.DatetimeColumn(disabled=True),
            "updatedAt": st.column_config.DatetimeColumn(disabled=True),
        }
    )

    # Determine changed rows for UPDATE
    updates = []
    for original, new in zip(view_rows, edited):
        # compare editable fields only
        changed = {}
        for f in ["name", "email", "age", "notes"]:
            if original.get(f) != new.get(f):
                changed[f] = new.get(f)
        if changed:
            updates.append((original["_id"], changed))

    colA, colB = st.columns(2)

    with colA:
        if st.button("💾 Save changes", type="primary", use_container_width=True):
            count = 0
            for id_str, fields in updates:
                fields["updatedAt"] = datetime.utcnow()
                coll.update_one({"_id": to_objectid(id_str)}, {"$set": fields})
                count += 1
            st.success(f"✅ Updated {count} document(s).")
            st.experimental_rerun()

    with colB:
        # multi-select for DELETE
        delete_ids = st.multiselect(
            "Select rows to delete",
            options=[r["_id"] for r in view_rows],
            format_func=lambda _id: next((r["name"] for r in view_rows if r["_id"] == _id), _id),
        )
        if st.button("🗑️ Delete selected", use_container_width=True, disabled=not delete_ids):
            result = coll.delete_many({"_id": {"$in": [to_objectid(x) for x in delete_ids]}})
            st.success(f"🗑️ Deleted {result.deleted_count} document(s).")
            st.experimental_rerun()
