import os
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

# --- CONFIGURATION ---

BASE_FOLDER = r"G:\Shared drives\GCS  - navify Analytics\02_Initiatives\24_AI Automation"

REACTIVE_FILE_NAME = "navify analytics closed tickets (Monthly).xlsx"
PROACTIVE_FILE_NAME = "Tenant.xlsx"

SENDER_EMAIL = "prathamesh.patil@roche.com"
RECEIVER_EMAIL = "prathamesh.patil@roche.com"
PASSWORD = "fcur kzfm ddso uwpk"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

SLA_DAYS = 10
OUTLIER_DAYS = 10
CLOSURE_OFFSET_DAYS = 7
PROACTIVE_LOOKBACK_DAYS = 30

PRODUCT_TAGS = {"#nacore", "#napoc", "#nahiv", "#naap", "#namol"}

# ----------------------------------------------------


# -------- FILE LOADER --------
def resolve_path(file_name):

    path = os.path.join(BASE_FOLDER, file_name)

    if os.path.exists(path):
        return path

    print("File not found:", path)
    return None


def load_file(file_name):

    try:

        path = resolve_path(file_name)

        if not path:
            return None

        print("Loading:", path)

        return pd.read_excel(path, engine="openpyxl")

    except Exception as e:

        print("Excel read error:", e)

        return None


# -------- DATA PREPARATION --------
def prepare_df(df):

    required_cols = {"Number", "Opened", "Updated"}

    if not required_cols.issubset(df.columns):

        missing = list(required_cols - set(df.columns))
        raise ValueError(f"Missing columns: {missing}")

    df = df.drop_duplicates(subset="Number").copy()

    df["Opened"] = pd.to_datetime(df["Opened"], errors="coerce")
    df["Updated"] = pd.to_datetime(df["Updated"], errors="coerce")

    df["Resolution Date (Adj)"] = df["Updated"] - pd.to_timedelta(CLOSURE_OFFSET_DAYS, unit="D")

    df["Open Duration (Days)"] = (
        (df["Resolution Date (Adj)"] - df["Opened"]).dt.total_seconds() / 86400
    )

    df.loc[df["Open Duration (Days)"] < 0, "Open Duration (Days)"] = pd.NA

    return df


# -------- TABLE GENERATOR --------
def build_ticket_table(df, empty_msg):

    if df.empty:
        return f"<p>{empty_msg}</p>"

    cols = ["Number", "Ticket Type"]

    if "Short description" in df.columns:
        cols.append("Short description")

    return (
        df[cols]
        .fillna("")
        .rename(columns={
            "Number": "Incident Number",
            "Short description": "Short Description"
        })
        .to_html(index=False, escape=False)
    )


# -------- TAG ANALYTICS --------
def build_tag_tables(df):

    if "Tags" not in df.columns:
        return "<p>No tags found</p>", "<p>No product tags</p>"

    tags = (
        df["Tags"]
        .dropna()
        .astype(str)
        .str.lower()
        .str.split(",")
        .explode()
        .str.strip()
    )

    product_tags = tags[tags.isin(PRODUCT_TAGS)]
    issue_tags = tags[~tags.isin(PRODUCT_TAGS)]

    issue_table = (
        issue_tags.value_counts()
        .head(5)
        .reset_index()
        .rename(columns={"index": "Tag", "count": "Count"})
        .to_html(index=False)
    )

    product_table = (
        product_tags.value_counts()
        .reset_index()
        .rename(columns={"index": "Tag", "count": "Count"})
        .to_html(index=False)
    )

    return issue_table, product_table


# -------- SLA CALCULATION --------
def sla_pct(df, sla_days):

    if df.empty:
        return 0

    valid = df["Open Duration (Days)"].dropna()

    if valid.empty:
        return 0

    return (len(df[df["Open Duration (Days)"] <= sla_days]) / len(df)) * 100


# -------- MAIN ANALYSIS --------
def analyze_data():

    reactive_raw = load_file(REACTIVE_FILE_NAME)
    proactive_raw = load_file(PROACTIVE_FILE_NAME)

    if reactive_raw is None:
        return "Error reading reactive file"

    if proactive_raw is None:
        return "Error reading proactive file"

    reactive = prepare_df(reactive_raw)
    proactive = prepare_df(proactive_raw)

    now = datetime.now()
    cutoff = now - timedelta(days=PROACTIVE_LOOKBACK_DAYS)

    cutoff_str = cutoff.strftime("%Y-%m-%d")

    proactive_30d = proactive[proactive["Opened"] >= cutoff].copy()

    reactive["Ticket Type"] = "Reactive"
    proactive_30d["Ticket Type"] = "Proactive"

    total_reactive = len(reactive)
    total_proactive = len(proactive_30d)

    sla_reactive = sla_pct(reactive, SLA_DAYS)
    sla_proactive = sla_pct(proactive_30d, SLA_DAYS)

    long_reactive = reactive[
        reactive["Open Duration (Days)"] > OUTLIER_DAYS
    ]

    long_proactive = proactive_30d[
        proactive_30d["Open Duration (Days)"] > OUTLIER_DAYS
    ]

    combined_long = pd.concat([long_reactive, long_proactive])

    long_running_table = build_ticket_table(
        combined_long,
        f"No tickets open > {OUTLIER_DAYS} days"
    )

    combined = pd.concat([reactive, proactive_30d])

    issue_tags_table, product_tags_table = build_tag_tables(combined)

    l3_mask = combined["Tags"].astype(str).str.contains("l3", case=False, na=False)
    l3_tickets = combined[l3_mask]

    l3_table = build_ticket_table(l3_tickets, "No L3 tickets found")

    critical = combined[combined["Open Duration (Days)"] > 30]

    critical_table = build_ticket_table(
        critical,
        "No tickets open >30 days"
    )

    # -------- HTML EMAIL --------
    summary = f"""
<html>
<head>

<style>

body {{
font-family: Arial, sans-serif;
}}

h2 {{
color:#2E86C1;
}}

h3 {{
color:#117A65;
}}

table {{
border-collapse: collapse;
width:100%;
margin-bottom:20px;
}}

th,td {{
border:1px solid #ddd;
padding:8px;
text-align:left;
}}

th {{
background-color:#f2f2f2;
}}

.critical-title {{
color:#B03A2E;
}}

.note {{
color:#555;
font-size:12px;
}}

</style>

</head>

<body>

<h2>Monthly Ticket Health Report for navify analytics</h2>

<p class="note">
This dataset is filtered to last 30 days (Opened on/after {cutoff_str})
</p>

<p>

<strong>Total Tickets Closed (Reactive):</strong> {total_reactive}<br>
<strong>Total Tickets Closed (Proactive):</strong> {total_proactive}<br><br>

<strong>SLA Performance (Reactive):</strong> {sla_reactive:.2f}%<br>
<strong>SLA Performance (Proactive):</strong> {sla_proactive:.2f}%

</p>

<h3>Long-Running Closed Tickets (> {OUTLIER_DAYS} days)</h3>

{long_running_table}

<h3>Most Common Issues</h3>

{issue_tags_table}

<h3>Product Tag Distribution</h3>

{product_tags_table}

<h3>Tickets Raised to L3</h3>

{l3_table}

<h3 class="critical-title">Combined Tickets – Open > 30 Days</h3>

{critical_table}

</body>
</html>
"""

    return summary


# -------- EMAIL SENDER --------
def send_email(subject, body):

    msg = MIMEMultipart()

    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "html"))

    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)

    server.starttls()

    server.login(SENDER_EMAIL, PASSWORD)

    server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())

    server.quit()

    print("Email sent successfully")


# -------- MAIN --------
if __name__ == "__main__":

    report = analyze_data()

    if report.startswith("Error"):

        send_email(
            "📊 Navify Analytics – Monthly Ticket Health Report FAILED",
            report
        )

    else:

        send_email(
            "📊 Navify Analytics – Monthly Ticket Health Report",
            report
        )