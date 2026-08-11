from datetime import datetime, timedelta


# ============================================================
# DQ RULE DESCRIPTIONS
# ============================================================

DQ_DESCRIPTIONS = {
    "DQ1": "Completeness check failed",
    "DQ2": "Accuracy check failed",
    "DQ3": "Validity check failed",
    "DQ4": "Uniqueness check failed",
    "DQ5": "Consistency check failed",
    "DQ6": "Integrity check failed",
    "DQ7": "Timeliness check failed",
    "DQ8": "Conformity check failed",
    "DQ9": "Range check failed",
    "DQ10": "Duplicate check failed",
    "DQ11": "Null value check failed",
    "DQ12": "Length check failed",
    "DQ13": "Data type check failed",
    "DQ14": "Pattern check failed",
    "DQ15": "Business rule check failed",
    "DQ16": "Volume check failed"
}


# ============================================================
# DEFAULT REVENUE IMPACT
# ============================================================

DQ_REVENUE_IMPACT = {
    "DQ1": 12500.00,
    "DQ2": 10000.00,
    "DQ3": 8000.00,
    "DQ4": 15000.00,
    "DQ5": 9000.00,
    "DQ6": 18500.00,
    "DQ7": 12000.00,
    "DQ8": 7000.00,
    "DQ9": 11000.00,
    "DQ10": 13500.00,
    "DQ11": 8500.00,
    "DQ12": 5000.00,
    "DQ13": 7500.00,
    "DQ14": 6000.00,
    "DQ15": 29000.00,
    "DQ16": 4200.00
}


# ============================================================
# INCIDENT LOGGER
# ============================================================

def create_incident_logs(
    dq_result,
    detection_type="Automated"
):
    """
    Create incident records for every failed DQ rule.

    One failed DQ rule = one incident.

    This is the initial operational incident layer.
    """

    incidents = []

    catalog = dq_result["catalog"]
    schema = dq_result["schema"]
    table = dq_result["table"]

    run_time = datetime.now()

    incident_number = 1

    # --------------------------------------------------------
    # Check all 16 DQ rules
    # --------------------------------------------------------

    for dq_id in DQ_DESCRIPTIONS.keys():

        status = dq_result.get(dq_id)

        if status != "FAIL":
            continue

        # ----------------------------------------------------
        # Incident ID
        # ----------------------------------------------------

        incident_id = (
            f"INC-{run_time.strftime('%Y%m%d')}-"
            f"{incident_number:03d}"
        )

        # ----------------------------------------------------
        # Timestamps
        #
        # These are dummy operational timestamps for now.
        # Later they will come from actual monitoring/logging.
        # ----------------------------------------------------

        event_timestamp = run_time - timedelta(minutes=30)

        detected_timestamp = run_time - timedelta(minutes=25)

        ack_timestamp = run_time - timedelta(minutes=15)

        resolved_timestamp = run_time

        # ----------------------------------------------------
        # Calculate incident timings
        # ----------------------------------------------------

        ttd_minutes = (
            detected_timestamp - event_timestamp
        ).total_seconds() / 60

        tta_minutes = (
            ack_timestamp - detected_timestamp
        ).total_seconds() / 60

        ttr_minutes = (
            resolved_timestamp - detected_timestamp
        ).total_seconds() / 60

        # ----------------------------------------------------
        # Revenue impact
        # ----------------------------------------------------

        revenue_impact = DQ_REVENUE_IMPACT.get(
            dq_id,
            0.0
        )

        # ----------------------------------------------------
        # Incident record
        # ----------------------------------------------------

        incident = {

            "Date": run_time.strftime("%Y-%m-%d"),

            "Incident_ID": incident_id,

            "Incident_Description":
                DQ_DESCRIPTIONS[dq_id],

            "DS":
                f"{catalog}.{schema}.{table}",

            "DQ_Rule":
                dq_id,

            "Rev_Impact_Flag":
                1,

            "Rev_Impact_Amount":
                revenue_impact,

            "Label":
                DQ_DESCRIPTIONS[dq_id],

            "Detection_Type":
                detection_type,

            "Event_Timestamp":
                event_timestamp,

            "Detected_Timestamp":
                detected_timestamp,

            "Ack_Timestamp":
                ack_timestamp,

            "Resolved_Timestamp":
                resolved_timestamp,

            "TTD_Minutes":
                round(ttd_minutes, 2),

            "TTA_Minutes":
                round(tta_minutes, 2),

            "TTR_Minutes":
                round(ttr_minutes, 2)
        }

        incidents.append(incident)

        incident_number += 1

    return incidents