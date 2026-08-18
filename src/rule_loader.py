from pathlib import Path
import yaml


# ============================================================
# LOAD DQ RULES
# ============================================================

def load_dq_rules():
    """
    Load the new YAML-based DQ configuration and convert it
    into the rule-list structure currently expected by
    dq_engine.py.

    Returns:
        rules
        overall_thresholds
    """

    # --------------------------------------------------------
    # Project root
    # --------------------------------------------------------

    project_root = Path(__file__).resolve().parent.parent

    # --------------------------------------------------------
    # YAML configuration path
    # --------------------------------------------------------

    rules_path = (
        project_root
        / "config"
        / "dq_rules.yml"
    )

    # --------------------------------------------------------
    # Validate file
    # --------------------------------------------------------

    if not rules_path.exists():

        raise FileNotFoundError(
            f"DQ rules file not found: {rules_path}"
        )

    # --------------------------------------------------------
    # Load YAML
    # --------------------------------------------------------

    with open(
        rules_path,
        "r",
        encoding="utf-8"
    ) as file:

        config = yaml.safe_load(file)

    if not config:
        raise ValueError(
            "DQ rules YAML is empty."
        )

    # ========================================================
    # FRAMEWORK CONFIGURATION
    # ========================================================

    framework_config = config.get(
        "framework",
        {}
    )

    if not framework_config:
        raise ValueError(
            "Missing 'framework' section in dq_rules.yml."
        )

    # --------------------------------------------------------
    # Overall score thresholds
    # --------------------------------------------------------

    overall_thresholds = (
        framework_config.get(
            "default_status_thresholds",
            {
                "pass": 90,
                "warning": 75,
                "fail": 0
            }
        )
    )

    # ========================================================
    # DQ RULES
    # ========================================================

    dq_rules_config = config.get(
        "dq_rules",
        {}
    )

    if not dq_rules_config:
        raise ValueError(
            "Missing 'dq_rules' section in dq_rules.yml."
        )

    # --------------------------------------------------------
    # Expected 16 DQ dimensions
    # --------------------------------------------------------

    expected_dimensions = [
        "completeness",
        "accuracy",
        "validity",
        "uniqueness",
        "consistency",
        "integrity",
        "timeliness",
        "conformity",
        "range",
        "duplicate",
        "null",
        "length",
        "data_type",
        "pattern",
        "business_rule",
        "volume"
    ]

    # --------------------------------------------------------
    # Validate all 16 dimensions
    # --------------------------------------------------------

    missing_dimensions = [
        dimension
        for dimension in expected_dimensions
        if dimension not in dq_rules_config
    ]

    if missing_dimensions:

        raise ValueError(
            "Missing DQ dimensions: "
            f"{missing_dimensions}"
        )

    # ========================================================
    # CONVERT YAML DICTIONARY → RULE LIST
    # ========================================================

    rules = []

    for dimension in expected_dimensions:

        rule_config = dq_rules_config[
            dimension
        ]

        # ----------------------------------------------------
        # Validate required fields
        # ----------------------------------------------------

        required_fields = [
            "id",
            "enabled",
            "weight",
            "level",
            "severity",
            "rules"
        ]

        missing_fields = [
            field
            for field in required_fields
            if field not in rule_config
        ]

        if missing_fields:

            raise ValueError(
                f"{dimension} is missing fields: "
                f"{missing_fields}"
            )

        # ----------------------------------------------------
        # Create rule object expected by dq_engine.py
        # ----------------------------------------------------

        rule = {
            "id": rule_config["id"],

            "name": dimension,

            "level": rule_config["level"],

            "enabled": rule_config["enabled"],

            "default_weight": float(
                rule_config["weight"]
            ),

            "severity": rule_config["severity"],

            "rules": rule_config["rules"],

            # Keep threshold available for the current
            # dq_engine implementation.
            #
            # If individual threshold configuration is
            # added later, it will be populated here.
            "threshold": {}
        }

        rules.append(rule)

    # ========================================================
    # VALIDATE TOTAL WEIGHT
    # ========================================================

    enabled_rules = [
        rule
        for rule in rules
        if rule["enabled"]
    ]

    total_weight = sum(
        rule["default_weight"]
        for rule in enabled_rules
    )

    if total_weight <= 0:

        raise ValueError(
            "Total enabled DQ weight must be greater than zero."
        )

    # ========================================================
    # PRINT CONFIGURATION
    # ========================================================

    print("\n======================================")
    print("DQ RULE CONFIGURATION")
    print("======================================")

    for rule in rules:

        print(
            f"{rule['id']} | "
            f"{rule['name']} | "
            f"Level: {rule['level']} | "
            f"Enabled: {rule['enabled']} | "
            f"Weight: {rule['default_weight']} | "
            f"Severity: {rule['severity']}"
        )

    print(
        f"\nTotal DQ Rules    : {len(rules)}"
    )

    print(
        f"Enabled DQ Rules  : {len(enabled_rules)}"
    )

    print(
        f"Total Weight      : {total_weight}"
    )

    print(
        "\nOverall Score Thresholds:"
    )

    print(
        f"PASS    >= "
        f"{overall_thresholds.get('pass', 90)}"
    )

    print(
        f"WARNING >= "
        f"{overall_thresholds.get('warning', 75)}"
    )

    print(
        f"FAIL    < "
        f"{overall_thresholds.get('warning', 75)}"
    )

    # ========================================================
    # RETURN
    # ========================================================

    return rules, overall_thresholds