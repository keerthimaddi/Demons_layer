from pathlib import Path
import yaml


# ============================================================
# LOAD DQ RULES
# ============================================================

def load_dq_rules():
    """
    Load DQ rules and overall score thresholds
    from config/dq_rules.yml.
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

    # --------------------------------------------------------
    # Validate configuration
    # --------------------------------------------------------

    if not config:
        raise ValueError(
            "DQ rules YAML is empty."
        )

    dq_framework = config.get(
        "dq_framework",
        {}
    )

    rules = dq_framework.get(
        "checks",
        []
    )

    overall_thresholds = dq_framework.get(
        "overall_score_threshold",
        {
            "pass": 90,
            "warning": 75,
            "fail": 0
        }
    )

    if not rules:
        raise ValueError(
            "No DQ rules found in dq_rules.yml."
        )

    # --------------------------------------------------------
    # Print configuration
    # --------------------------------------------------------

    print("\n======================================")
    print("DQ RULE CONFIGURATION")
    print("======================================")

    for rule in rules:

        print(
            f"{rule['id']} | "
            f"{rule['name']} | "
            f"Level: {rule['level']} | "
            f"Enabled: {rule['enabled']} | "
            f"Weight: {rule['default_weight']}"
        )

    print(
        f"\nOverall Score Thresholds:"
        f"\nPASS    >= {overall_thresholds.get('pass')}"
        f"\nWARNING >= {overall_thresholds.get('warning')}"
        f"\nFAIL    < {overall_thresholds.get('warning')}"
    )

    return rules, overall_thresholds