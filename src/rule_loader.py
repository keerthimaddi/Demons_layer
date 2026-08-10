import os
import yaml


def load_dq_rules():

    # Get project root directory
    project_root = os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )

    # Build path to YAML configuration
    rules_path = os.path.join(
        project_root,
        "config",
        "dq_rules.yml"
    )

    # Check whether file exists
    if not os.path.exists(rules_path):
        raise FileNotFoundError(
            f"DQ rules file not found: {rules_path}"
        )

    # Read YAML file
    with open(rules_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    # Validate YAML structure
    if not config:
        raise ValueError("DQ rules YAML file is empty.")

    if "dq_framework" not in config:
        raise ValueError(
            "Missing 'dq_framework' section in dq_rules.yml"
        )

    if "checks" not in config["dq_framework"]:
        raise ValueError(
            "Missing 'checks' section in dq_rules.yml"
        )

    checks = config["dq_framework"]["checks"]

    print("\n======================================")
    print("DQ RULE CONFIGURATION")
    print("======================================")

    print(f"Rules loaded: {len(checks)}")

    for rule in checks:

        print(
            f"{rule['id']} | "
            f"{rule['name']} | "
            f"Category: {rule['category']} | "
            f"Enabled: {rule['enabled']} | "
            f"Weight: {rule['default_weight']}"
        )

    print("======================================\n")

    return checks


if __name__ == "__main__":

    rules = load_dq_rules()

    print("DQ YAML configuration loaded successfully.")