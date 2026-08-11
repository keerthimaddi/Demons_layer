import os
import yaml


def load_dq_rules():

    project_root = os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )

    rules_path = os.path.join(
        project_root,
        "config",
        "dq_rules.yml"
    )

    if not os.path.exists(rules_path):
        raise FileNotFoundError(
            f"DQ rules file not found: {rules_path}"
        )

    with open(rules_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

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

    if len(checks) != 16:
        raise ValueError(
            f"Expected 16 DQ checks, but found {len(checks)}"
        )

    print("\n======================================")
    print("DQ RULE CONFIGURATION")
    print("======================================")

    print(f"Rules loaded: {len(checks)}")

    for rule in checks:

        print(
            f"{rule['id']} | "
            f"{rule['name']} | "
            f"Level: {rule['level']} | "
            f"Enabled: {rule['enabled']} | "
            f"Weight: {rule['default_weight']}"
        )

    print("======================================\n")

    return checks


if __name__ == "__main__":

    rules = load_dq_rules()

    print("All 16 DQ rules loaded successfully.")