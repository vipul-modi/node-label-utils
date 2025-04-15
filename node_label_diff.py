import kubernetes
from kubernetes import client, config

def get_node_labels(node_prefix="nd96"):
    """Fetches all nodes with the specified prefix and their labels from the Kubernetes cluster."""
    config.load_kube_config()
    v1 = client.CoreV1Api()
    nodes = v1.list_node().items

    node_labels = {}
    for node in nodes:
        node_name = node.metadata.name
        if node_name.startswith(node_prefix):
            labels = node.metadata.labels
            node_labels[node_name] = labels

    return node_labels

def find_label_differences(node_labels):
    """Identifies nodes with differing labels and highlights the differences."""
    all_labels = set()
    for labels in node_labels.values():
        all_labels.update(labels.keys())

    differences = {}
    for node, labels in node_labels.items():
        missing_labels = all_labels - set(labels.keys())
        extra_labels = set(labels.keys()) - all_labels
        if missing_labels or extra_labels:
            differences[node] = {
                "missing_labels": missing_labels,
                "extra_labels": extra_labels,
            }

    return differences

def get_label_value_histogram(node_labels):
    """Builds a histogram of label values across all nodes, ignoring unique values and specific labels."""
    from collections import defaultdict, Counter
    label_value_histogram = defaultdict(Counter)
    ignored_labels = {"REPAIR_STATE", "RepairStatus"}

    for labels in node_labels.values():
        for label, value in labels.items():
            if label not in ignored_labels:
                label_value_histogram[label][value] += 1

    # Remove values with count == 1
    filtered_histogram = {}
    for label, value_counts in label_value_histogram.items():
        filtered = {value: count for value, count in value_counts.items() if count > 1}
        if filtered:
            filtered_histogram[label] = filtered
    return filtered_histogram

def main():
    import sys

    node_prefix = "nd96"  # Default prefix
    if len(sys.argv) > 1:
        node_prefix = sys.argv[1]

    node_labels = get_node_labels(node_prefix)
    differences = find_label_differences(node_labels)

    if not differences:
        print("All nodes have consistent labels.")
    else:
        print("Nodes with label differences:")
        for node, diff in differences.items():
            print(f"Node: {node}")
            print(f"  Missing labels: {diff['missing_labels']}")
            print(f"  Extra labels: {diff['extra_labels']}")

    # Print histogram of label values
    print("\nHistogram of label values across all nodes (excluding unique values):")
    label_value_histogram = get_label_value_histogram(node_labels)
    for label, value_counts in label_value_histogram.items():
        print(f"Label: {label}")
        for value, count in value_counts.items():
            print(f"  Value: {value} - Count: {count}")

if __name__ == "__main__":
    main()