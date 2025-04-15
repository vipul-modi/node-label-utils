import kubernetes
from kubernetes import client, config
import sys

def get_nodes_by_label(label_key):
    """Fetches nodes that have and do not have the specified label."""
    config.load_kube_config()
    v1 = client.CoreV1Api()
    nodes = v1.list_node().items

    nodes_with_label = []
    nodes_without_label = []

    for node in nodes:
        node_name = node.metadata.name
        labels = node.metadata.labels

        if label_key in labels:
            nodes_with_label.append(node_name)
        else:
            nodes_without_label.append(node_name)

    return nodes_with_label, nodes_without_label

def main():
    if len(sys.argv) != 2:
        print("Usage: python nodes_by_label.py <label_key>")
        sys.exit(1)

    label_key = sys.argv[1]
    nodes_with_label, nodes_without_label = get_nodes_by_label(label_key)

    print(f"Nodes with label '{label_key}':")
    for node in nodes_with_label:
        print(f"  {node}")

    print(f"\nNodes without label '{label_key}':")
    for node in nodes_without_label:
        print(f"  {node}")

if __name__ == "__main__":
    main()