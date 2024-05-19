from read_trace import process_trace_dataset
from path_vector import path_embedding
from sketch import SketchHash
from DenStream.DenStream import DenStream
from tqdm import tqdm
import os
from sklearn.cluster import DBSCAN
import argparse


def calculate_metrics(predicted_list, ground_truth_list, original_size):
    print("Predicted: ", len(predicted_list))
    print("Ground Truth: ", len(ground_truth_list))

    predicted_list = set(predicted_list)
    ground_truth_list = set(ground_truth_list)
    coverage = len(predicted_list & ground_truth_list) / len(ground_truth_list)
    sampling_rate = len(predicted_list) / original_size
    print(f"Coverage: {coverage}, Sampling Rate: {sampling_rate}")


def get_config():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sketch_length", type=int, default=100, help="Length of the sketch")
    parser.add_argument("--eps", type=float, default=0.01, help="Epsilon value for clustering")
    parser.add_argument("--data_path", type=str, default="../datasets/", help="Path to the dataset")
    parser.add_argument("--dataset", type=str, default="online_boutique", help="Name of the dataset")
    parser.add_argument("--budget", type=float, default=0.01, help="Budget for the sampling")
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = get_config()

    all_calls = process_trace_dataset(os.path.join(args.data_path, args.dataset, "train.csv"))
    graph = path_embedding(chunk_length=100)
    sketch_hash = SketchHash(L=args.sketch_length, chunk_length=100)

    sketch_list = []
    for idx, call_tuple in enumerate(all_calls):
        trace_id, call = call_tuple
        chunked_shingles = graph.convert_call_to_chunked_paths(call)
        sketch, projection = sketch_hash.construct_streamhash_sketch(chunked_shingles)
        sketch_list.append(sketch)

    dbscan = DBSCAN(eps=args.eps, min_samples=1, algorithm="brute", metric="cosine")
    dbscan.fit(sketch_list)

    labels = dbscan.labels_
    print("Number of clusters: ", len(set(labels)))
    clusters = {}
    for i, label in enumerate(labels):
        if label not in clusters:
            clusters[label] = []
        clusters[label].append(sketch_list[i])
    
    DS_clusterer = DenStream(lambd=0.01, eps=args.eps, beta=10, mu=1, init_clusters=clusters, budget=args.budget)
    # DS_clusterer.print_info()

    test_calls = process_trace_dataset(os.path.join(args.data_path, args.dataset, "test.csv"))
    sampled_count = 0
    sampled_trace = []
    for idx, call_tuple in tqdm(enumerate(test_calls), total=len(test_calls)):
        trace_id, call = call_tuple
        chunked_shingles = graph.convert_call_to_chunked_paths(call)
        sketch, projection = sketch_hash.construct_streamhash_sketch(chunked_shingles)
        sampled = DS_clusterer.process_sample(sketch)
        if sampled:
            sampled_count += 1
            sampled_trace.append(trace_id)
    print("Anomaly count: {}".format(sampled_count))
    # DS_clusterer.print_info()

    anomaly_labels = []
    with open(os.path.join(args.data_path, args.dataset, "test_label.txt"), 'r') as file:
        lines = file.readlines()
        for line in lines:
            anomaly_labels.append(str(line.strip()))
    
    calculate_metrics(sampled_trace, anomaly_labels, len(test_calls))
