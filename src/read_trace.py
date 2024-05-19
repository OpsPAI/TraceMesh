import pandas as pd


def read_csv(trace_file: str):
    """
    Read trace file and return a dataframe
    """
    df = pd.read_csv(trace_file)
    return df


def extract_calls(trace: pd.DataFrame):
    """
    Extract calls from single trace
    """
    if trace["TraceID"].nunique() > 1:
        print("Trace contains multiple TraceIDs")
        return None
    trace = trace.sort_values(by="StartTimeUnixNano")
    id_to_operation = trace[['SpanID', 'OperationName']].set_index('SpanID').to_dict()['OperationName']
    id_to_operation["root"] = "root"
    trace['ParentOperation'] = trace['ParentID'].map(id_to_operation)
    calls = [tuple(row) for row in trace[["OperationName", "ParentOperation", "Duration", "SpanID", "ParentID"]].values]
    return calls


def get_calls(traces: pd.DataFrame):
    """
    Get calls from trace dataframe
    """
    all_calls = []
    for trace_id, trace in traces.groupby("TraceID"):
        all_calls.append((trace_id, extract_calls(trace)))
    return all_calls
    

def process_trace_dataset(csv_file: str):
    """
    Process trace dataset
    """
    # dataset_path = os.path.join(dataset_dir, dataset_name)
    # spans_list = []
    # for root, _, files in os.walk(dataset_path):
    #     for file in files:
    #         if not file.endswith(".csv"):
    #             continue
    #         data = pd.read_csv(os.path.join(root, file))
    #         spans_list.append(data)
    # all_spans = pd.concat(spans_list, ignore_index=True)
    all_spans = read_csv(csv_file)
    print("Total Number of Spans: {}".format(len(all_spans)))
    print("Total Number of Traces: {}".format(len(all_spans["TraceID"].unique())))
    
    all_calls = get_calls(all_spans)
    print("Total Number of call groups: {}".format(len(all_calls)))
    return all_calls