def divide_shingle_chunks(shingle: str, chunk_length: int):
    """
    Divide shingle into chunks
    """
    chunks = []
    for i in range(0, len(shingle), chunk_length):
        chunks.append(shingle[i:min(i+chunk_length, len(shingle))])
    return chunks


class path_embedding:
    def __init__(self, chunk_length: int=20):
        self.operation_to_id = {}
        self.chunk_length = chunk_length

    def convert_operation_to_id(self, operation_name: str):
        """
        Convert operation name to id
        """
        if operation_name in self.operation_to_id:
            return self.operation_to_id[operation_name]
        current_key = len(self.operation_to_id)
        self.operation_to_id[operation_name] = current_key
        return current_key

    def convert_duration_to_id(self, duration: int):
        """
        Convert duration to id
        """
        if duration < 0:
            raise ValueError("Duration cannot be negative")
        cnt = 0
        while duration > 0:
            duration = duration // 100
            cnt += 1
        return cnt


    def build_graph(self, calls: list):
        """
        Build graph from calls
        """
        span_to_operation = {}
        nodes = set()
        edges = {}

        for call in calls:
            operation_name, parent_operation, duration, span_id, parent_span = call
            span_to_operation[span_id] = operation_name
            span_to_operation[parent_span] = parent_operation
            if parent_span not in edges:
                edges[parent_span] = []
            edges[parent_span].append((span_id, duration))
            nodes.add(parent_span)
            nodes.add(span_id)
        for key, value in edges.items():
            edges[key] = sorted(value, key=lambda x: span_to_operation[x[0]])
        nodes = list(nodes)
        nodes.sort(key=lambda x: span_to_operation[x])
        return nodes, edges, span_to_operation


    def dfs(self, node: str, edges: dict, span_to_operation: dict, visited_nodes: list, current_path: [], current_duration: None, all_paths: list):
        """
        DFS to get all paths
        """
        if node in visited_nodes:
            return []
        visited_nodes.add(node)
        current_path.append(self.convert_operation_to_id(span_to_operation[node]))
        if current_duration is not None:
            all_paths.append((current_path, self.convert_duration_to_id(current_duration)))
        if node not in edges:
            return
        for span_id, duration in edges[node]:
            self.dfs(span_id, edges, span_to_operation, visited_nodes, current_path.copy(), duration, all_paths)
        return

    def convert_calls_to_path(self, calls: list):
        """
        Convert calls to path
        """
        shingles = []
        nodes, edges, span_to_operation = self.build_graph(calls)
        # for span, operation in span_to_operation.items():
            # print(span, operation, self.convert_operation_to_id(operation))
        visited_nodes = set()
        all_paths = []
        self.dfs("root", edges, span_to_operation, visited_nodes, [], None, all_paths)
        # print(visited_nodes)
        # print(all_paths)
        for node in nodes:
            if node not in visited_nodes:
                all_paths = []
                self.dfs(node, edges, span_to_operation, visited_nodes, [], None, all_paths)
        # print(all_paths)
        return all_paths

    
    def convert_paths_to_chunk(self, shingles: list):
        """
        Convert shingles to chunks
        """
        shingles_count = {}
        for shingle_data in shingles:
            shingle_list, duration = shingle_data
            chunked_shingles = divide_shingle_chunks(shingle_list, self.chunk_length)
            for c in chunked_shingles:
                chunk = tuple(c)
                if chunk not in shingles_count:
                    shingles_count[chunk] = 0
                shingles_count[chunk] = max(shingles_count[chunk], duration)
        return [(list(shingle), cnt) for shingle, cnt in shingles_count.items()]


    def convert_call_to_chunked_paths(self, calls: list):
        """
        Convert calls to chunked shingles
        """
        shingles = self.convert_calls_to_path(calls)
        # print(shingles[1])
        chunked_shingles = self.convert_paths_to_chunk(shingles)
        return chunked_shingles