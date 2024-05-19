import numpy as np
import random


def cosine_similarity(vector_a, vector_b):
    vector_a = np.array(vector_a)
    vector_b = np.array(vector_b)

    dot_product = np.dot(vector_a, vector_b)
    norm_a = np.linalg.norm(vector_a)
    norm_b = np.linalg.norm(vector_b)

    cosine_similarity = dot_product / (norm_a * norm_b)

    return cosine_similarity


class SketchHash():
    def __init__(self, L: int=1000, chunk_length: int=10):
        self.L = L
        self.chunk_length = chunk_length
        self.H = []
        self.allocate_random_bits()

    def allocate_random_bits(self):
        """
        Allocate random bits for hashing.

        :param H: A list of lists to store the random bits.
        :param chunk_length: The length of each chunk.
        """
        random.seed(42)
        for i in range(self.L):
            self.H.append([])
            for _ in range(self.chunk_length + 2):
                self.H[i].append(random.getrandbits(64))

    def hashmulti(self, shingle_list: list, randbits: list):
        """
        Hash a shingle using multiple hash functions.
        """
        sum = randbits[0]
        for i in range(len(shingle_list)):
            sum += shingle_list[i] * randbits[i + 1]
        return 2 * ((sum >> 63) & 1) - 1 
        
    def construct_streamhash_sketch(self, shingles: list):
        """
        Construct streamhash sketch for a given list of shingles
        """
        projection = [0 for _ in range(self.L)]

        for shingle in shingles:
            shingle_list, count = shingle
            for i in range(self.L):
                projection[i] += count * self.hashmulti(shingle_list, self.H[i])

        sketch = [1 if p >= 0 else 0 for p in projection]

        return (sketch, projection)

