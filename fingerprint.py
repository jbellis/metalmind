import xxhash
import numpy as np
from typing import Dict, Any, Set, List, Tuple
from nltk import ngrams
import re

_NON_ALPHA = re.compile(r'\W+')
_MAX_HASH = np.uint64((1 << 32) - 1)
_MERSENNE_PRIME = np.uint64((1 << 61) - 1)


def mh_permutations(num_perm: int) -> np.ndarray:
    def generate_coprime(prime: int) -> int:
        while True:
            x = np.random.randint(1, prime, dtype=np.uint64)
            if np.gcd(x, prime) == 1:
                return x

    # Generate 'a' coefficients
    a = np.array([generate_coprime(_MERSENNE_PRIME) for _ in range(num_perm)], dtype=np.uint64)
    # Generate 'b' offsets
    b = np.random.randint(0, _MERSENNE_PRIME, size=num_perm, dtype=np.uint64)

    return np.array([a, b])


def mh_signature(
        content: str,
        *,
        ngram_size: int,
        n_minhashes: int,
        signature_size: int,
        band_size: int,
        permutations: np.ndarray,
) -> np.array:
    """
    Generate a MinHash signature for the given content.

    Args:
        content (str): The input text to generate a signature for.
        ngram_size (int): Number of words per n-gram.
        n_minhashes (int): Number of minhashes per document.
        signature_size (int): Number of float32 elements in the final signature.
        band_size (int): Number of minhashes per band.
        permutations (np.ndarray): Seed for minhash permutations.

    Returns:
        np.ndarray: The normalized MinHash signature.
    """
    # Generate the raw minhash signature
    a, b = permutations
    masks = np.full(shape=n_minhashes, dtype=np.uint64, fill_value=_MAX_HASH)
    tokens = set(" ".join(t) for t in ngrams(_NON_ALPHA.split(content), ngram_size))
    hashvalues = np.fromiter((xxhash.xxh64(token.encode("utf-8")).intdigest() for token in tokens),
                             dtype=np.uint64, count=len(tokens))
    permuted_hashvalues = np.bitwise_and(
        ((hashvalues[:, np.newaxis] * a + b) % _MERSENNE_PRIME), _MAX_HASH
    )
    hashvalues = np.vstack([permuted_hashvalues, masks]).min(axis=0)

    # Generate the LSH bands and quantize down to the space available
    n_bands = n_minhashes // band_size
    bits_per_band = signature_size // n_bands
    bands = [xxhash.xxh64(band.tobytes()).intdigest() % bits_per_band
             for band in np.array_split(hashvalues, n_bands)]

    # Create a float32 array of signature_size and set values to 1.0 based on the bands
    signature = np.zeros(signature_size, dtype=np.float32)
    indices = np.fromiter((i * bits_per_band + v for i, v in enumerate(bands)), dtype=np.int64)
    signature[indices] = 1.0

    # Normalize the signature
    norm = np.linalg.norm(signature)
    if norm > 0:
        signature /= norm

    return signature

_permutations = np.load('fingerprint_seed.npz')['arr']
def encode(text: str) -> np.array:
    return mh_signature(text, ngram_size=3, signature_size=2048, n_minhashes=512, band_size=2, permutations=_permutations)


def similarity(a, b):
    return (1 + np.dot(a, b)) / 2
