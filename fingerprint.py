import xxhash
import numpy as np
from typing import Dict, Any, Set, List
from nltk import ngrams
import re

_NON_ALPHA = re.compile(r'\W+')
_MAX_HASH = np.uint64((1 << 32) - 1)
_MERSENNE_PRIME = np.uint64((1 << 61) - 1)


def mh_permutations(num_perm: int) -> np.ndarray:
    # Generate 'a' coefficients
    a = np.random.randint(1, _MERSENNE_PRIME, size=num_perm, dtype=np.uint64)
    # Ensure 'a' coefficients are coprime with MERSENNE_PRIME
    a = np.array([ai if np.gcd(ai, _MERSENNE_PRIME) == 1 else 1 for ai in a], dtype=np.uint64)
    # Generate 'b' offsets
    b = np.random.randint(0, _MERSENNE_PRIME, size=num_perm, dtype=np.uint64)

    return np.array([a, b])


def mh_signature(
        content: str,
        *,
        ngram_size: int,          # number of words per ngram
        n_minhashes: int,         # number of minhashes per document
        signature_size: int,      # number of f32 elements in signature
        band_size: int,           # number of minhashes per band
        permutations: np.ndarray, # seed for minhash permutations
) -> np.array:
    # Generate the raw minhash signature
    a, b = permutations
    masks = np.full(shape=n_minhashes, dtype=np.uint64, fill_value=_MAX_HASH)
    tokens = {" ".join(t) for t in ngrams(_NON_ALPHA.split(content), ngram_size)}
    hashvalues = np.array([xxhash.xxh64(token.encode("utf-8")).intdigest() for token in tokens],
                          dtype=np.uint64)
    permuted_hashvalues = np.bitwise_and(
        ((hashvalues * np.tile(a, (len(hashvalues), 1)).T).T + b) % _MERSENNE_PRIME, _MAX_HASH
    )
    hashvalues = np.vstack([permuted_hashvalues, masks]).min(axis=0)

    # Generate the LSH bands and quantize down to the space available
    n_bands = n_minhashes // band_size
    bits_per_band = signature_size // n_bands
    bands = [xxhash.xxh64(hashvalues[i*band_size:(i+1)*band_size].data).intdigest() % bits_per_band
             for i in range(n_bands)]
    # Create a float32 array of signature_size and set values to 1.0 based on the bands
    signature = np.zeros(signature_size, dtype=np.float32)
    indices = np.array([i * bits_per_band + v for i, v in enumerate(bands)])
    indices = indices.astype(np.int64)
    np.put(signature, indices, 1.0)

    # Normalize the signature
    norm = np.linalg.norm(signature)
    if norm > 0:
        signature /= norm

    return signature