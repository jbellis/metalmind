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
    num_perm: int,
    ngram_size: int,
    band_size: int,
    permutations: np.ndarray,
) -> List[bytes]:
    a, b = permutations
    masks: np.ndarray = np.full(shape=num_perm, dtype=np.uint64, fill_value=_MAX_HASH)
    tokens: Set[str] = {" ".join(t) for t in ngrams(_NON_ALPHA.split(content), ngram_size)}
    hashvalues: np.ndarray = np.array([xxhash.xxh64(token.encode("utf-8")).intdigest() for token in tokens], dtype=np.uint64)
    permuted_hashvalues = np.bitwise_and(
        ((hashvalues * np.tile(a, (len(hashvalues), 1)).T).T + b) % _MERSENNE_PRIME, _MAX_HASH
    )
    hashvalues = np.vstack([permuted_hashvalues, masks]).min(axis=0)
    num_bands = num_perm // band_size
    return [bytes(hashvalues[i*band_size:(i+1)*band_size].byteswap().data) for i in range(num_bands)]
