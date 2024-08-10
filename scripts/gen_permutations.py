import scriptutil
util.update_sys_path()

import argparse
import numpy as np
from fingerprint import mh_permutations


def main():
    parser = argparse.ArgumentParser(description="Generate new permutations for fingerprinting.")
    parser.add_argument("num_perm", type=int, help="Number of permutations to generate")
    args = parser.parse_args()

    # Generate new permutations
    new_permutations = mh_permutations(args.num_perm)

    # Save the new permutations, overwriting the existing file
    np.savez('fingerprint_seed.npz', arr=new_permutations)

    print(f"Generated and saved {args.num_perm} new permutations to fingerprint_seed.npz")


if __name__ == "__main__":
    main()