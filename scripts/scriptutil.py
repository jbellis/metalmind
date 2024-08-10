def update_sys_path():
    # add parent directory of this script to sys.path
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).resolve().parents[1]))
