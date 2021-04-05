# general-purpose tool whose job is to exercise io.py, since the unit tests only
# really exercise the local filesystem path
import argparse
import sys

from arlo_e2e.io import make_file_ref_from_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="General-purpose tool to exercise io.py"
    )

    parser.add_argument(
        "-c",
        "--command",
        type=str,
        default="read",
        help="one of (read, write, scandir, unlink, size), default: read\n"
        "for the 'write' command, the data to be written comes from stdin",
    )

    parser.add_argument(
        "name",
        type=str,
        help="filename or directory name, which can start with s3:// or just be a local path",
    )

    args = parser.parse_args()

    command = args.command
    name = args.name

    fr = make_file_ref_from_path(name)

    if command == "read":
        result = fr.read()
        if result is None:
            print("Read failed.")
            exit(1)
        else:
            try:
                print(result.decode("utf-8"))
            except UnicodeDecodeError:
                print(result)

    elif command == "write":
        data = sys.stdin.read()
        success = fr.write(data)
        if success:
            print("Success.")
        else:
            print("Write failed.")

    elif command == "scandir":
        files, subdirs = fr.scandir()
        print("Subdirectories:")
        for s in sorted(subdirs.keys()):
            print(f"    {s}: {subdirs[s]}")

        print("Files:")
        for f in sorted(files.keys()):
            print(f"    {f}: {files[f]}")

    elif command == "unlink":
        fr.unlink()
        print("Unlinked.")

    elif command == "size":
        l = fr.size()
        print(f"{fr}: {l} bytes")

    else:
        parser.print_help()
        exit(1)
