import concurrent.futures
import enum
import os
import shutil
import subprocess
from pathlib import Path
from signal import SIGINT
from typing import Annotated
from uuid import uuid4

import typer
from tqdm import tqdm

app = typer.Typer()


class ExecutionError(Exception): ...


@enum.unique
class RaftCommand(enum.StrEnum):
    Test3A = "3A"
    Test3B = "3B"
    Test3C = "3C"
    Test3D = "3D"
    TestBackup3B = "TestBackup3B"
    TestBasicAgree3B = "TestBasicAgree3B"
    TestFigure8Unreliable3C = "TestFigure8Unreliable3C"
    TestInitialElection3A = "TestInitialElection3A"
    TestManyElections3A = "TestManyElections3A"
    TestReElection3A = "TestReElection3A"
    TestSnapshotBasic3D = "TestSnapshotBasic3D"


@enum.unique
class RSMCommand(enum.StrEnum):
    Test4A = "4A"


@enum.unique
class KvRaftCommand(enum.StrEnum):
    TestBasic4B = "TestBasic4B"
    TestSpeed4B = "TestSpeed4B"
    TestConcurrent4B = "TestConcurrent4B"
    TestUnreliable4B = "TestUnreliable4B"
    TestOnePartition4B = "TestOnePartition4B"
    TestManyPartitionsOneClient4B = "TestManyPartitionsOneClient4B"


@app.command()
def raft(
    case: Annotated[RaftCommand, typer.Option(case_sensitive=False)],
    iterations: int = 5,
    verbose: bool = False,
    race: bool = False,
    timeout: int = 600,
    parallel: bool = False,
):
    if verbose:
        os.environ["DEBUG"] = "1"

    execute(
        cwd=Path("src/raft1"),
        command=case,
        race=race,
        sequence=not parallel,
        iterations=iterations,
        timeout=timeout,
    )


@app.command()
def rsm(
    case: Annotated[RSMCommand, typer.Option(case_sensitive=False)],
    iterations: int = 5,
    verbose: bool = False,
    race: bool = False,
    timeout: int = 600,
    parallel: bool = False,
):
    if verbose:
        os.environ["DEBUG"] = "1"
        os.environ["RSM_DEBUG"] = "1"

    execute(
        cwd=Path("src/kvraft1/rsm"),
        command=case,
        race=race,
        sequence=not parallel,
        iterations=iterations,
        timeout=timeout,
    )


@app.command()
def kvraft(
    case: Annotated[KvRaftCommand, typer.Option(case_sensitive=False)],
    iterations: int = 5,
    verbose: bool = False,
    race: bool = False,
    timeout: int = 60,
    parallel: bool = False,
):
    if verbose:
        os.environ["DEBUG"] = "1"
        os.environ["RSM_DEBUG"] = "1"
        os.environ["KVRAFT_DEBUG"] = "1"

    execute(
        cwd=Path("src/kvraft1"),
        command=case,
        race=race,
        sequence=not parallel,
        iterations=iterations,
        timeout=timeout,
    )


@app.command()
def extract_log(path: Path, pattern: str):
    new_output_file = path.with_suffix(".out")
    new_output_file.touch()

    with path.open("r") as src, new_output_file.open("w") as dst:
        for line in src:
            if pattern in line:
                _ = dst.write(line)


def execute(
    cwd: Path,
    command: str,
    race: bool,
    sequence: bool,
    iterations: int,
    timeout: int = 100,
):
    shutil.rmtree(output_dir())

    command = f"go test -run {command}"
    if race:
        command += " --race"

    print(
        f"Running `{command}` in {"sequence" if sequence else "parallel" } with {iterations} iterations"
    )

    if sequence:
        for _ in tqdm(range(iterations)):
            run_command(cwd=cwd, command=command, timeout=timeout)
    else:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futures = concurrent.futures.wait(
                (
                    executor.submit(run_command, cwd, command, timeout)
                    for _ in range(iterations)
                ),
                return_when=concurrent.futures.FIRST_EXCEPTION,
            )
            for f in futures.done:
                ex = f.exception(timeout=0.0)
                if ex is not None:
                    if isinstance(ex, ExecutionError):
                        print(ex)
                        return
                    else:
                        raise ex


def run_command(cwd: Path, command: str, timeout: int):
    output_file = output_dir() / uuid4().hex
    with subprocess.Popen(
        command.split(" "),
        stdout=output_file.open("w"),
        start_new_session=True,
        cwd=cwd,
    ) as process:
        try:
            _ = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # kill the old running process
            os.killpg(os.getpgid(process.pid), SIGINT)
            _ = process.wait()
            print(f"Command {command}, timeout after {timeout}s")

    with output_file.open() as f:
        for line in f:
            if "FAIL" in line or "DATA RACE" in line or "warning:" in line:
                raise ExecutionError(f"error:\n{output_file}")

    # remove those success
    output_file.unlink()


def output_dir() -> Path:
    out = Path("output")
    out.mkdir(exist_ok=True)
    return out


if __name__ == "__main__":
    app()
