import concurrent.futures
import os
import shutil
import subprocess
from enum import Enum
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import typer
from tqdm import tqdm

app = typer.Typer()


class CommandError(Exception): ...


def output_dir() -> Path:
    out = Path("output")
    out.mkdir(exist_ok=True)
    return out


class Command(str, Enum):
    TestInitialElection3A = "TestInitialElection3A"
    TestReElection3A = "TestReElection3A"
    TestManyElections3A = "TestManyElections3A"
    TestBasicAgree3B = "TestBasicAgree3B"
    TestFigure8Unreliable3C = "TestFigure8Unreliable3C"
    TestBackup3B = "TestBackup3B"
    Test3A = "3A"
    Test3B = "3B"
    Test3C = "3C"


def failed(s: str) -> bool:
    s = s.strip()
    if "FAIL" in s or "DATA RACE" in s or "warning:" in s:
        return True
    return False


def check_future(future: concurrent.futures.Future[None]):
    ex = future.exception(timeout=0.0)
    if ex is not None:
        if isinstance(ex, CommandError):
            print(ex)
            return
        else:
            raise ex


def run_command_fn(command: str) -> None:
    output_file = output_dir() / uuid4().hex
    _ = subprocess.run(command.split(" "), stdout=output_file.open("w"))

    with output_file.open() as f:
        has_error = any(map(failed, f))

    if has_error:
        raise CommandError(output_file.read_text())


@app.command()
def parallel(
    case: Annotated[Command, typer.Option(case_sensitive=False)],
    iterations: int = 5,
    verbose: bool = False,
):
    if verbose:
        os.environ["DEBUG"] = "1"

    command = f"go test -run {case.value} --race"
    print(f"Running `{command}` with {iterations} iterations")
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = concurrent.futures.wait(
            (executor.submit(run_command_fn, command) for _ in range(iterations)),
            return_when=concurrent.futures.FIRST_EXCEPTION,
        )
        for f in futures.done:
            check_future(f)

    print("PASS")


@app.command()
def sequence(
    case: Annotated[Command, typer.Option(case_sensitive=False)],
    iterations: int = 5,
    verbose: bool = False,
):
    if verbose:
        os.environ["DEBUG"] = "1"

    command = f"go test -run {case.value} --race"
    print(f"Running `{command}` with {iterations} iterations")
    for _ in tqdm(range(iterations)):
        try:
            run_command_fn(command=command)
        except CommandError as ex:
            print(ex)
            return

    print("PASS")


if __name__ == "__main__":
    shutil.rmtree(output_dir())
    app()
