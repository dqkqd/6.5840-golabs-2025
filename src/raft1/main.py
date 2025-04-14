import concurrent.futures
import os
import subprocess
from enum import Enum
from typing import Annotated

import typer

app = typer.Typer()


class Command(str, Enum):
    Test3A = "3A"
    TestBasicAgree3B = "TestBasicAgree3B"


def failed(s: str) -> bool:
    for x in reversed(s.splitlines()):
        x = x.strip()
        if "FAIL" in x or "DATA RACE" in x:
            return True
    return False


def run_command_fn(command: str) -> None:
    output = subprocess.run(command.split(" "), capture_output=True)
    stdout = output.stdout.decode()
    if failed(stdout):
        raise ValueError(stdout)


@app.command()
def main(
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
            ex = f.exception(timeout=0.0)
            if ex is not None and isinstance(ex, ValueError):
                print(ex)
                return

    print("PASS")


if __name__ == "__main__":
    app()
