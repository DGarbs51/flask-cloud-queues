"""Size Python's view of the machine from the container's cgroup on Laravel Cloud.

Pods get a CFS quota and a memory limit, not a cpuset, so os.cpu_count(),
os.sysconf() and every default pool size derived from them report the node
(4-16x the instance size on 1 vCPU pods). This module, imported at interpreter
start by zz_laravel_cloud_bootstrap.pth, caps them at the cgroup limits.

Users may set a lower CPU count but never a higher one. Explicit sizes such as
ThreadPoolExecutor(50) are left alone: the kernel quota still bounds real use.
Never raises: a broken bootstrap must not stop Python from starting.
"""

import os
import sys

# Read by native libraries and child processes that never ask Python.
CPU_ENV_VARS = (
    "PYTHON_CPU_COUNT",  # CPython 3.13+ os.cpu_count / os.process_cpu_count
    "OMP_NUM_THREADS",  # OpenMP, PyTorch, coreutils nproc
    "OPENBLAS_NUM_THREADS",  # NumPy / SciPy (OpenBLAS)
    "MKL_NUM_THREADS",  # NumPy / SciPy (MKL)
    "NUMEXPR_MAX_THREADS",  # numexpr / pandas: upper bound...
    "NUMEXPR_NUM_THREADS",  # ...and pool size, which errors when above the bound
    "RAYON_NUM_THREADS",  # Rust extensions using rayon
    "POLARS_MAX_THREADS",  # Polars
)

CGROUP = "/sys/fs/cgroup"


def _read(name):
    with open(os.path.join(CGROUP, name)) as f:
        return f.read().strip()


def _positive_int(value):
    if isinstance(value, str) and value.isascii() and value.isdigit() and int(value) > 0:
        return int(value)
    return None


def _cpu_limit():
    try:
        quota, period = _read("cpu.max").split()
        if quota == "max":
            return None
        cpus = max(1, -(-int(quota) // int(period)))  # ceil, at least 1
        return min(cpus, len(os.sched_getaffinity(0)))
    except (OSError, ValueError, ZeroDivisionError):
        return None


def _memory_limit():
    try:
        value = _read("memory.max")
        return None if value == "max" else int(value)
    except (OSError, ValueError):
        return None


def _as_os_function(func, name):
    # Pickle (process pools) finds functions by module and name; point it at
    # os.<name> so the receiving process uses its own os.<name>.
    func.__module__ = "os"
    func.__name__ = func.__qualname__ = name
    func.__doc__ = getattr(os, name).__doc__
    setattr(os, name, func)


def _cap_env(cpus):
    for name in CPU_ENV_VARS:
        value = os.environ.get(name, "")
        if name == "OMP_NUM_THREADS":
            value = value.split(",")[0]  # nested levels: "2,1"
        current = _positive_int(value)
        # Keep a lower user value; replace a missing, invalid or higher one.
        if current is None or current > cpus:
            os.environ[name] = str(cpus)


def _install():
    cpus = _cpu_limit()
    memory = _memory_limit()
    if cpus is None and memory is None:
        return

    count = None
    if cpus is not None:
        _cap_env(cpus)
        count = int(os.environ["PYTHON_CPU_COUNT"])
        # -X cpu_count=N (3.13+) outranks PYTHON_CPU_COUNT; keep it when lower.
        count = min(count, _positive_int(sys._xoptions.get("cpu_count")) or count)

        def cpu_count():
            return count

        _as_os_function(cpu_count, "cpu_count")
        if hasattr(os, "process_cpu_count"):

            def process_cpu_count():
                return count

            _as_os_function(process_cpu_count, "process_cpu_count")

    original_sysconf = os.sysconf
    names = {value: key for key, value in os.sysconf_names.items()}

    def sysconf(name):
        key = names.get(name, name) if isinstance(name, int) else name
        if count is not None and key in ("SC_NPROCESSORS_ONLN", "SC_NPROCESSORS_CONF"):
            return count
        if memory is not None and key in ("SC_PHYS_PAGES", "SC_AVPHYS_PAGES"):
            node = original_sysconf(name)
            limit = memory
            if key == "SC_AVPHYS_PAGES":
                try:
                    limit = max(0, memory - int(_read("memory.current")))
                except (OSError, ValueError):
                    pass
            return min(node, limit // original_sysconf("SC_PAGE_SIZE"))
        return original_sysconf(name)

    _as_os_function(sysconf, "sysconf")


if os.environ.get("LARAVEL_CLOUD") == "1":
    try:
        _install()
    except Exception:
        pass
